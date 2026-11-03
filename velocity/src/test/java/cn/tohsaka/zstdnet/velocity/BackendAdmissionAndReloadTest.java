package cn.tohsaka.zstdnet.velocity;

import com.velocitypowered.api.event.player.ServerPreConnectEvent;
import com.velocitypowered.api.network.ProtocolState;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ServerConnection;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.MinecraftSessionHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import net.kyori.adventure.text.Component;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.tohsaka.zstdnet.velocity.RequiredClientTest.*;
import static org.junit.jupiter.api.Assertions.*;

class BackendAdmissionAndReloadTest {
    @TempDir Path directory;

    @Test void configurationProbeIsSentOnlyOnceAndDoesNotApproveAClientWithoutHello() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.protocol.set(ProtocolState.CONFIGURATION);
            var first = f.awaitAdmission();
            ByteBuf wire = f.channel.readOutbound();
            assertNotNull(wire);
            var decoder = new com.velocitypowered.proxy.protocol.netty.MinecraftDecoder(
                    com.velocitypowered.proxy.protocol.ProtocolUtils.Direction.CLIENTBOUND);
            decoder.setProtocolVersion(com.velocitypowered.api.network.ProtocolVersion.MINECRAFT_1_21_11);
            decoder.setState(com.velocitypowered.proxy.protocol.StateRegistry.CONFIG);
            var receiver = new io.netty.channel.embedded.EmbeddedChannel(decoder);
            try {
                receiver.writeInbound(wire);
                com.velocitypowered.proxy.protocol.packet.PluginMessagePacket packet = receiver.readInbound();
                assertNotNull(packet);
                try {
                    assertEquals("minecraft:register", packet.getChannel());
                    assertEquals(ControlMessage.CHANNEL, packet.content().toString(java.nio.charset.StandardCharsets.UTF_8));
                } finally { packet.release(); }
            } finally { receiver.finishAndReleaseAll(); }
            var second = f.awaitAdmission();
            assertNull(f.channel.readOutbound(), "concurrent connects must not spam configuration registration");
            assertFalse(first.isDone()); assertFalse(second.isDone());
            f.advance(10);
            assertTrue(first.isDone()); assertTrue(second.isDone());
            assertFalse(f.backendEvent.getResult().isAllowed());
            f.assertGuidedDisconnect();
        }
    }

    @Test void helloAlreadyReceivedBeforeAdmissionDoesNotNeedAProbe() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.protocol.set(ProtocolState.CONFIGURATION);
            f.receive(ControlMessage.HELLO, ControlMessage.VERSION);
            var checked = f.awaitAdmission();
            assertTrue(checked.isDone());
            assertTrue(f.backendEvent.getResult().isAllowed());
            assertNull(f.channel.readOutbound());
        }
    }

    @Test void configurationHelloReleasesInitialBackendEventBeforePlayWithoutWaitingForWorldPackets() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.protocol.set(ProtocolState.CONFIGURATION);
            var checked = f.awaitAdmission();
            assertFalse(checked.isDone(), "the awaited event must prevent the backend connect continuation");
            f.receive(ControlMessage.HELLO, ControlMessage.VERSION);
            assertTrue(checked.isDone());
            assertTrue(f.backendEvent.getResult().isAllowed());
            assertEquals("VANILLA", f.stage(), "the transport itself still switches at PLAY");
            f.advance(120);
            assertNull(f.disconnected.get(), "backend resource/configuration loading has its own deadline");
        }
    }

    @Test void simultaneousInitialRequestsStayPendingUntilHelloAndResolveTogether() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.protocol.set(ProtocolState.CONFIGURATION);
            var first = f.awaitAdmission();
            var second = f.awaitAdmission();
            assertFalse(first.isDone()); assertFalse(second.isDone());
            f.receive(ControlMessage.HELLO, ControlMessage.VERSION);
            assertTrue(first.isDone()); assertTrue(second.isDone());
            f.advance(20);
            assertNull(f.disconnected.get());
        }
    }

    @Test void invalidVersionDeniesTheInitialBackendEventAndSendsDecodableGuidance() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.protocol.set(ProtocolState.CONFIGURATION);
            var waiting = f.awaitAdmission();
            f.drainOutbound();
            f.receive(ControlMessage.HELLO, 99);
            assertTrue(waiting.isDone());
            assertFalse(f.backendEvent.getResult().isAllowed());
            f.assertGuidedDisconnect();
        }
    }

    @Test void disconnectDuringAdmissionResolvesTheEventAndCancelsItsTimer() throws Exception {
        try (Fixture f = new Fixture(true)) {
            var waiting = f.awaitAdmission();
            f.invoke("finish");
            assertTrue(waiting.isDone());
            assertFalse(f.backendEvent.getResult().isAllowed());
            f.advance(20);
            assertNull(f.disconnected.get());
        }
    }

    @Test void queuedWriteFailureCannotReplaceGuidanceWithNativeInternalConnectionError() throws Exception {
        try (Fixture f = new Fixture(true)) {
            AtomicInteger internalErrors = new AtomicInteger();
            MinecraftConnection nativeConnection = new MinecraftConnection(f.channel, null);
            set(nativeConnection, "activeSessionHandler", new MinecraftSessionHandler() {
                @Override public void exception(Throwable cause) {
                    internalErrors.incrementAndGet();
                    f.player.disconnect(Component.text("internal connection error"));
                }
            });
            f.channel.pipeline().replace("handler", "handler", nativeConnection);
            f.receive(ControlMessage.HELLO, ControlMessage.VERSION);
            f.drainOutbound();
            ByteBuf world = Unpooled.buffer().writeByte(42);
            var pending = f.channel.writeAndFlush(world);
            pending.addListener(done -> {
                if (!done.isSuccess()) f.channel.pipeline().fireExceptionCaught(done.cause());
            });
            f.advance(5);
            assertEquals(0, internalErrors.get(), "native exceptionCaught must see an already-closed connection");
            assertEquals(0, world.refCnt());
            assertFalse(pending.isSuccess());
            f.assertGuidedDisconnect();
        }
    }

    @Test void cachedPromisesAreNotFailedBeforeTheDisconnectWriteFlushes() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.receive(ControlMessage.HELLO, ControlMessage.VERSION);
            f.drainOutbound();
            ByteBuf world = Unpooled.buffer().writeByte(7);
            var pending = f.channel.writeAndFlush(world);
            java.util.List<Object> held = new java.util.ArrayList<>();
            java.util.List<ChannelPromise> promises = new java.util.ArrayList<>();
            f.channel.pipeline().addBefore("cipher-encoder", "slow-socket", new ChannelOutboundHandlerAdapter() {
                @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                    held.add(msg); promises.add(promise);
                }
            });
            f.invoke("rejectRequiredClient");
            assertTrue(f.channel.isActive());
            assertEquals(1, held.size(), "only the guided disconnect is written; cached world data stays discarded");
            assertFalse(pending.isDone(), "failure listeners cannot run while the socket is still open");
            assertEquals(1, world.refCnt());
            f.channel.pipeline().remove("slow-socket");
            f.channel.pipeline().context("cipher-encoder").writeAndFlush(held.getFirst(), promises.getFirst());
            assertFalse(f.channel.isActive());
            assertTrue(pending.isDone());
            assertEquals(0, world.refCnt());
            f.assertGuidedDisconnect();
        }
    }

    @Test void reloadOffImmediatelyReleasesPendingAdmissionsAndDoesNotEvictConnectedPlayers() throws Exception {
        try (Fixture f = new Fixture(true, directory)) {
            var waiting = f.awaitAdmission();
            Files.writeString(directory.resolve("zstdnet.properties"), "enabled=true\nrequire_client=false\nlevel=6\n");
            reload(f.plugin);
            f.channel.runPendingTasks();
            assertTrue(waiting.isDone());
            assertTrue(f.backendEvent.getResult().isAllowed());
            f.advance(20);
            assertNull(f.disconnected.get());
        }
    }

    @Test void reloadOnAppliesToAnUnadmittedSessionButLeavesOnlineBackendSwitchesAlone() throws Exception {
        try (Fixture f = new Fixture(false, directory)) {
            // A prior optional reload must not pre-approve a connection that has not requested a backend.
            Files.writeString(directory.resolve("zstdnet.properties"), "require_client=false\n");
            reload(f.plugin); f.channel.runPendingTasks();
            Files.writeString(directory.resolve("zstdnet.properties"), "require_client=true\n");
            reload(f.plugin); f.channel.runPendingTasks();
            var waiting = f.awaitAdmission();
            assertFalse(waiting.isDone());
            f.advance(10);
            assertFalse(f.backendEvent.getResult().isAllowed());
            assertNotNull(f.disconnected.get());
        }
        try (Fixture f = new Fixture(true)) {
            RegisteredServer backend = fake(RegisteredServer.class, (name, args) -> null);
            ServerConnection connection = fake(ServerConnection.class, (name, args) -> name.equals("getServer") ? backend : null);
            Player onlinePlayer = fake(Player.class, (name, args) -> name.equals("getCurrentServer") ? Optional.of(connection) : null);
            assertNull(f.plugin.preConnect(new ServerPreConnectEvent(onlinePlayer, backend)));
            assertNull(f.disconnected.get());
        }
    }

    @Test void invalidReloadKeepsTheOldPolicyAndEnabledChangesRequireRestart() throws Exception {
        try (Fixture f = new Fixture(true, directory)) {
            var waiting = f.awaitAdmission();
            Files.writeString(directory.resolve("zstdnet.properties"), "require_client=false\nlevel=99\n");
            assertThrows(java.io.IOException.class, () -> reload(f.plugin));
            Files.writeString(directory.resolve("zstdnet.properties"), "enabled=false\nrequire_client=false\n");
            assertThrows(java.io.IOException.class, () -> reload(f.plugin));
            f.channel.runPendingTasks();
            assertFalse(waiting.isDone());
            f.advance(10);
            assertNotNull(f.disconnected.get());
        }
    }

    @Test void reloadDoesNotResetAnAdmissionDeadlineOrExistingIpConnectionLeases() throws Exception {
        try (Fixture f = new Fixture(true, directory)) {
            var waiting = f.awaitAdmission();
            f.advance(9);
            Files.writeString(directory.resolve("zstdnet.properties"), "require_client=true\ndebug=true\n");
            reload(f.plugin); f.channel.runPendingTasks();
            f.advance(1);
            assertTrue(waiting.isDone());
            assertFalse(f.backendEvent.getResult().isAllowed());
        }
        ProxySettings old = new ProxySettings(true, 9, false, 2, 0, Duration.ZERO, Duration.ZERO, Set.of(), false);
        ConnectionLimiter limiter = new ConnectionLimiter(old);
        var lease = limiter.acquire("test-ip", 1);
        assertNotNull(lease);
        limiter.reconfigure(new ProxySettings(true, 9, false, 1, 0, Duration.ZERO, Duration.ZERO, Set.of(), true));
        assertNull(limiter.acquire("test-ip", 2), "reload must not forget an existing live connection");
        lease.close();
        var replacement = limiter.acquire("test-ip", 3);
        assertNotNull(replacement);
        replacement.close();
    }

    private static void reload(ZstdNetVelocityPlugin plugin) throws Exception {
        var method = plugin.getClass().getDeclaredMethod("reloadSettings"); method.setAccessible(true);
        try { method.invoke(plugin); }
        catch (InvocationTargetException error) {
            if (error.getCause() instanceof Exception cause) throw cause;
            throw error;
        }
    }
}
