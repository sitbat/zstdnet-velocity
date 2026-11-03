package cn.tohsaka.zstdnet.velocity;

import cn.tohsaka.zstdnet.transport.TransportConnection;
import com.velocitypowered.api.event.connection.PreLoginEvent;
import com.velocitypowered.api.event.player.ServerPreConnectEvent;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.proxy.protocol.packet.DisconnectPacket;
import io.netty.channel.ChannelFutureListener;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import com.velocitypowered.api.network.ProtocolState;
import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.InboundConnection;
import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.proxy.config.ProxyConfig;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.netty.MinecraftEncoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftVarintFrameDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import net.kyori.adventure.text.Component;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the production Session, native packet encoder and timer, not a parallel policy model. */
class RequiredClientTest {
    @TempDir Path directory;

    @Test void configurationDefaultsOffUpgradesAndPreservesRequiredModeWhenGrantingReports() throws Exception {
        assertFalse(ProxySettings.startupPolicy(directory).requireClient());
        assertFalse(ProxySettings.load(directory).requireClient());
        Path file = directory.resolve("zstdnet.properties");
        Files.writeString(file, "# Keep settings\nlevel=6\nrequire_client=true\n");
        ProxySettings loaded = ProxySettings.load(directory);
        assertTrue(loaded.requireClient());
        assertEquals(6, loaded.level());
        assertTrue(loaded.allowReport(directory, UUID.randomUUID()).requireClient());
        assertTrue(ProxySettings.load(directory).requireClient());
        Files.writeString(file, "require_client=typo\n");
        assertTrue(ProxySettings.startupPolicy(directory).requireClient());
        assertThrows(java.io.IOException.class, () -> ProxySettings.load(directory));
        Files.writeString(file, "require_client=true\nlevel=99\n");
        assertTrue(ProxySettings.startupPolicy(directory).requireClient());
        assertThrows(java.io.IOException.class, () -> ProxySettings.load(directory));
    }

    @Test void vanillaClientIsRejectedBeforeBackendWithInstallMessage() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.awaitAdmission();
            f.advance(9);
            assertNull(f.disconnected.get());
            f.advance(1);
            assertNotNull(f.disconnected.get());
            assertTrue(f.disconnected.get().toString().contains("zstdnet-client/releases"));
            assertEquals("REJECTED", f.stage());
        }
    }

    @Test void optionalVanillaClientStillConnects() throws Exception {
        try (Fixture f = new Fixture(false)) {
            f.awaitAdmission();
            f.advance(60);
            assertNull(f.disconnected.get());
            assertEquals("VANILLA", f.stage());
            assertTrue(f.channel.isActive());
        }
    }

    @Test void missingHelloInConfigurationTimesOutBeforeAnyBackendAdmission() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.protocol.set(ProtocolState.CONFIGURATION);
            CompletableFuture<Void> waiting = f.awaitAdmission();
            assertFalse(waiting.isDone());
            f.advance(10);
            assertTrue(waiting.isDone());
            assertTrue(f.backendEvent.getResult().getServer().isEmpty());
            assertNotNull(f.disconnected.get());
        }
    }

    @Test void declaredHelloWithoutReadyCannotBypassRequirement() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.receive(ControlMessage.HELLO, ControlMessage.VERSION);
            assertEquals("WAIT_READY", f.stage());
            f.advance(10);
            assertNotNull(f.disconnected.get());
            assertFalse(TransportConnection.isActive(f.channel));
        }
    }

    @Test void incompatibleAndMalformedClientsAreRejectedOnlyWhenRequired() throws Exception {
        for (boolean required : new boolean[]{false, true}) {
            for (byte[] bytes : new byte[][]{new byte[]{0, 99}, new byte[]{(byte) 0x80}}) {
                try (Fixture f = new Fixture(required)) {
                    f.receive(bytes);
                    assertEquals(required, f.disconnected.get() != null);
                    assertEquals("REJECTED", f.stage());
                }
            }
        }
    }

    @Test void successfulHandshakeEnablesTransportAndCancelsRejection() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.awaitAdmission();
            f.receive(ControlMessage.HELLO, ControlMessage.VERSION);
            f.receive(ControlMessage.READY, ControlMessage.VERSION);
            f.channel.runPendingTasks();
            assertEquals("ACTIVE", f.stage());
            assertTrue(TransportConnection.isActive(f.channel));
            f.advance(60);
            assertNull(f.disconnected.get());
        }
    }

    @Test void requiredGateTimeoutDiscardsWorldDataAndDoesNotBufferTheDisconnect() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.receive(ControlMessage.HELLO, ControlMessage.VERSION);
            f.drainOutbound();
            ByteBuf world = Unpooled.buffer().writeByte(42);
            var pending = f.channel.writeAndFlush(world);
            assertFalse(pending.isDone());
            f.advance(5);
            assertNotNull(f.disconnected.get());
            assertEquals(0, world.refCnt());
            assertFalse(pending.isSuccess());
            f.assertGuidedDisconnect();
            assertNull(f.channel.pipeline().context(f.gate));
        }
    }

    @Test void requiredModeRejectsLoginWhenTransportIsDisabled() throws Exception {
        try (Fixture f = new Fixture(true)) {
            set(f.plugin, "enabled", false);
            PreLoginEvent event = new PreLoginEvent(fake(InboundConnection.class, (name, args) -> null), "fixture", UUID.randomUUID());
            f.plugin.preLogin(event);
            assertFalse(event.getResult().isAllowed());
        }
    }

    @Test void requiredQueueOverflowRejectsInsteadOfFallingBackToVanilla() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.receive(ControlMessage.HELLO, ControlMessage.VERSION);
            f.drainOutbound();
            ByteBuf world = Unpooled.buffer((int) InitialSyncGate.MAX_BYTES + 1)
                    .writerIndex((int) InitialSyncGate.MAX_BYTES + 1);
            var pending = f.channel.writeAndFlush(world);
            assertNotNull(f.disconnected.get());
            assertEquals(0, world.refCnt());
            assertFalse(pending.isSuccess());
            f.assertGuidedDisconnect();
        }
    }

    @Test void helloDuringConfigurationContinuesNegotiatingWhenPlayBecomesAvailable() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.protocol.set(ProtocolState.CONFIGURATION);
            f.awaitAdmission();
            f.receive(ControlMessage.HELLO, ControlMessage.VERSION);
            assertEquals("VANILLA", f.stage());
            f.advance(60);
            assertNull(f.disconnected.get());
            f.protocol.set(ProtocolState.PLAY);
            f.advance(1);
            assertEquals("WAIT_READY", f.stage());
            f.receive(ControlMessage.READY, ControlMessage.VERSION);
            f.channel.runPendingTasks();
            assertEquals("ACTIVE", f.stage());
            f.advance(60);
            assertNull(f.disconnected.get());
        }
    }

    static final class Fixture implements AutoCloseable {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final AtomicReference<Component> disconnected = new AtomicReference<>();
        final AtomicReference<ProtocolState> protocol = new AtomicReference<>(ProtocolState.PLAY);
        final ZstdNetVelocityPlugin plugin;
        final Object session;
        final InitialSyncGate gate;
        final Player player;
        ServerPreConnectEvent backendEvent;

        Fixture(boolean required) throws Exception { this(required, Path.of("unused")); }
        Fixture(boolean required, Path directory) throws Exception {
            channel.freezeTime();
            ProxyConfig config = fake(ProxyConfig.class, (name, args) -> name.equals("isOnlineMode") ? true : null);
            ProxyServer proxy = fake(ProxyServer.class, (name, args) -> name.equals("getConfiguration") ? config : null);
            player = fake(Player.class, (name, args) -> switch (name) {
                case "isOnlineMode" -> true;
                case "getProtocolState" -> protocol.get();
                case "getUsername" -> "fixture";
                case "getCurrentServer" -> Optional.empty();
                case "disconnect" -> {
                    Component reason = (Component) args[0];
                    disconnected.compareAndSet(null, reason);
                    StateRegistry state = protocol.get() == ProtocolState.CONFIGURATION ? StateRegistry.CONFIG : StateRegistry.PLAY;
                    ((MinecraftEncoder) channel.pipeline().get("minecraft-encoder")).setState(state);
                    channel.writeAndFlush(DisconnectPacket.create(reason, ProtocolVersion.MINECRAFT_1_21_11, state))
                            .addListener(ChannelFutureListener.CLOSE);
                    yield null;
                }
                default -> null;
            });
            plugin = new ZstdNetVelocityPlugin(proxy, LoggerFactory.getLogger(RequiredClientTest.class), directory);
            set(plugin, "enabled", true);
            set(plugin, "settings", new ProxySettings(true, 9, false, 64, 50,
                    Duration.ofSeconds(10), Duration.ofMinutes(1), Set.of(), required));
            MinecraftVarintFrameDecoder frame = new MinecraftVarintFrameDecoder(ProtocolUtils.Direction.SERVERBOUND);
            frame.setProtocolVersion(ProtocolVersion.MINECRAFT_1_21_11);
            frame.setState(StateRegistry.PLAY);
            channel.pipeline().addLast("cipher-decoder", new ChannelInboundHandlerAdapter());
            channel.pipeline().addLast("cipher-encoder", new ChannelOutboundHandlerAdapter());
            channel.pipeline().addLast("frame-decoder", frame);
            channel.pipeline().addLast("frame-encoder", new ChannelOutboundHandlerAdapter());
            MinecraftEncoder encoder = new MinecraftEncoder(ProtocolUtils.Direction.CLIENTBOUND);
            encoder.setProtocolVersion(ProtocolVersion.MINECRAFT_1_21_11);
            encoder.setState(StateRegistry.PLAY);
            channel.pipeline().addLast("minecraft-encoder", encoder);
            channel.pipeline().addLast("handler", new ChannelInboundHandlerAdapter());
            Class<?> type = Class.forName(ZstdNetVelocityPlugin.class.getName() + "$Session");
            var constructor = type.getDeclaredConstructor(ZstdNetVelocityPlugin.class, Player.class, Channel.class, VelocityAccess.class);
            constructor.setAccessible(true);
            session = constructor.newInstance(plugin, player, channel, new VelocityAccess());
            var field = type.getDeclaredField("gate"); field.setAccessible(true);
            gate = (InitialSyncGate) field.get(session);
            channel.pipeline().addBefore("minecraft-encoder", "gate", gate);
            var sessionsField = plugin.getClass().getDeclaredField("sessions"); sessionsField.setAccessible(true);
            @SuppressWarnings("unchecked") var sessions = (java.util.Map<Channel, Object>) sessionsField.get(plugin);
            sessions.put(channel, session);
        }

        CompletableFuture<Void> awaitAdmission() throws Exception {
            ((MinecraftEncoder) channel.pipeline().get("minecraft-encoder")).setState(
                    protocol.get() == ProtocolState.CONFIGURATION ? StateRegistry.CONFIG : StateRegistry.PLAY);
            backendEvent = new ServerPreConnectEvent(player, fake(RegisteredServer.class, (name, args) -> null));
            CompletableFuture<Void> checked = new CompletableFuture<>();
            var method = session.getClass().getDeclaredMethod("awaitBackendAdmission", ServerPreConnectEvent.class, CompletableFuture.class);
            method.setAccessible(true); method.invoke(session, backendEvent, checked);
            return checked;
        }
        void assertGuidedDisconnect() {
            ByteBuf wire = channel.readOutbound();
            assertNotNull(wire, "the guided disconnect must reach the wire");
            var decoder = new com.velocitypowered.proxy.protocol.netty.MinecraftDecoder(ProtocolUtils.Direction.CLIENTBOUND);
            decoder.setProtocolVersion(ProtocolVersion.MINECRAFT_1_21_11);
            decoder.setState(protocol.get() == ProtocolState.CONFIGURATION ? StateRegistry.CONFIG : StateRegistry.PLAY);
            EmbeddedChannel receiver = new EmbeddedChannel(decoder);
            try {
                receiver.writeInbound(wire);
                DisconnectPacket packet = receiver.readInbound();
                assertNotNull(packet);
                assertTrue(packet.getReason().getComponent().toString().contains("zstdnet-client/releases"));
                assertNull(receiver.readInbound());
                assertNull(channel.readOutbound(), "only one disconnect should be emitted");
            } finally { receiver.finishAndReleaseAll(); }
        }
        void drainOutbound() {
            Object message;
            while ((message = channel.readOutbound()) != null) io.netty.util.ReferenceCountUtil.release(message);
        }
        void invoke(String name) throws Exception {
            var method = session.getClass().getDeclaredMethod(name); method.setAccessible(true); method.invoke(session);
        }
        void receive(int opcode, int version) throws Exception { receive(new byte[]{(byte) opcode, (byte) version}); }
        void receive(byte[] bytes) throws Exception {
            var method = session.getClass().getDeclaredMethod("receive", byte[].class, boolean.class);
            method.setAccessible(true); method.invoke(session, bytes, true);
        }
        String stage() throws Exception {
            var field = session.getClass().getDeclaredField("stage"); field.setAccessible(true); return field.get(session).toString();
        }
        void advance(long seconds) { channel.advanceTimeBy(seconds, TimeUnit.SECONDS); channel.runScheduledPendingTasks(); channel.runPendingTasks(); }
        @Override public void close() throws Exception { invoke("finish"); channel.finishAndReleaseAll(); }
    }

    static void set(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name); field.setAccessible(true); field.set(target, value);
    }
    @SuppressWarnings("unchecked") static <T> T fake(Class<T> type, Answer answer) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> switch (method.getName()) {
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            case "toString" -> "fixture-" + type.getSimpleName();
            default -> answer.value(method.getName(), args);
        });
    }
    interface Answer { Object value(String name, Object[] args); }
}
