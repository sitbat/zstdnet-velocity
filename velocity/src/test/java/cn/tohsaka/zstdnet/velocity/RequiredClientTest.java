package cn.tohsaka.zstdnet.velocity;

import cn.tohsaka.zstdnet.transport.TransportConnection;
import com.velocitypowered.api.event.connection.PreLoginEvent;
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

    @Test void vanillaClientIsRejectedAtPlayDeadlineWithInstallMessage() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.invoke("awaitRequiredClient");
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
            f.invoke("awaitRequiredClient");
            f.advance(60);
            assertNull(f.disconnected.get());
            assertEquals("VANILLA", f.stage());
            assertTrue(f.channel.isActive());
        }
    }

    @Test void configurationLoadingDoesNotConsumeThePlayDeadline() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.protocol.set(ProtocolState.CONFIGURATION);
            f.invoke("awaitRequiredClient");
            f.advance(60);
            assertNull(f.disconnected.get());
            f.protocol.set(ProtocolState.PLAY);
            f.advance(1); // polling sees the transition and arms the PLAY timer
            f.advance(9);
            assertNull(f.disconnected.get());
            f.advance(1);
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
            f.invoke("awaitRequiredClient");
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
            ByteBuf world = Unpooled.buffer().writeByte(42);
            var pending = f.channel.writeAndFlush(world);
            assertFalse(pending.isDone());
            f.advance(5);
            assertNotNull(f.disconnected.get());
            assertEquals(0, world.refCnt());
            assertFalse(pending.isSuccess());
            assertNull(f.channel.readOutbound(), "rejected world data must not be replayed");
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
            ByteBuf world = Unpooled.buffer((int) InitialSyncGate.MAX_BYTES + 1)
                    .writerIndex((int) InitialSyncGate.MAX_BYTES + 1);
            var pending = f.channel.writeAndFlush(world);
            assertNotNull(f.disconnected.get());
            assertEquals(0, world.refCnt());
            assertFalse(pending.isSuccess());
            assertNull(f.channel.readOutbound());
        }
    }

    @Test void helloDuringConfigurationContinuesNegotiatingWhenPlayBecomesAvailable() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.protocol.set(ProtocolState.CONFIGURATION);
            f.invoke("awaitRequiredClient");
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

    private static final class Fixture implements AutoCloseable {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final AtomicReference<Component> disconnected = new AtomicReference<>();
        final AtomicReference<ProtocolState> protocol = new AtomicReference<>(ProtocolState.PLAY);
        final ZstdNetVelocityPlugin plugin;
        final Object session;
        final InitialSyncGate gate;

        Fixture(boolean required) throws Exception {
            channel.freezeTime();
            ProxyConfig config = fake(ProxyConfig.class, (name, args) -> name.equals("isOnlineMode") ? true : null);
            ProxyServer proxy = fake(ProxyServer.class, (name, args) -> name.equals("getConfiguration") ? config : null);
            Player player = fake(Player.class, (name, args) -> switch (name) {
                case "isOnlineMode" -> true;
                case "getProtocolState" -> protocol.get();
                case "getUsername" -> "fixture";
                case "disconnect" -> { disconnected.set((Component) args[0]); yield null; }
                default -> null;
            });
            plugin = new ZstdNetVelocityPlugin(proxy, LoggerFactory.getLogger(RequiredClientTest.class), Path.of("unused"));
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

    private static void set(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name); field.setAccessible(true); field.set(target, value);
    }
    @SuppressWarnings("unchecked") private static <T> T fake(Class<T> type, Answer answer) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> switch (method.getName()) {
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            case "toString" -> "fixture-" + type.getSimpleName();
            default -> answer.value(method.getName(), args);
        });
    }
    private interface Answer { Object value(String name, Object[] args); }
}
