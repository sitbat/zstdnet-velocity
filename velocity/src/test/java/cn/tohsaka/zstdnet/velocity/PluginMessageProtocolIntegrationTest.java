package cn.tohsaka.zstdnet.velocity;

import cn.tohsaka.zstdnet.transport.TransportConnection;
import com.velocitypowered.api.network.ProtocolState;
import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.proxy.config.ProxyConfig;
import com.velocitypowered.api.event.player.ServerPreConnectEvent;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.natives.util.Natives;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.netty.MinecraftCipherDecoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftCipherEncoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftCompressDecoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftCompressorAndLengthEncoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftDecoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftEncoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftVarintFrameDecoder;
import com.velocitypowered.proxy.protocol.packet.PluginMessagePacket;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.concurrent.CompletableFuture;
import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** Uses the fixed official runtime's packet registries and byte handlers, including the plugin's interceptor. */
class PluginMessageProtocolIntegrationTest {
    private static final ProtocolVersion[] VERSIONS = {
            ProtocolVersion.MINECRAFT_1_21_11, ProtocolVersion.MINECRAFT_26_3};

    static Stream<Arguments> payloadCases() {
        return Arrays.stream(VERSIONS).flatMap(version -> Stream.of(StateRegistry.CONFIG, StateRegistry.PLAY)
                .flatMap(state -> Stream.of(ProtocolUtils.Direction.SERVERBOUND, ProtocolUtils.Direction.CLIENTBOUND)
                        .map(direction -> Arguments.of(version, state, direction))));
    }

    @ParameterizedTest(name = "custom payload registry and encrypted roundtrip: {0}, {1}, {2}")
    @MethodSource("payloadCases")
    void nativeCustomPayloadRoundtripUsesTheCorrectVersionedPacketRegistry(
            ProtocolVersion version, StateRegistry state, ProtocolUtils.Direction direction) throws Exception {
        EmbeddedChannel sender = sender(version, state, direction);
        EmbeddedChannel receiver = receiver(version, state, direction);
        try {
            VelocityAccess access = new VelocityAccess();
            PluginMessagePacket packet = (PluginMessagePacket) access.packet(ControlMessage.HELLO);
            int expectedId = state == StateRegistry.CONFIG
                    ? (direction == ProtocolUtils.Direction.SERVERBOUND ? 0x02 : 0x01)
                    : (direction == ProtocolUtils.Direction.CLIENTBOUND ? 0x18
                        : version == ProtocolVersion.MINECRAFT_26_3 ? 0x16 : 0x15);
            var registry = state.getProtocolRegistry(direction, version);
            assertTrue(registry.containsPacket(packet));
            assertEquals(expectedId, registry.getPacketId(packet));
            assertInstanceOf(PluginMessagePacket.class, registry.createPacket(expectedId));
            assertTrue(version.isSupported());
            if (version == ProtocolVersion.MINECRAFT_26_3) assertEquals(777, version.getProtocol());

            sender.writeAndFlush(packet);
            transferFragmented(sender, receiver);
            PluginMessagePacket decoded = receiver.readInbound();
            assertNotNull(decoded);
            try {
                assertTrue(access.controlPacket(decoded));
                assertEquals(new ControlMessage(ControlMessage.HELLO, ControlMessage.VERSION),
                        ControlMessage.decode(access.payload(decoded)));
            } finally { decoded.release(); }
            assertNull(receiver.readInbound());
        } finally {
            sender.finishAndReleaseAll();
            receiver.finishAndReleaseAll();
        }
    }

    @ParameterizedTest(name = "real CONFIG HELLO interceptor consumes control before handler: {0}")
    @EnumSource(value = ProtocolVersion.class, names = {"MINECRAFT_1_21_11", "MINECRAFT_26_3"})
    void configHelloReachesTheProductionInterceptorWithoutLeakingOrPrematureActivation(ProtocolVersion version)
            throws Exception {
        EmbeddedChannel sender = sender(version, StateRegistry.CONFIG, ProtocolUtils.Direction.SERVERBOUND);
        EmbeddedChannel receiver = receiver(version, StateRegistry.CONFIG, ProtocolUtils.Direction.SERVERBOUND);
        try {
            VelocityAccess access = new VelocityAccess();
            Object session = installProductionInterceptor(receiver, access);
            var playerField = session.getClass().getDeclaredField("player"); playerField.setAccessible(true);
            Player player = (Player) playerField.get(session);
            var event = new ServerPreConnectEvent(player, fake(RegisteredServer.class, (name, args) -> null), null);
            CompletableFuture<Void> admitted = new CompletableFuture<>();
            var await = session.getClass().getDeclaredMethod("awaitBackendAdmission", ServerPreConnectEvent.class, CompletableFuture.class);
            await.setAccessible(true); await.invoke(session, event, admitted);
            assertFalse(admitted.isDone(), "no backend is allowed before CONFIG HELLO");
            sender.writeAndFlush(access.packet(ControlMessage.HELLO));
            transferFragmented(sender, receiver);
            assertTrue(admitted.isDone(), "the real encrypted CONFIG HELLO must release the backend event");
            assertTrue(event.getResult().isAllowed());

            var hello = session.getClass().getDeclaredField("hello");
            hello.setAccessible(true);
            assertTrue(hello.getBoolean(session), "the actual Session must receive the CONFIG HELLO");
            assertNull(receiver.readInbound(), "the reserved control must not reach the downstream handler/backend");
            assertNull(receiver.readOutbound(), "CONFIG must not send PREPARE or ACTIVATE");
            assertFalse(TransportConnection.isActive(receiver), "authentication/PLAY boundary must remain intact");
            assertNull(receiver.pipeline().get("zstdnet-proxy-decode"));
        } finally {
            sender.finishAndReleaseAll();
            receiver.finishAndReleaseAll();
        }
    }

    private static Object installProductionInterceptor(EmbeddedChannel channel, VelocityAccess access) throws Exception {
        ProxyConfig configuration = fake(ProxyConfig.class, (method, arguments) ->
                method.equals("isOnlineMode") ? true : null);
        ProxyServer proxy = fake(ProxyServer.class, (method, arguments) ->
                method.equals("getConfiguration") ? configuration : null);
        Player player = fake(Player.class, (method, arguments) -> switch (method) {
            case "isOnlineMode" -> true;
            case "getProtocolState" -> ProtocolState.CONFIGURATION;
            case "getUsername" -> "ConfigHello";
            case "getUniqueId" -> UUID.fromString("00000000-0000-0000-0000-000000000001");
            default -> null;
        });
        ZstdNetVelocityPlugin plugin = new ZstdNetVelocityPlugin(proxy,
                LoggerFactory.getLogger(PluginMessageProtocolIntegrationTest.class), Path.of("unused-test-directory"));
        var enabled = ZstdNetVelocityPlugin.class.getDeclaredField("enabled");
        enabled.setAccessible(true);
        enabled.setBoolean(plugin, true);
        var settings = ZstdNetVelocityPlugin.class.getDeclaredField("settings"); settings.setAccessible(true);
        settings.set(plugin, new ProxySettings(true, 9, false, 64, 50,
                Duration.ofSeconds(10), Duration.ofMinutes(1), Set.of(), true));
        Class<?> sessionType = Class.forName(ZstdNetVelocityPlugin.class.getName() + "$Session");
        var sessionConstructor = sessionType.getDeclaredConstructor(
                ZstdNetVelocityPlugin.class, Player.class, Channel.class, VelocityAccess.class);
        sessionConstructor.setAccessible(true);
        Object session = sessionConstructor.newInstance(plugin, player, channel, access);
        Class<?> interceptorType = Class.forName(ZstdNetVelocityPlugin.class.getName() + "$ControlInterceptor");
        var interceptorConstructor = interceptorType.getDeclaredConstructor(
                ZstdNetVelocityPlugin.class, sessionType, VelocityAccess.class);
        interceptorConstructor.setAccessible(true);
        channel.pipeline().addLast("handler", new ChannelInboundHandlerAdapter());
        channel.pipeline().addBefore("handler", "zstdnet-proxy-control",
                (ChannelHandler) interceptorConstructor.newInstance(plugin, session, access));
        return session;
    }

    private static EmbeddedChannel sender(ProtocolVersion version, StateRegistry state,
                                          ProtocolUtils.Direction direction) throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        var cipherFactory = Natives.cipher.get();
        channel.pipeline().addLast("cipher-encoder", new MinecraftCipherEncoder(cipherFactory.forEncryption(key())));
        channel.pipeline().addLast("compression-encoder", new MinecraftCompressorAndLengthEncoder(256, Natives.compress.get().create(3)));
        MinecraftEncoder encoder = new MinecraftEncoder(direction);
        encoder.setProtocolVersion(version);
        encoder.setState(state);
        channel.pipeline().addLast("minecraft-encoder", encoder);
        return channel;
    }

    private static EmbeddedChannel receiver(ProtocolVersion version, StateRegistry state,
                                            ProtocolUtils.Direction direction) throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        MinecraftVarintFrameDecoder frame = new MinecraftVarintFrameDecoder(direction);
        frame.setProtocolVersion(version);
        frame.setState(state);
        frame.setCompressionEnabled(true);
        var cipherFactory = Natives.cipher.get();
        channel.pipeline().addLast("cipher-decoder", new MinecraftCipherDecoder(cipherFactory.forDecryption(key())));
        channel.pipeline().addLast("frame-decoder", frame);
        channel.pipeline().addLast("cipher-encoder", new MinecraftCipherEncoder(cipherFactory.forEncryption(key())));
        channel.pipeline().addLast("compression-decoder", new MinecraftCompressDecoder(256, Natives.compress.get().create(3), direction));
        MinecraftDecoder decoder = new MinecraftDecoder(direction);
        decoder.setProtocolVersion(version);
        decoder.setState(state);
        channel.pipeline().addLast("minecraft-decoder", decoder);
        return channel;
    }

    private static SecretKeySpec key() {
        byte[] secret = new byte[16]; Arrays.fill(secret, (byte) 71);
        return new SecretKeySpec(secret, "AES");
    }

    private static void transferFragmented(EmbeddedChannel sender, EmbeddedChannel receiver) {
        ByteBuf wire;
        while ((wire = sender.readOutbound()) != null) {
            try {
                while (wire.isReadable()) receiver.writeInbound(wire.readRetainedSlice(Math.min(2, wire.readableBytes())));
            } finally { wire.release(); }
        }
        receiver.checkException();
    }

    @SuppressWarnings("unchecked")
    private static <T> T fake(Class<T> type, Answer answer) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, arguments) -> {
            if (method.getName().equals("toString")) return "fixture-" + type.getSimpleName();
            if (method.getName().equals("hashCode")) return System.identityHashCode(proxy);
            if (method.getName().equals("equals")) return proxy == arguments[0];
            return answer.value(method.getName(), arguments);
        });
    }

    private interface Answer { Object value(String method, Object[] arguments); }
}
