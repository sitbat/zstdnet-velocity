package cn.tohsaka.zstdnet.velocity;

import cn.tohsaka.zstdnet.transport.TransportConnection;
import cn.tohsaka.zstdnet.transport.TransportStats;
import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.natives.compression.VelocityCompressor;
import com.velocitypowered.natives.util.BufferPreference;
import com.velocitypowered.natives.util.Natives;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.netty.MinecraftCipherDecoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftCipherEncoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftCompressDecoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftCompressorAndLengthEncoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftVarintFrameDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.DataFormatException;

import static org.junit.jupiter.api.Assertions.*;

/** Runs the shipped Velocity 4.3.0-44 handlers, rather than replicas of its native byte path. */
class NativeTransportIntegrationTest {
    private static final TransportConnection.Layout LAYOUT = new TransportConnection.Layout(
        "frame-decoder", "minecraft-encoder", "compression-decoder", "compression-encoder", "frame-encoder", 2 * 1024 * 1024);

    @ParameterizedTest(name = "encrypted bidirectional upgrade: {0}")
    @EnumSource(value = ProtocolVersion.class, names = {"MINECRAFT_1_21_11", "MINECRAFT_26_3"})
    void actualVelocityZlibAndCipherHandlersSupportAnEncryptedBidirectionalUpgrade(ProtocolVersion version) throws Exception {
        Peer proxy = peer(ProtocolUtils.Direction.SERVERBOUND, version);
        Peer client = peer(ProtocolUtils.Direction.CLIENTBOUND, version);
        try {
            byte[] vanillaDown = packet(700, 17);
            byte[] vanillaUp = packet(600, 21);
            proxy.channel.writeAndFlush(Unpooled.wrappedBuffer(vanillaDown));
            client.channel.writeAndFlush(Unpooled.wrappedBuffer(vanillaUp));
            transfer(proxy.channel, client.channel, 1);
            transfer(client.channel, proxy.channel, 2);
            assertPacket(client.channel, vanillaDown);
            assertPacket(proxy.channel, vanillaUp);
            assertEquals(1, proxy.nativeResource.encodes);
            assertEquals(1, proxy.nativeResource.decodes);
            assertEquals(1, client.nativeResource.encodes);
            assertEquals(1, client.nativeResource.decodes);

            prepare(proxy);
            prepare(client);
            byte[] activation = packet(400, 34);
            proxy.channel.writeAndFlush(Unpooled.wrappedBuffer(activation));
            TransportConnection.enableOutbound(proxy.channel, 3, LAYOUT, proxy.stats);
            for (int i = 0; i < 3; i++) proxy.channel.writeAndFlush(Unpooled.wrappedBuffer(packet(70 * 1024 + i, 40 + i)));
            flush(proxy.channel);
            transfer(proxy.channel, client.channel, 3);
            assertPacket(client.channel, activation);
            for (int i = 0; i < 3; i++) assertPacket(client.channel, packet(70 * 1024 + i, 40 + i));

            TransportConnection.enableOutbound(client.channel, 3, LAYOUT, client.stats);
            for (int i = 0; i < 50; i++) client.channel.writeAndFlush(Unpooled.wrappedBuffer(packet(200 + i, i)));
            flush(client.channel);
            transfer(client.channel, proxy.channel, 7);
            for (int i = 0; i < 50; i++) assertPacket(proxy.channel, packet(200 + i, i));

            assertEquals(2, proxy.nativeResource.encodes, "the activation is the last zlib packet");
            assertEquals(2, client.nativeResource.decodes);
            assertEquals(1, proxy.nativeResource.decodes);
            assertEquals(1, client.nativeResource.encodes);
            assertEquals(0, proxy.nativeResource.closes, "keep the shared native object alive until disconnect");
            assertEquals(0, client.nativeResource.closes);
            assertSame(proxy.nativeResource, compressorField(proxy.channel.pipeline().get("compression-encoder")));
            assertSame(proxy.nativeResource, compressorField(proxy.channel.pipeline().get("compression-decoder")));
            assertEquals(proxy.stats.txRaw(), client.stats.rxRaw());
            assertEquals(proxy.stats.txWire(), client.stats.rxWire());
            assertEquals(client.stats.txRaw(), proxy.stats.rxRaw());
            assertEquals(client.stats.txWire(), proxy.stats.rxWire());
            assertTrue(proxy.stats.txWire() < proxy.stats.txRaw() / 10);
            assertNull(proxy.channel.readInbound());
            assertNull(client.channel.readInbound());
        } finally {
            proxy.channel.finishAndReleaseAll();
            client.channel.finishAndReleaseAll();
        }
        assertTrue(proxy.nativeResource.closes >= 1);
        assertTrue(client.nativeResource.closes >= 1);
        System.out.println("Velocity native factories: compression=" + Natives.compress.get().getClass().getName()
            + ", cipher=" + Natives.cipher.get().getClass().getName());
    }

    @ParameterizedTest(name = "partial vanilla splitter migration: {0}")
    @EnumSource(value = ProtocolVersion.class, names = {"MINECRAFT_1_21_11", "MINECRAFT_26_3"})
    void actualVelocitySplitterRetainsPartialVanillaBytesDuringDecoderInstallation(ProtocolVersion version) throws Exception {
        Peer proxy = peer(ProtocolUtils.Direction.SERVERBOUND, version);
        Peer client = peer(ProtocolUtils.Direction.CLIENTBOUND, version);
        try {
            byte[] payload = packet(4000, 22);
            client.channel.writeAndFlush(Unpooled.wrappedBuffer(payload));
            ByteBuf encrypted = client.channel.readOutbound();
            assertNotNull(encrypted);
            proxy.channel.writeInbound(encrypted.readRetainedSlice(2));
            assertNull(proxy.channel.readInbound());
            prepare(proxy);
            while (encrypted.isReadable()) proxy.channel.writeInbound(encrypted.readRetainedSlice(Math.min(3, encrypted.readableBytes())));
            encrypted.release();
            assertPacket(proxy.channel, payload);
            assertEquals(1, proxy.nativeResource.decodes);
            assertEquals(0, proxy.nativeResource.closes);
        } finally {
            proxy.channel.finishAndReleaseAll();
            client.channel.finishAndReleaseAll();
        }
    }

    @ParameterizedTest(name = "native receive limiter after upgrade: {0}")
    @EnumSource(value = ProtocolVersion.class, names = {"MINECRAFT_1_21_11", "MINECRAFT_26_3"})
    void zstdPacketsRemainSubjectToTheConfiguredVelocityPacketLimiter(ProtocolVersion version) throws Exception {
        Peer proxy = peer(ProtocolUtils.Direction.SERVERBOUND, version);
        Peer client = peer(ProtocolUtils.Direction.CLIENTBOUND, version);
        try {
            AtomicInteger accounted = new AtomicInteger();
            ((MinecraftVarintFrameDecoder) proxy.channel.pipeline().get("frame-decoder"))
                .setPacketLimiter(bytes -> accounted.incrementAndGet() <= 2);
            AtomicInteger ready = new AtomicInteger();
            var account = new VelocityAccess().inboundAccount(proxy.channel);
            TransportConnection.prepare(proxy.channel, LAYOUT, proxy.stats, account, ready::incrementAndGet);
            proxy.channel.runPendingTasks();
            assertEquals(1, ready.get());
            prepare(client);
            TransportConnection.enableOutbound(client.channel, 3, LAYOUT, client.stats);
            for (int i = 0; i < 3; i++) client.channel.writeAndFlush(Unpooled.wrappedBuffer(packet(80, 10 + i)));
            flush(client.channel);
            assertThrows(Exception.class, () -> transfer(client.channel, proxy.channel, 3));
            assertEquals(3, accounted.get(), "all decoded packets must reach Velocity's existing limiter");
            assertPacket(proxy.channel, packet(80, 10));
            assertPacket(proxy.channel, packet(80, 11));
            assertNull(proxy.channel.readInbound());
            assertFalse(proxy.channel.isActive());
        } finally {
            client.channel.finishAndReleaseAll();
            try { proxy.channel.finishAndReleaseAll(); } catch (Exception ignored) { }
        }
    }

    private static Object compressorField(Object handler) throws Exception {
        var field = handler.getClass().getDeclaredField("compressor");
        field.setAccessible(true);
        return field.get(handler);
    }

    private static Peer peer(ProtocolUtils.Direction inboundDirection, ProtocolVersion version) throws Exception {
        byte[] secret = new byte[16];
        Arrays.fill(secret, (byte) 49);
        var key = new SecretKeySpec(secret, "AES");
        var cipherFactory = Natives.cipher.get();
        TrackingCompressor compressor = new TrackingCompressor(Natives.compress.get().create(3));
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        MinecraftVarintFrameDecoder frame = new MinecraftVarintFrameDecoder(inboundDirection);
        frame.setState(StateRegistry.PLAY);
        frame.setProtocolVersion(version);
        frame.setCompressionEnabled(true);
        channel.pipeline().addLast("cipher-decoder", new MinecraftCipherDecoder(cipherFactory.forDecryption(key)));
        channel.pipeline().addLast("frame-decoder", frame);
        channel.pipeline().addLast("cipher-encoder", new MinecraftCipherEncoder(cipherFactory.forEncryption(key)));
        channel.pipeline().addLast("compression-decoder", new MinecraftCompressDecoder(256, compressor, inboundDirection));
        channel.pipeline().addLast("compression-encoder", new MinecraftCompressorAndLengthEncoder(256, compressor));
        channel.pipeline().addLast("minecraft-decoder", new ChannelInboundHandlerAdapter());
        channel.pipeline().addLast("minecraft-encoder", new ChannelOutboundHandlerAdapter());
        return new Peer(channel, compressor, new TransportStats());
    }

    private static void prepare(Peer peer) {
        AtomicInteger ready = new AtomicInteger();
        TransportConnection.prepare(peer.channel, LAYOUT, peer.stats, ready::incrementAndGet);
        peer.channel.runPendingTasks();
        peer.channel.checkException();
        assertEquals(1, ready.get());
    }

    private static byte[] packet(int size, int seed) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) bytes[i] = (byte) ((i + seed) % 17);
        bytes[0] = 0;
        return bytes;
    }

    private static void flush(EmbeddedChannel channel) {
        channel.advanceTimeBy(2, TimeUnit.MILLISECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
        channel.checkException();
    }

    private static void transfer(EmbeddedChannel sender, EmbeddedChannel receiver, int slice) {
        ByteBuf combined = Unpooled.buffer();
        ByteBuf wire;
        while ((wire = sender.readOutbound()) != null) {
            combined.writeBytes(wire);
            wire.release();
        }
        try {
            while (combined.isReadable()) receiver.writeInbound(combined.readRetainedSlice(Math.min(slice, combined.readableBytes())));
            receiver.checkException();
        } finally { combined.release(); }
    }

    private static void assertPacket(EmbeddedChannel channel, byte[] expected) {
        ByteBuf actual = channel.readInbound();
        assertNotNull(actual);
        try {
            byte[] bytes = new byte[actual.readableBytes()];
            actual.readBytes(bytes);
            assertArrayEquals(expected, bytes);
        } finally { actual.release(); }
    }

    private record Peer(EmbeddedChannel channel, TrackingCompressor nativeResource, TransportStats stats) { }

    private static final class TrackingCompressor implements VelocityCompressor {
        private final VelocityCompressor delegate;
        private int encodes;
        private int decodes;
        private int closes;
        private TrackingCompressor(VelocityCompressor delegate) { this.delegate = delegate; }
        @Override public void inflate(ByteBuf source, ByteBuf output, int size) throws DataFormatException {
            assertEquals(0, closes, "native decompression after disposal");
            decodes++;
            delegate.inflate(source, output, size);
        }
        @Override public void deflate(ByteBuf source, ByteBuf output) throws DataFormatException {
            assertEquals(0, closes, "native compression after disposal");
            encodes++;
            delegate.deflate(source, output);
        }
        @Override public BufferPreference preferredBufferType() { return delegate.preferredBufferType(); }
        @Override public void close() {
            // The real pipeline invokes both handlers' cleanup on the same shared compressor.
            if (closes++ == 0) delegate.close();
        }
    }
}
