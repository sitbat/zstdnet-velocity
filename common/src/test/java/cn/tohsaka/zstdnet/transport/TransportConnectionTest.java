package cn.tohsaka.zstdnet.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.flow.FlowControlHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.ReferenceCounted;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the real byte path across vanilla zlib, Zstd, and a continuous AES stream. */
class TransportConnectionTest {
    private static final TransportConnection.Layout LAYOUT = new TransportConnection.Layout(
        "frame-decoder", "minecraft-encoder", "compression-decoder", "compression-encoder", "frame-encoder", 2 * 1024 * 1024);
    private static final TransportConnection.Layout CLIENT_LAYOUT = new TransportConnection.Layout(
        "splitter", "encoder", "decompress", "compress", "prepender", 8 * 1024 * 1024);

    @Test
    void encryptedBidirectionalUpgradeBypassesZlibWithoutClosingItsSharedResource() throws Exception {
        Peer server = peer(true);
        Peer client = peer(true);
        try {
            byte[] vanilla = packet(127, 1);
            server.channel.writeAndFlush(Unpooled.wrappedBuffer(vanilla));
            transfer(server.channel, client.channel, 1);
            assertPacket(client.channel, vanilla);
            assertEquals(1, server.nativeResource.encodes);
            assertEquals(1, client.nativeResource.decodes);

            prepare(server);
            prepare(client);
            byte[] activation = packet(17, 2);
            server.channel.writeAndFlush(Unpooled.wrappedBuffer(activation));
            TransportConnection.enableOutbound(server.channel, 3, LAYOUT, server.stats);
            byte[] firstCompressed = packet(96 * 1024, 3);
            server.channel.writeAndFlush(Unpooled.wrappedBuffer(firstCompressed));
            flush(server.channel);

            // One encrypted TCP read can contain the final vanilla activation and Zstd data.
            transfer(server.channel, client.channel, 1);
            assertPacket(client.channel, activation);
            assertPacket(client.channel, firstCompressed);
            assertEquals(2, server.nativeResource.encodes, "Zstd bytes must skip the old zlib encoder");
            assertEquals(2, client.nativeResource.decodes, "Zstd packets must skip the old zlib decoder");
            assertEquals(0, server.nativeResource.closes);
            assertEquals(0, client.nativeResource.closes);

            TransportConnection.enableOutbound(client.channel, 3, LAYOUT, client.stats);
            for (int i = 0; i < 40; i++) {
                client.channel.writeAndFlush(Unpooled.wrappedBuffer(packet(111 + i, 10 + i)));
            }
            flush(client.channel);
            transfer(client.channel, server.channel, 7);
            for (int i = 0; i < 40; i++) assertPacket(server.channel, packet(111 + i, 10 + i));
            assertEquals(0, client.nativeResource.encodes);
            assertEquals(0, server.nativeResource.decodes);
            assertTrue(TransportConnection.isActive(server.channel));
            assertTrue(TransportConnection.isActive(client.channel));
            assertTrue(server.stats.txRaw() > firstCompressed.length);
            assertEquals(server.stats.txRaw(), client.stats.rxRaw());
            assertEquals(server.stats.txWire(), client.stats.rxWire());
            assertEquals(client.stats.txRaw(), server.stats.rxRaw());
            assertEquals(client.stats.txWire(), server.stats.rxWire());
            assertEquals(0, server.nativeResource.closes);
            assertEquals(0, client.nativeResource.closes);
        } finally {
            server.channel.finishAndReleaseAll();
            client.channel.finishAndReleaseAll();
        }
    }

    @Test
    void preparedDecoderPreservesVanillaTrafficWhenPeerNeverActivates() throws Exception {
        Peer sender = peer(false);
        Peer receiver = peer(false);
        try {
            prepare(receiver);
            for (int i = 0; i < 8; i++) sender.channel.writeAndFlush(Unpooled.wrappedBuffer(packet(200 + i, i)));
            transfer(sender.channel, receiver.channel, 3);
            for (int i = 0; i < 8; i++) assertPacket(receiver.channel, packet(200 + i, i));
            assertFalse(TransportConnection.isActive(receiver.channel));
            assertEquals(8, receiver.nativeResource.decodes);
            assertEquals(0, receiver.nativeResource.closes);
        } finally {
            sender.channel.finishAndReleaseAll();
            receiver.channel.finishAndReleaseAll();
        }
    }

    @Test
    void installationMigratesAnIncompleteVanillaFrameFromTheExistingSplitter() throws Exception {
        Peer sender = peer(false);
        Peer receiver = peer(false);
        try {
            byte[] expected = packet(900, 91);
            sender.channel.writeAndFlush(Unpooled.wrappedBuffer(expected));
            ByteBuf wire = sender.channel.readOutbound();
            assertNotNull(wire);
            receiver.channel.writeInbound(wire.readRetainedSlice(2));
            assertNull(receiver.channel.readInbound());
            prepare(receiver);
            receiver.channel.writeInbound(wire);
            assertPacket(receiver.channel, expected);
            assertEquals(1, receiver.nativeResource.decodes);
        } finally {
            sender.channel.finishAndReleaseAll();
            receiver.channel.finishAndReleaseAll();
        }
    }

    @Test
    void closesFlushSmallPendingPacketsAndDoNotLeaveATimerWrite() throws Exception {
        Peer sender = peer(false);
        Peer receiver = peer(false);
        try {
            prepare(receiver);
            prepare(sender);
            TransportConnection.enableOutbound(sender.channel, 3, LAYOUT, sender.stats);
            byte[] expected = packet(47, 8);
            var future = sender.channel.writeAndFlush(Unpooled.wrappedBuffer(expected));
            assertNull(sender.channel.readOutbound());
            assertFalse(future.isDone());
            sender.channel.close();
            transfer(sender.channel, receiver.channel, 4);
            assertPacket(receiver.channel, expected);
            assertTrue(future.isSuccess());
            sender.channel.advanceTimeBy(10, TimeUnit.MILLISECONDS);
            sender.channel.runScheduledPendingTasks();
            assertNull(sender.channel.readOutbound());
        } finally {
            sender.channel.finishAndReleaseAll();
            receiver.channel.finishAndReleaseAll();
        }
    }

    @Test
    void rejectsAPacketExceedingInboundLimitEvenIfZstdFrameIsValid() throws Exception {
        Peer sender = peer(false);
        Peer receiver = peer(false);
        try {
            var smallLayout = new TransportConnection.Layout("frame-decoder", "minecraft-encoder",
                "compression-decoder", "compression-encoder", "frame-encoder", 1024);
            AtomicInteger ready = new AtomicInteger();
            TransportConnection.prepare(receiver.channel, smallLayout, receiver.stats, ready::incrementAndGet);
            receiver.channel.runPendingTasks();
            assertEquals(1, ready.get());
            prepare(sender);
            TransportConnection.enableOutbound(sender.channel, 3, LAYOUT, sender.stats);
            sender.channel.writeAndFlush(Unpooled.wrappedBuffer(packet(2048, 8)));
            flush(sender.channel);
            ByteBuf wire = sender.channel.readOutbound();
            assertNotNull(wire);
            assertThrows(Exception.class, () -> receiver.channel.writeInbound(wire));
            assertNull(receiver.channel.readInbound());
        } finally {
            sender.channel.finishAndReleaseAll();
            try { receiver.channel.finishAndReleaseAll(); } catch (Exception ignored) { }
        }
    }

    @Test
    void rejectsOversizedAndCorruptHeadersBeforeForwardingAnyPacket() throws Exception {
        for (int[] lengths : new int[][] {{Integer.MAX_VALUE, 1}, {128, Integer.MAX_VALUE}, {-1, 1}, {1, 0}}) {
            Peer receiver = peer(false);
            try {
                prepare(receiver);
                ByteBuf bad = Unpooled.buffer(12).writeInt(0x80808080).writeInt(lengths[0]).writeInt(lengths[1]);
                assertThrows(Exception.class, () -> receiver.channel.writeInbound(bad));
                assertNull(receiver.channel.readInbound());
            } finally {
                try { receiver.channel.finishAndReleaseAll(); } catch (Exception ignored) { }
            }
        }
    }

    @Test
    void worksWithVanillaCompressionDisabledAndPreservesMultipleZstdBatches() throws Exception {
        Peer sender = barePeer(true);
        Peer receiver = barePeer(true);
        try {
            byte[] old = packet(71, 5);
            sender.channel.writeAndFlush(Unpooled.wrappedBuffer(old));
            transfer(sender.channel, receiver.channel, 2);
            assertPacket(receiver.channel, old);
            prepare(sender);
            prepare(receiver);
            TransportConnection.enableOutbound(sender.channel, 3, LAYOUT, sender.stats);
            for (int i = 0; i < 4; i++) sender.channel.writeAndFlush(Unpooled.wrappedBuffer(packet(70 * 1024 + i, i + 90)));
            flush(sender.channel);
            transfer(sender.channel, receiver.channel, 11);
            for (int i = 0; i < 4; i++) assertPacket(receiver.channel, packet(70 * 1024 + i, i + 90));
            assertNull(receiver.channel.readInbound());
            assertEquals(sender.stats.txRaw(), receiver.stats.rxRaw());
            assertEquals(sender.stats.txWire(), receiver.stats.rxWire());
        } finally {
            sender.channel.finishAndReleaseAll();
            receiver.channel.finishAndReleaseAll();
        }
    }

    @Test
    void aCommittedZstdStreamRejectsAReversionToVanilla() throws Exception {
        Peer sender = peer(false);
        Peer receiver = peer(false);
        try {
            prepare(sender);
            prepare(receiver);
            TransportConnection.enableOutbound(sender.channel, 3, LAYOUT, sender.stats);
            byte[] expected = packet(80, 6);
            sender.channel.writeAndFlush(Unpooled.wrappedBuffer(expected));
            flush(sender.channel);
            transfer(sender.channel, receiver.channel, 5);
            assertPacket(receiver.channel, expected);
            assertThrows(Exception.class, () -> receiver.channel.writeInbound(Unpooled.wrappedBuffer(new byte[] {2, 1, 2})));
            assertNull(receiver.channel.readInbound());
        } finally {
            sender.channel.finishAndReleaseAll();
            try { receiver.channel.finishAndReleaseAll(); } catch (Exception ignored) { }
        }
    }

    @Test
    void rejectsACompressedBatchContainingAnIncompleteMinecraftPacket() throws Exception {
        EmbeddedChannel malformedEncoder = new EmbeddedChannel(new ZstdFramedCodec.Encoder(3, ignored -> { }, ignored -> { }));
        malformedEncoder.freezeTime();
        Peer receiver = peer(false);
        try {
            prepare(receiver);
            // Valid Zstd data, invalid internal Minecraft frame: the declared 100 bytes do not exist.
            malformedEncoder.writeAndFlush(Unpooled.wrappedBuffer(new byte[] {100, 1, 2}));
            flush(malformedEncoder);
            ByteBuf wire = malformedEncoder.readOutbound();
            assertNotNull(wire);
            assertThrows(Exception.class, () -> receiver.channel.writeInbound(wire));
            assertNull(receiver.channel.readInbound());
        } finally {
            malformedEncoder.finishAndReleaseAll();
            try { receiver.channel.finishAndReleaseAll(); } catch (Exception ignored) { }
        }
    }

    @Test
    void corruptCompressedDataNeverReachesTheMinecraftDecoder() throws Exception {
        Peer receiver = peer(false);
        try {
            prepare(receiver);
            ByteBuf bad = Unpooled.buffer(16).writeInt(0x80808080).writeInt(100).writeInt(4).writeInt(0);
            assertThrows(Exception.class, () -> receiver.channel.writeInbound(bad));
            assertNull(receiver.channel.readInbound());
        } finally {
            try { receiver.channel.finishAndReleaseAll(); } catch (Exception ignored) { }
        }
    }

    @Test
    void minecraftSeparateCompressionAndPrependerDoesNotWrapZstdInAnotherLengthPrefix() throws Exception {
        Peer proxy = peer(true);
        Peer client = separatedClientPeer();
        try {
            byte[] vanillaDown = packet(311, 50);
            byte[] vanillaUp = packet(301, 51);
            proxy.channel.writeAndFlush(Unpooled.wrappedBuffer(vanillaDown));
            client.channel.writeAndFlush(Unpooled.wrappedBuffer(vanillaUp));
            transfer(proxy.channel, client.channel, 3);
            transfer(client.channel, proxy.channel, 2);
            assertPacket(client.channel, vanillaDown);
            assertPacket(proxy.channel, vanillaUp);
            prepare(proxy);
            prepare(client, CLIENT_LAYOUT);
            byte[] activation = packet(33, 52);
            proxy.channel.writeAndFlush(Unpooled.wrappedBuffer(activation));
            TransportConnection.enableOutbound(proxy.channel, 3, LAYOUT, proxy.stats);
            proxy.channel.writeAndFlush(Unpooled.wrappedBuffer(packet(70 * 1024, 53)));
            flush(proxy.channel);
            transfer(proxy.channel, client.channel, 7);
            assertPacket(client.channel, activation);
            assertPacket(client.channel, packet(70 * 1024, 53));
            TransportConnection.enableOutbound(client.channel, 3, CLIENT_LAYOUT, client.stats);
            client.channel.writeAndFlush(Unpooled.wrappedBuffer(packet(5000, 54)));
            flush(client.channel);
            transfer(client.channel, proxy.channel, 1);
            assertPacket(proxy.channel, packet(5000, 54));
            assertEquals(1, client.nativeResource.encodes, "Zstd must skip both compress and prepender");
            assertEquals(2, client.nativeResource.decodes);
            assertEquals(0, client.nativeResource.closes);
            assertEquals(client.stats.txRaw(), proxy.stats.rxRaw());
            assertEquals(client.stats.txWire(), proxy.stats.rxWire());
        } finally {
            proxy.channel.finishAndReleaseAll();
            client.channel.finishAndReleaseAll();
        }
    }

    @Test
    void anOversizedAdvertisedZstdWindowIsRejectedBeforeNativeHistoryAllocation() throws Exception {
        Peer sender = peer(false);
        Peer receiver = peer(false);
        try {
            prepare(sender);
            prepare(receiver);
            TransportConnection.enableOutbound(sender.channel, 3, LAYOUT, sender.stats);
            sender.channel.writeAndFlush(Unpooled.wrappedBuffer(packet(80, 2)));
            flush(sender.channel);
            ByteBuf wire = sender.channel.readOutbound();
            assertNotNull(wire);
            // Streaming frames have an explicit window descriptor after Zstd magic + flags.
            // Increasing only its declared history is a valid frame and needs no huge compressor
            // allocation in this test process. A 0xa0 descriptor advertises a 1 GiB (log 30) window.
            int flags = wire.getUnsignedByte(12 + 4);
            assertEquals(0, flags & 0x20, "the test stream must not use the single-segment format");
            wire.setByte(12 + 5, 0xa0);
            assertEquals(30, 10 + (wire.getUnsignedByte(12 + 5) >>> 3));
            assertThrows(Exception.class, () -> receiver.channel.writeInbound(wire));
            assertNull(receiver.channel.readInbound());
        } finally {
            sender.channel.finishAndReleaseAll();
            try { receiver.channel.finishAndReleaseAll(); } catch (Exception ignored) { }
        }
    }

    @Test
    void supportedCompressionLevelsFitTheBoundedNativeDecompressionWindow() throws Exception {
        for (int level : new int[] {1, 3, 6, 12, 22}) {
            Peer sender = peer(false);
            Peer receiver = peer(false);
            try {
                prepare(sender);
                prepare(receiver);
                TransportConnection.enableOutbound(sender.channel, level, LAYOUT, sender.stats);
                byte[] expected = packet(96 * 1024, level);
                sender.channel.writeAndFlush(Unpooled.wrappedBuffer(expected));
                flush(sender.channel);
                transfer(sender.channel, receiver.channel, 11);
                assertPacket(receiver.channel, expected);
            } finally {
                sender.channel.finishAndReleaseAll();
                receiver.channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void minecraftFlowControlQueuesTheRestOfAZstdBatchAndReleasesItOnClose() throws Exception {
        Peer sender = peer(false);
        Peer receiver = peer(false);
        List<ByteBuf> received = new ArrayList<>();
        FlowControlHandler flow = new FlowControlHandler();
        receiver.channel.pipeline().addAfter("compression-decoder", "flow-control", flow);
        receiver.channel.pipeline().addLast("terminal-collector", new ChannelInboundHandlerAdapter() {
            @Override public void channelRead(ChannelHandlerContext context, Object message) {
                received.add((ByteBuf) message);
                // Matches the configuration transition's terminal packet suspending network reads.
                context.channel().config().setAutoRead(false);
            }
        });
        try {
            prepare(sender);
            prepare(receiver);
            TransportConnection.enableOutbound(sender.channel, 3, LAYOUT, sender.stats);
            for (int i = 0; i < 3; i++) sender.channel.writeAndFlush(Unpooled.wrappedBuffer(packet(80, i)));
            flush(sender.channel);
            transfer(sender.channel, receiver.channel, 65536);
            assertEquals(1, received.size(), "autoRead=false must suspend the remainder of one compressed batch");
            assertFalse(receiver.channel.config().isAutoRead());
            received.removeFirst().release();
            receiver.channel.read();
            receiver.channel.runPendingTasks();
            assertEquals(1, received.size(), "an explicit read should release exactly the next queued packet");
            received.removeFirst().release();

            var queueField = FlowControlHandler.class.getDeclaredField("queue");
            queueField.setAccessible(true);
            Collection<?> queue = (Collection<?>) queueField.get(flow);
            assertNotNull(queue);
            assertEquals(1, queue.size());
            ReferenceCounted queuedPacket = (ReferenceCounted) queue.iterator().next();
            assertTrue(queuedPacket.refCnt() > 0);
            receiver.channel.close();
            receiver.channel.runPendingTasks();
            assertEquals(0, queuedPacket.refCnt(), "disconnect must release unread decompressed packets");
            assertEquals(0, received.size());
        } finally {
            for (ByteBuf packet : received) packet.release();
            sender.channel.finishAndReleaseAll();
            receiver.channel.finishAndReleaseAll();
        }
    }

    @Test
    void decodedZstdPacketsKeepTheExistingReadTimeoutHandlerInTheInboundPath() throws Exception {
        Peer sender = peer(false);
        Peer receiver = peer(false);
        AtomicInteger timeoutReads = new AtomicInteger();
        ReadTimeoutHandler readTimeout = new ReadTimeoutHandler(30, TimeUnit.SECONDS) {
            @Override public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
                timeoutReads.incrementAndGet();
                super.channelRead(context, message);
            }
        };
        receiver.channel.pipeline().addAfter("frame-decoder", "read-timeout", readTimeout);
        try {
            prepare(sender);
            prepare(receiver);
            TransportConnection.enableOutbound(sender.channel, 3, LAYOUT, sender.stats);
            for (int i = 0; i < 3; i++) sender.channel.writeAndFlush(Unpooled.wrappedBuffer(packet(90 + i, i)));
            flush(sender.channel);
            transfer(sender.channel, receiver.channel, 4);
            for (int i = 0; i < 3; i++) assertPacket(receiver.channel, packet(90 + i, i));
            assertEquals(3, timeoutReads.get(), "decoded packets must reset the same native read-timeout state");
            assertSame(readTimeout, receiver.channel.pipeline().get("read-timeout"));
            assertEquals(0, receiver.nativeResource.decodes, "the timeout path must not reintroduce zlib");
            assertTrue(receiver.channel.isActive());
        } finally {
            sender.channel.finishAndReleaseAll();
            receiver.channel.finishAndReleaseAll();
        }
    }

    private static void prepare(Peer peer) {
        prepare(peer, LAYOUT);
    }

    private static void prepare(Peer peer, TransportConnection.Layout layout) {
        AtomicInteger ready = new AtomicInteger();
        TransportConnection.prepare(peer.channel, layout, peer.stats, ready::incrementAndGet);
        peer.channel.runPendingTasks();
        peer.channel.checkException();
        assertEquals(1, ready.get());
    }

    private static Peer peer(boolean encrypted) throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        SharedNative resource = new SharedNative();
        if (encrypted) channel.pipeline().addLast("cipher-decoder", new Decryptor(cipher(Cipher.DECRYPT_MODE)));
        channel.pipeline().addLast("frame-decoder", new VarIntFrameDecoder());
        if (encrypted) channel.pipeline().addLast("cipher-encoder", new Encryptor(cipher(Cipher.ENCRYPT_MODE)));
        channel.pipeline().addLast("compression-decoder", new VanillaDecompressor(resource));
        channel.pipeline().addLast("compression-encoder", new VanillaCompressor(resource));
        channel.pipeline().addLast("minecraft-decoder", new ChannelInboundHandlerAdapter());
        channel.pipeline().addLast("minecraft-encoder", new ChannelOutboundHandlerAdapter());
        return new Peer(channel, resource, new TransportStats());
    }

    private static Peer barePeer(boolean encrypted) throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        if (encrypted) channel.pipeline().addLast("cipher-decoder", new Decryptor(cipher(Cipher.DECRYPT_MODE)));
        channel.pipeline().addLast("frame-decoder", new VarIntFrameDecoder());
        if (encrypted) channel.pipeline().addLast("cipher-encoder", new Encryptor(cipher(Cipher.ENCRYPT_MODE)));
        channel.pipeline().addLast("frame-encoder", new BareFrameEncoder());
        channel.pipeline().addLast("minecraft-decoder", new ChannelInboundHandlerAdapter());
        channel.pipeline().addLast("minecraft-encoder", new ChannelOutboundHandlerAdapter());
        return new Peer(channel, new SharedNative(), new TransportStats());
    }

    private static Peer separatedClientPeer() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        SharedNative resource = new SharedNative();
        channel.pipeline().addLast("decrypt", new Decryptor(cipher(Cipher.DECRYPT_MODE)));
        channel.pipeline().addLast("splitter", new VarIntFrameDecoder());
        channel.pipeline().addLast("encrypt", new Encryptor(cipher(Cipher.ENCRYPT_MODE)));
        channel.pipeline().addLast("prepender", new BareFrameEncoder());
        channel.pipeline().addLast("decompress", new VanillaDecompressor(resource));
        channel.pipeline().addLast("decoder", new ChannelInboundHandlerAdapter());
        channel.pipeline().addLast("compress", new VanillaCompressor(resource, false));
        channel.pipeline().addLast("encoder", new ChannelOutboundHandlerAdapter());
        return new Peer(channel, resource, new TransportStats());
    }

    private static Cipher cipher(int mode) throws Exception {
        byte[] secret = new byte[16];
        Arrays.fill(secret, (byte) 37);
        Cipher cipher = Cipher.getInstance("AES/CFB8/NoPadding");
        cipher.init(mode, new SecretKeySpec(secret, "AES"), new IvParameterSpec(secret));
        return cipher;
    }

    private static void flush(EmbeddedChannel channel) {
        channel.advanceTimeBy(2, TimeUnit.MILLISECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
        channel.checkException();
    }

    private static void transfer(EmbeddedChannel sender, EmbeddedChannel receiver, int fragmentSize) {
        ByteBuf combined = Unpooled.buffer();
        ByteBuf wire;
        while ((wire = sender.readOutbound()) != null) {
            combined.writeBytes(wire);
            wire.release();
        }
        try {
            while (combined.isReadable()) receiver.writeInbound(combined.readRetainedSlice(Math.min(fragmentSize, combined.readableBytes())));
            receiver.checkException();
        } finally { combined.release(); }
    }

    private static void assertPacket(EmbeddedChannel channel, byte[] expected) {
        ByteBuf actual = channel.readInbound();
        assertNotNull(actual, "a complete Minecraft packet should be delivered");
        try {
            byte[] bytes = new byte[actual.readableBytes()];
            actual.readBytes(bytes);
            assertArrayEquals(expected, bytes);
        } finally { actual.release(); }
    }

    private static byte[] packet(int length, int seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        // Keep representative repeated sections so both zlib and Zstd actually compress.
        for (int i = 16; i < length; i++) bytes[i] = (byte) (i % 11);
        return bytes;
    }

    private static int readVarInt(ByteBuf buffer) {
        int value = 0;
        for (int i = 0; i < 5; i++) {
            int next = buffer.readUnsignedByte();
            value |= (next & 127) << (7 * i);
            if ((next & 128) == 0) return value;
        }
        throw new DecoderException("bad test VarInt");
    }

    private static void writeVarInt(ByteBuf buffer, int value) {
        do {
            int part = value & 127;
            value >>>= 7;
            buffer.writeByte(part | (value == 0 ? 0 : 128));
        } while (value != 0);
    }

    private record Peer(EmbeddedChannel channel, SharedNative nativeResource, TransportStats stats) { }

    private static final class SharedNative {
        int encodes;
        int decodes;
        int closes;
    }

    private static final class VarIntFrameDecoder extends ByteToMessageDecoder {
        @Override protected void decode(ChannelHandlerContext context, ByteBuf input, List<Object> output) {
            input.markReaderIndex();
            int length = 0;
            for (int i = 0; i < 5; i++) {
                if (!input.isReadable()) { input.resetReaderIndex(); return; }
                int b = input.readUnsignedByte();
                length |= (b & 127) << (i * 7);
                if ((b & 128) == 0) {
                    if (length <= 0 || length > 9 * 1024 * 1024) throw new DecoderException("invalid test frame");
                    if (input.readableBytes() < length) { input.resetReaderIndex(); return; }
                    output.add(input.readRetainedSlice(length));
                    return;
                }
            }
            throw new DecoderException("invalid frame prefix");
        }
    }

    private static final class VanillaCompressor extends MessageToMessageEncoder<ByteBuf> {
        private final SharedNative resource;
        private final boolean combinedLength;
        private VanillaCompressor(SharedNative resource) { this(resource, true); }
        private VanillaCompressor(SharedNative resource, boolean combinedLength) {
            this.resource = resource;
            this.combinedLength = combinedLength;
        }
        @Override protected void encode(ChannelHandlerContext context, ByteBuf input, List<Object> output) {
            assertEquals(0, resource.closes, "shared native compressor was closed too early");
            resource.encodes++;
            byte[] raw = new byte[input.readableBytes()];
            input.getBytes(input.readerIndex(), raw);
            Deflater deflater = new Deflater();
            try {
                deflater.setInput(raw);
                deflater.finish();
                byte[] compressed = new byte[raw.length + 128];
                int size = deflater.deflate(compressed);
                ByteBuf body = context.alloc().buffer();
                writeVarInt(body, raw.length);
                body.writeBytes(compressed, 0, size);
                if (!combinedLength) {
                    output.add(body);
                    return;
                }
                ByteBuf wire = context.alloc().buffer();
                writeVarInt(wire, body.readableBytes());
                wire.writeBytes(body);
                body.release();
                output.add(wire);
            } finally { deflater.end(); }
        }
        @Override public void handlerRemoved(ChannelHandlerContext context) { resource.closes++; }
    }

    private static final class VanillaDecompressor extends MessageToMessageDecoder<ByteBuf> {
        private final SharedNative resource;
        private VanillaDecompressor(SharedNative resource) { this.resource = resource; }
        @Override protected void decode(ChannelHandlerContext context, ByteBuf input, List<Object> output) throws Exception {
            assertEquals(0, resource.closes, "shared native compressor was closed too early");
            resource.decodes++;
            int expected = readVarInt(input);
            byte[] compressed = new byte[input.readableBytes()];
            input.readBytes(compressed);
            Inflater inflater = new Inflater();
            try {
                inflater.setInput(compressed);
                byte[] raw = new byte[expected];
                assertEquals(expected, inflater.inflate(raw));
                output.add(Unpooled.wrappedBuffer(raw));
            } finally { inflater.end(); }
        }
        @Override public void handlerRemoved(ChannelHandlerContext context) { resource.closes++; }
    }

    private static final class Encryptor extends MessageToMessageEncoder<ByteBuf> {
        private final Cipher cipher;
        private Encryptor(Cipher cipher) { this.cipher = cipher; }
        @Override protected void encode(ChannelHandlerContext context, ByteBuf input, List<Object> output) {
            byte[] bytes = new byte[input.readableBytes()];
            input.readBytes(bytes);
            output.add(Unpooled.wrappedBuffer(cipher.update(bytes)));
        }
    }

    private static final class BareFrameEncoder extends MessageToMessageEncoder<ByteBuf> {
        @Override protected void encode(ChannelHandlerContext context, ByteBuf input, List<Object> output) {
            ByteBuf wire = context.alloc().buffer(input.readableBytes() + 5);
            writeVarInt(wire, input.readableBytes());
            wire.writeBytes(input, input.readerIndex(), input.readableBytes());
            output.add(wire);
        }
    }

    private static final class Decryptor extends MessageToMessageDecoder<ByteBuf> {
        private final Cipher cipher;
        private Decryptor(Cipher cipher) { this.cipher = cipher; }
        @Override protected void decode(ChannelHandlerContext context, ByteBuf input, List<Object> output) {
            byte[] bytes = new byte[input.readableBytes()];
            input.readBytes(bytes);
            output.add(Unpooled.wrappedBuffer(cipher.update(bytes)));
        }
    }
}
