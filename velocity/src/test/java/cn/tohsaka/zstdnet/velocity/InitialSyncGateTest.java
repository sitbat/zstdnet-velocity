package cn.tohsaka.zstdnet.velocity;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class InitialSyncGateTest {
    @Test void controlsOvertakeBoundedInitialQueueAndFallbackPreservesPacketOrder() {
        var fallback = new AtomicInteger();
        EmbeddedChannel channel = new EmbeddedChannel();
        InitialSyncGate gate = new InitialSyncGate("encoder", () -> true, () -> {}, fallback::incrementAndGet);
        channel.pipeline().addLast("gate", gate).addLast("encoder", new ChannelOutboundHandlerAdapter());
        try {
            var pending = channel.writeAndFlush(Unpooled.buffer().writeByte(1));
            channel.writeAndFlush(Unpooled.buffer().writeByte(2));
            assertNull(channel.readOutbound());
            gate.allow(() -> channel.writeAndFlush(Unpooled.buffer().writeByte(9)));
            assertByte(channel, 9);
            assertFalse(pending.isDone());
            gate.release(false);
            assertByte(channel, 1);
            assertByte(channel, 2);
            assertTrue(pending.isSuccess());
            assertEquals(0, fallback.get());
        } finally { channel.finishAndReleaseAll(); }
    }

    @Test void upgradeReplaysAlreadyEncodedPacketsThroughNewCompressionWithoutDoublePacketEncoding() {
        EmbeddedChannel channel = new EmbeddedChannel();
        AtomicInteger encoded = new AtomicInteger();
        InitialSyncGate gate = new InitialSyncGate("encoder", () -> true, () -> {}, () -> {});
        channel.pipeline().addLast("gate", gate).addLast("encoder", new MessageToMessageEncoder<ByteBuf>() {
            @Override protected void encode(ChannelHandlerContext ctx, ByteBuf input, List<Object> output) {
                encoded.incrementAndGet();
                output.add(input.retain());
            }
        });
        try {
            channel.writeAndFlush(Unpooled.buffer().writeByte(31));
            channel.pipeline().addBefore("encoder", "new-zstd", new MessageToMessageEncoder<ByteBuf>() {
                @Override protected void encode(ChannelHandlerContext ctx, ByteBuf input, List<Object> output) {
                    output.add(ctx.alloc().buffer().writeByte(77).writeBytes(input));
                }
            });
            gate.release(true);
            ByteBuf output = channel.readOutbound();
            try { assertEquals(77, output.readUnsignedByte()); assertEquals(31, output.readUnsignedByte()); }
            finally { output.release(); }
            assertEquals(1, encoded.get());
        } finally { channel.finishAndReleaseAll(); }
    }

    @Test void fiveSecondTimeoutReleasesInitialDataInVanillaAndCallsFallbackOnce() {
        var count = new AtomicInteger();
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        InitialSyncGate gate = new InitialSyncGate("encoder", () -> true, () -> {}, count::incrementAndGet);
        channel.pipeline().addLast("gate", gate).addLast("encoder", new ChannelOutboundHandlerAdapter());
        try {
            channel.writeAndFlush(Unpooled.buffer().writeByte(44));
            channel.advanceTimeBy(5, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertByte(channel, 44);
            assertEquals(1, count.get());
            assertTrue(channel.isActive());
        } finally { channel.finishAndReleaseAll(); }
    }

    @Test void disconnectReleasesQueuedBuffersAndFailsTheirPromises() {
        EmbeddedChannel channel = new EmbeddedChannel();
        InitialSyncGate gate = new InitialSyncGate("encoder", () -> true, () -> {}, () -> {});
        channel.pipeline().addLast("gate", gate).addLast("encoder", new ChannelOutboundHandlerAdapter());
        ByteBuf input = Unpooled.buffer().writeByte(1);
        var promise = channel.writeAndFlush(input);
        channel.close();
        assertEquals(0, input.refCnt());
        assertTrue(promise.isDone());
        assertFalse(promise.isSuccess());
        channel.finishAndReleaseAll();
    }

    @Test void packetAndByteCapsReleaseWithoutWaitingForTimeout() {
        for (boolean bytes : new boolean[] {false, true}) {
            EmbeddedChannel channel = new EmbeddedChannel();
            var fallback = new AtomicInteger();
            InitialSyncGate gate = new InitialSyncGate("encoder", () -> true, () -> {}, fallback::incrementAndGet);
            channel.pipeline().addLast("gate", gate).addLast("encoder", new ChannelOutboundHandlerAdapter());
            try {
                int count = bytes ? 1 : InitialSyncGate.MAX_PACKETS;
                for (int i=0;i<count;i++) {
                    int size = bytes ? (int) InitialSyncGate.MAX_BYTES + 1 : 1;
                    channel.writeAndFlush(Unpooled.buffer(size).writerIndex(size));
                }
                assertEquals(1, fallback.get());
                ByteBuf output; int received=0;
                while ((output=channel.readOutbound()) != null) { received++; output.release(); }
                assertEquals(count, received);
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    private static void assertByte(EmbeddedChannel channel, int value) {
        ByteBuf output = channel.readOutbound();
        assertNotNull(output);
        try { assertEquals(value, output.readUnsignedByte()); } finally { output.release(); }
    }
}
