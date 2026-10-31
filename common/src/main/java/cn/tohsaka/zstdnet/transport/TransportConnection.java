package cn.tohsaka.zstdnet.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

import java.lang.reflect.Field;
import java.util.function.IntConsumer;

/**
 * Installs a transport inside Minecraft's cipher boundary. Native zlib handlers remain owned by
 * Minecraft/Velocity until disconnect: some implementations share a single native compressor
 * between directions, so removing only one of them during a half-switched connection is unsafe.
 */
public final class TransportConnection {
    private static final String DECODE = "zstdnet-proxy-decode";
    private static final String ROUTE = "zstdnet-proxy-route";
    private static final String BYPASS = "zstdnet-proxy-bypass";
    private static final String ENCODE = "zstdnet-proxy-encode";
    private static final String FRAME = "zstdnet-proxy-frame";
    private static final String UNWRAP = "zstdnet-proxy-unframe";
    private static final AttributeKey<Boolean> ACTIVE = AttributeKey.valueOf("zstdnet-proxy-active");
    private static final AttributeKey<TransportStats> STATS = AttributeKey.valueOf("zstdnet-proxy-stats");
    private static final Field CUMULATION = findCumulation();

    private TransportConnection() { }

    public record Layout(String frameDecoder, String packetEncoder, String compressionDecoder,
                         String compressionEncoder, String frameEncoder, int maxInboundPacketBytes) {
        public Layout {
            if (maxInboundPacketBytes <= 0 || maxInboundPacketBytes > 8 * 1024 * 1024) {
                throw new IllegalArgumentException("invalid inbound packet limit");
            }
        }
    }

    /** Always schedules after the current read, so the old splitter is no longer decoding. */
    public static void prepare(Channel channel, Layout layout, TransportStats stats, Runnable onReady) {
        prepare(channel, layout, stats, ignored -> { }, onReady);
    }

    /** Optional accounting callback lets proxies preserve their configured native packet limiters. */
    public static void prepare(Channel channel, Layout layout, TransportStats stats,
                               IntConsumer inboundAccount, Runnable onReady) {
        channel.eventLoop().execute(() -> {
            if (!channel.isActive()) return;
            try {
                ChannelPipeline pipeline = channel.pipeline();
                if (pipeline.get(DECODE) == null) {
                    if (!(pipeline.get(layout.frameDecoder()) instanceof ByteToMessageDecoder splitter)) {
                        throw new IllegalStateException("unsupported frame decoder: " + layout.frameDecoder());
                    }
                    if (pipeline.get(layout.packetEncoder()) == null) {
                        throw new IllegalStateException("missing packet encoder");
                    }
                    if (CUMULATION == null) throw new IllegalStateException("splitter buffer access unavailable");
                    ByteBuf buffered = (ByteBuf) CUMULATION.get(splitter);
                    ZstdFramedCodec.Decoder decoder = new ZstdFramedCodec.Decoder(stats::addRxRaw, stats::addRxWire);
                    pipeline.addBefore(layout.frameDecoder(), DECODE, decoder);
                    pipeline.addAfter(DECODE, ROUTE, new BatchRouter(layout, inboundAccount));
                    String unwrapAfter = pipeline.get(layout.compressionDecoder()) == null
                        ? layout.frameDecoder() : layout.compressionDecoder();
                    pipeline.addAfter(unwrapAfter, UNWRAP, new PlainPacketUnwrapper(layout));
                    channel.attr(STATS).set(stats);
                    if (buffered != null && buffered.isReadable()) {
                        // Already decrypted bytes must enter AFTER the cipher, never at pipeline head.
                        ByteBuf continuation = buffered.readRetainedSlice(buffered.readableBytes());
                        decoder.channelRead(pipeline.context(DECODE), continuation);
                    }
                }
                if (channel.isActive()) onReady.run();
            } catch (Exception error) {
                channel.pipeline().fireExceptionCaught(error);
                channel.close();
            }
        });
    }

    /** Call only after the final vanilla activation message has synchronously passed its encoder. */
    public static void enableOutbound(Channel channel, int level, Layout layout, TransportStats stats) {
        if (!channel.eventLoop().inEventLoop()) {
            throw new IllegalStateException("transport activation must run on the channel event loop");
        }
        if (level < 1 || level > 22) throw new IllegalArgumentException("compression level must be 1..22");
        if (isActive(channel)) return;
        if (!channel.isActive() || channel.pipeline().get(DECODE) == null) {
            throw new IllegalStateException("inbound transport is not ready");
        }
        ChannelPipeline pipeline = channel.pipeline();
        if (outboundTarget(pipeline, layout) == null) throw new IllegalStateException("missing wire encoder");
        pipeline.addBefore(layout.packetEncoder(), BYPASS, new NativeCompressionBypass(layout));
        pipeline.addAfter(BYPASS, ENCODE,
            new ZstdFramedCodec.Encoder(level, stats::addTxRaw, stats::addTxWire));
        pipeline.addAfter(ENCODE, FRAME, new PacketFramer());
        channel.attr(STATS).set(stats);
        channel.attr(ACTIVE).set(true);
    }

    public static boolean isActive(Channel channel) { return Boolean.TRUE.equals(channel.attr(ACTIVE).get()); }
    public static TransportStats stats(Channel channel) { return channel.attr(STATS).get(); }

    private static ChannelHandlerContext outboundTarget(ChannelPipeline pipeline, Layout layout) {
        // Vanilla Minecraft has a separate length prepender before its compressor. Velocity
        // removes that prepender and combines both operations in its compression encoder.
        ChannelHandlerContext target = pipeline.context(layout.frameEncoder());
        return target != null ? target : pipeline.context(layout.compressionEncoder());
    }

    private static Field findCumulation() {
        try {
            Field field = ByteToMessageDecoder.class.getDeclaredField("cumulation");
            field.setAccessible(true);
            return field;
        } catch (ReflectiveOperationException | RuntimeException unavailable) {
            return null;
        }
    }

    private static final class NativeCompressionBypass extends ChannelOutboundHandlerAdapter {
        private final Layout layout;
        private NativeCompressionBypass(Layout layout) { this.layout = layout; }
        @Override
        public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
            ChannelHandlerContext target = outboundTarget(ctx.pipeline(), layout);
            if (target == null) {
                ReferenceCountUtil.release(message);
                promise.tryFailure(new EncoderException("native wire encoder disappeared"));
                ctx.close();
                return;
            }
            // Skip native zlib + length encoding; earlier cipher handlers still run normally.
            target.write(message, promise);
        }
        @Override
        public void flush(ChannelHandlerContext ctx) {
            ChannelHandlerContext target = outboundTarget(ctx.pipeline(), layout);
            if (target != null) target.flush();
            else ctx.close();
        }
    }

    private static final class PacketFramer extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
            if (!(message instanceof ByteBuf packet)) {
                ctx.write(message, promise);
                return;
            }
            ByteBuf framed = null;
            try {
                int size = packet.readableBytes();
                if (size <= 0 || size > 8 * 1024 * 1024) throw new EncoderException("invalid packet size: " + size);
                framed = ctx.alloc().buffer(size + 5);
                int length = size;
                do {
                    int next = length & 0x7f;
                    length >>>= 7;
                    framed.writeByte(length == 0 ? next : next | 0x80);
                } while (length != 0);
                framed.writeBytes(packet, packet.readerIndex(), size);
                ctx.write(framed, promise);
                framed = null;
            } catch (Exception error) {
                ReferenceCountUtil.release(framed);
                promise.tryFailure(error);
                ctx.fireExceptionCaught(error);
                ctx.close();
            } finally {
                packet.release();
            }
        }
    }

    private static final class BatchRouter extends ChannelInboundHandlerAdapter {
        private final Layout layout;
        private long windowStarted = System.nanoTime();
        private long windowBytes;
        private int windowPackets;
        private final IntConsumer inboundAccount;
        private BatchRouter(Layout layout, IntConsumer inboundAccount) {
            this.layout = layout;
            this.inboundAccount = inboundAccount;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (!(message instanceof ZstdFramedCodec.DecodedBatch batch)) {
                ctx.fireChannelRead(message);
                return;
            }
            ByteBuf frames = batch.data();
            try {
                // Preserve a bounded decompressed-byte budget even when native zlib is bypassed.
                long now = System.nanoTime();
                if (now - windowStarted >= 1_000_000_000L) {
                    windowStarted = now;
                    windowBytes = 0;
                    windowPackets = 0;
                }
                windowBytes += frames.readableBytes();
                if (windowBytes > 128L * 1024 * 1024) throw new DecoderException("Zstd receive budget exceeded");
                while (frames.isReadable()) {
                    int size = readLength(frames);
                    if (size <= 0 || size > layout.maxInboundPacketBytes() || size > frames.readableBytes()) {
                        throw new DecoderException("invalid Zstd packet length: " + size);
                    }
                    if (++windowPackets > 16384) throw new DecoderException("Zstd packet budget exceeded");
                    inboundAccount.accept(size);
                    ChannelHandlerContext target = ctx.pipeline().context(layout.frameDecoder());
                    if (target == null) throw new DecoderException("native packet decoder disappeared");
                    // Preserve read-timeout and flow-control handlers BETWEEN frame splitting
                    // and zlib. A holder is not a ByteBuf, so native zlib decoders pass it through.
                    target.fireChannelRead(new PlainPacket(frames.readRetainedSlice(size)));
                    if (!ctx.channel().isActive()) break;
                }
            } catch (Exception error) {
                ctx.fireExceptionCaught(error);
                ctx.close();
            } finally {
                frames.release();
            }
        }

        private int readLength(ByteBuf input) {
            long value = 0;
            for (int index = 0; index < 5; index++) {
                if (!input.isReadable()) throw new DecoderException("truncated Minecraft frame length");
                int next = input.readUnsignedByte();
                value |= (long) (next & 0x7f) << (index * 7);
                if ((next & 0x80) == 0) {
                    if (value > Integer.MAX_VALUE) throw new DecoderException("Minecraft length overflow");
                    return (int) value;
                }
            }
            throw new DecoderException("Minecraft length exceeds five bytes");
        }
    }

    private static final class PlainPacket extends DefaultByteBufHolder {
        private PlainPacket(ByteBuf content) { super(content); }
    }

    private static final class PlainPacketUnwrapper extends ChannelInboundHandlerAdapter {
        private final Layout layout;
        private PlainPacketUnwrapper(Layout layout) { this.layout = layout; }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (!(message instanceof PlainPacket packet)) {
                ctx.fireChannelRead(message);
                return;
            }
            ByteBuf raw = packet.content().retain();
            try {
                ChannelHandlerContext compression = ctx.pipeline().context(layout.compressionDecoder());
                // If the owner replaced/reinserted its native zlib decoder after our adapter
                // during reconfiguration, skip the new decoder too. Timeout/flow gates before
                // it have already seen the holder; the remaining native path is preserved.
                var names = ctx.pipeline().names();
                if (compression != null && names.indexOf(compression.name()) > names.indexOf(ctx.name())) {
                    compression.fireChannelRead(raw);
                } else {
                    ctx.fireChannelRead(raw);
                }
            } finally {
                packet.release();
            }
        }
    }
}
