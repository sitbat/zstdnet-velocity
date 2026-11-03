package cn.tohsaka.zstdnet.velocity;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** Bounded initial PLAY queue AFTER packet encoding, so limits count actual encoded bytes. */
final class InitialSyncGate extends ChannelDuplexHandler {
    static final int MAX_PACKETS = 4096;
    static final long MAX_BYTES = 8L * 1024 * 1024;
    private final String packetEncoder;
    private final BooleanSupplier shouldBuffer;
    private final Runnable onFirstBuffered;
    private final Runnable onFallback;
    private final ArrayDeque<PendingWrite> pending = new ArrayDeque<>();
    private ChannelHandlerContext context;
    private ScheduledFuture<?> timeout;
    private long pendingBytes;
    private int bypassDepth;
    private boolean finished;

    InitialSyncGate(String packetEncoder, BooleanSupplier shouldBuffer,
                    Runnable onFirstBuffered, Runnable onFallback) {
        this.packetEncoder = packetEncoder;
        this.shouldBuffer = shouldBuffer;
        this.onFirstBuffered = onFirstBuffered;
        this.onFallback = onFallback;
    }

    @Override public void handlerAdded(ChannelHandlerContext context) { this.context = context; }

    void allow(Runnable writeControl) {
        bypassDepth++;
        try { writeControl.run(); } finally { bypassDepth--; }
    }

    @Override public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
        if (finished || bypassDepth > 0 || !(message instanceof ByteBuf bytes) || !shouldBuffer.getAsBoolean()) {
            ctx.write(message, promise);
            return;
        }
        pending.add(new PendingWrite(message, promise));
        pendingBytes += bytes.readableBytes();
        if (timeout == null) {
            timeout = ctx.executor().schedule(this::fallback, 5, TimeUnit.SECONDS);
            onFirstBuffered.run();
        }
        if (!finished && (pending.size() >= MAX_PACKETS || pendingBytes > MAX_BYTES)) fallback();
    }

    private void fallback() {
        if (finished) return;
        onFallback.run();
        release(false);
    }

    void release(boolean upgraded) {
        if (finished) return;
        finished = true;
        cancelTimer();
        ChannelHandlerContext target = upgraded ? context.pipeline().context(packetEncoder) : context;
        if (target == null || !context.channel().isActive()) {
            failPending();
        } else {
            PendingWrite write;
            while ((write = pending.poll()) != null) target.write(write.message(), write.promise());
            target.flush();
        }
        pendingBytes = 0;
        if (context.pipeline().context(this) != null) context.pipeline().remove(this);
    }

    private void failPending() {
        PendingWrite write;
        while ((write = pending.poll()) != null) {
            ReferenceCountUtil.release(write.message());
            write.promise().tryFailure(new ClosedChannelException());
        }
        pendingBytes = 0;
    }

    /** Reject a required-client negotiation without replaying the buffered world data. */
    void discard() {
        if (finished) return;
        finished = true;
        cancelTimer();
        failPending();
        if (context != null && context.pipeline().context(this) != null) context.pipeline().remove(this);
    }

    private void cancelTimer() {
        if (timeout != null) { timeout.cancel(false); timeout = null; }
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) {
        finished = true;
        cancelTimer();
        failPending();
        ctx.fireChannelInactive();
    }

    @Override public void handlerRemoved(ChannelHandlerContext ctx) {
        cancelTimer();
        if (!finished) { finished = true; failPending(); }
    }

    private record PendingWrite(Object message, ChannelPromise promise) { }
}
