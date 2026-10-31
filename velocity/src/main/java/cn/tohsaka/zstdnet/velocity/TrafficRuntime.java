package cn.tohsaka.zstdnet.velocity;

import cn.tohsaka.zstdnet.transport.TransportStats;
import cn.tohsaka.zstdnet.velocity.stats.TrafficStatisticsService;
import cn.tohsaka.zstdnet.velocity.stats.TrafficStats;
import com.google.gson.JsonObject;
import io.netty.channel.Channel;
import org.slf4j.Logger;

import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

/** Bridges per-front-connection counters into process-lifetime history without dropping departures. */
final class TrafficRuntime {
    private final Logger logger;
    private final IntSupplier listenPort;
    private final Runnable sweepLimiter;
    private final TrafficStats totals = new TrafficStats();
    private final TrafficStatisticsService history;
    private final Map<Channel, Cursor> cursors = new ConcurrentHashMap<>();
    private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "zstdnet-proxy-hud"); thread.setDaemon(true); return thread;
    });
    private volatile boolean historyAvailable;
    private volatile Rates rates = new Rates(0, 0, 0, 0);
    private long sampledAt = System.nanoTime();
    private long previousRawUp, previousRawDown, previousWireUp, previousWireDown;

    TrafficRuntime(Path directory, Logger logger, IntSupplier listenPort, Runnable sweepLimiter) {
        this.logger = logger; this.listenPort = listenPort; this.sweepLimiter = sweepLimiter;
        history = new TrafficStatisticsService(directory.resolve("traffic"), ZoneId.systemDefault(), totals);
    }

    void start() {
        try { history.startSession(); historyAvailable = true; }
        catch (RuntimeException error) { logger.error("ZstdNet traffic history could not start; existing files were preserved.", error); }
        ticker.scheduleAtFixedRate(this::tickSafely, 1, 1, TimeUnit.SECONDS);
    }

    void track(Channel channel, TransportStats counters, BooleanSupplier active, Consumer<byte[]> hudSender) {
        cursors.putIfAbsent(channel, new Cursor(counters, active, hudSender));
    }

    void untrack(Channel channel) {
        Cursor cursor = cursors.remove(channel);
        if (cursor != null) cursor.collect();
        refreshActive();
    }

    void collectAll() {
        for (Cursor cursor : cursors.values()) cursor.collect();
        refreshActive();
    }

    void sendImmediate(Channel channel) {
        Cursor cursor = cursors.get(channel);
        if (cursor == null || !cursor.active.getAsBoolean()) return;
        collectAll();
        cursor.hudSender.accept(hudPayload());
    }

    String report(String range) {
        if (!historyAvailable) throw new IllegalStateException("Traffic history is unavailable; check the proxy log.");
        collectAll();
        return history.buildReportJson(range, "VELOCITY", ":" + listenPort.getAsInt());
    }

    TrafficStats.Snapshot snapshot() { return totals.snapshot(); }

    void close() {
        ticker.shutdown();
        try {
            if (!ticker.awaitTermination(3, TimeUnit.SECONDS)) ticker.shutdownNow();
        } catch (InterruptedException error) { Thread.currentThread().interrupt(); ticker.shutdownNow(); }
        collectAll();
        if (historyAvailable) {
            try { history.stopSession(); }
            catch (RuntimeException error) { logger.error("ZstdNet final traffic save failed; recovery state was preserved.", error); }
        }
    }

    private void refreshActive() {
        int active = 0;
        for (Map.Entry<Channel, Cursor> entry : cursors.entrySet()) {
            if (entry.getKey().isActive() && entry.getValue().active.getAsBoolean()) active++;
        }
        totals.activeConn.set(active);
    }

    private void tickSafely() {
        try {
            collectAll(); sweepLimiter.run();
            long now = System.nanoTime();
            long elapsed = Math.max(1, now - sampledAt);
            TrafficStats.Snapshot snapshot = totals.snapshot();
            rates = new Rates(rate(snapshot.rawIngressBytes() - previousRawUp, elapsed),
                    rate(snapshot.rawEgressBytes() - previousRawDown, elapsed),
                    rate(snapshot.tcpIngressBytes() - previousWireUp, elapsed),
                    rate(snapshot.tcpEgressBytes() - previousWireDown, elapsed));
            previousRawUp = snapshot.rawIngressBytes(); previousRawDown = snapshot.rawEgressBytes();
            previousWireUp = snapshot.tcpIngressBytes(); previousWireDown = snapshot.tcpEgressBytes(); sampledAt = now;
            byte[] payload = hudPayload();
            for (Map.Entry<Channel, Cursor> entry : cursors.entrySet()) {
                if (entry.getKey().isActive() && entry.getValue().active.getAsBoolean()) entry.getValue().hudSender.accept(payload);
            }
        } catch (RuntimeException error) { logger.warn("ZstdNet HUD sampling failed.", error); }
    }

    private byte[] hudPayload() {
        TrafficStats.Snapshot data = totals.snapshot(); Rates current = rates;
        JsonObject json = new JsonObject();
        json.addProperty("mode", "VELOCITY"); json.addProperty("listenHost", ""); json.addProperty("listenPort", listenPort.getAsInt());
        json.addProperty("rawUpBytes", data.rawIngressBytes()); json.addProperty("rawDownBytes", data.rawEgressBytes());
        json.addProperty("wireUpBytes", data.tcpIngressBytes()); json.addProperty("wireDownBytes", data.tcpEgressBytes());
        json.addProperty("rawUpRate", current.rawUp); json.addProperty("rawDownRate", current.rawDown);
        json.addProperty("wireUpRate", current.wireUp); json.addProperty("wireDownRate", current.wireDown);
        long raw = data.rawIngressBytes() + data.rawEgressBytes();
        long wire = data.tcpIngressBytes() + data.tcpEgressBytes();
        json.addProperty("ratioPercent", raw <= 0 ? 0 : (double) wire * 100.0 / raw);
        json.addProperty("connections", data.activeConnections());
        return AuxiliaryProtocol.utfPayload(json.toString(), 8192);
    }

    private static long rate(long delta, long nanos) { return (long) (Math.max(0, delta) * 1_000_000_000.0 / nanos); }
    private record Rates(long rawUp, long rawDown, long wireUp, long wireDown) { }

    private final class Cursor {
        private final TransportStats counters;
        private final BooleanSupplier active;
        private final Consumer<byte[]> hudSender;
        private long rawUp, rawDown, wireUp, wireDown;
        private Cursor(TransportStats counters, BooleanSupplier active, Consumer<byte[]> hudSender) {
            this.counters = counters; this.active = active; this.hudSender = hudSender;
        }
        private synchronized void collect() {
            long nextRawUp = counters.rxRaw(), nextRawDown = counters.txRaw();
            long nextWireUp = counters.rxWire(), nextWireDown = counters.txWire();
            totals.addRawUp(Math.max(0, nextRawUp - rawUp)); totals.addRawDown(Math.max(0, nextRawDown - rawDown));
            totals.addZstdUp(Math.max(0, nextWireUp - wireUp)); totals.addZstdDown(Math.max(0, nextWireDown - wireDown));
            rawUp = nextRawUp; rawDown = nextRawDown; wireUp = nextWireUp; wireDown = nextWireDown;
        }
    }
}
