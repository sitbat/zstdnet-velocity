/* Derived from wish131400/zstdnet, Copyright (c) [2026] [wish], MIT License. */
package cn.tohsaka.zstdnet.velocity.stats;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.math.BigInteger;

/**
 * Owns traffic history for one Minecraft server process. Proxy runtime reloads may come and go,
 * while this service and its {@link TrafficStats} instance remain alive for the whole server run.
 */
public final class TrafficStatisticsService {
    public static final int REPORT_SCHEMA_VERSION = 1;
    public static final long BUCKET_MILLIS = 5L * 60L * 1000L;
    private static final long SAMPLE_INTERVAL_MILLIS = 1000L;
    private static final long PERSIST_INTERVAL_MILLIS = 30L * 1000L;
    private static final Logger LOGGER = LoggerFactory.getLogger(TrafficStatisticsService.class);
    private static final Gson STORAGE_GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final Gson REPORT_GSON = new GsonBuilder().disableHtmlEscaping().create();

    private final Path root;
    private final Path dailyDir;
    private final Path seriesDir;
    private final Path runsDir;
    private final Path statePath;
    private final ZoneId zoneId;
    private final TrafficStats stats;
    private final LongSupplier clock;
    private final boolean automaticSampling;
    private final Map<LocalDate, DailyRecord> dailyCache = new HashMap<>();
    private final Map<LocalDate, SeriesFile> seriesCache = new HashMap<>();
    private final Map<LocalDate, Boolean> dirtyDaily = new HashMap<>();
    private final Map<LocalDate, Boolean> dirtySeries = new HashMap<>();
    private final Map<Long, Bucket> sessionBuckets = new HashMap<>();

    private boolean active;
    private String runId = "";
    private long runStartedAt;
    private long lastSampleAt;
    private long lastPersistAt;
    private TrafficStats.Snapshot previousSnapshot;
    private Metrics runMetrics = new Metrics();
    private long latestIngressRate;
    private long latestEgressRate;
    private ScheduledExecutorService sampler;

    public TrafficStatisticsService(Path root, ZoneId zoneId, TrafficStats stats) {
        this(root, zoneId, stats, System::currentTimeMillis, true);
    }

    /** Package-visible deterministic clock/sampler control for persistence regression tests. */
    TrafficStatisticsService(Path root, ZoneId zoneId, TrafficStats stats, LongSupplier clock, boolean automaticSampling) {
        this.root = root;
        this.dailyDir = root.resolve("daily");
        this.seriesDir = root.resolve("series");
        this.runsDir = root.resolve("runs");
        this.statePath = root.resolve("state-v1.json");
        this.zoneId = zoneId;
        this.stats = stats;
        this.clock = clock;
        this.automaticSampling = automaticSampling;
    }

    public synchronized void startSession() {
        if (active) stopSession();
        long now = clock.getAsLong();
        stopSampler();
        ensureDirectories();
        recoverInterruptedSession();
        stats.reset();
        active = true;
        runId = runId(now);
        runStartedAt = now;
        lastSampleAt = now;
        lastPersistAt = 0L;
        previousSnapshot = stats.snapshot();
        runMetrics = new Metrics();
        sessionBuckets.clear();
        latestIngressRate = 0L;
        latestEgressRate = 0L;
        persist(now, true);
        if (!automaticSampling) return;
        sampler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "zstdnet-traffic-stats");
            thread.setDaemon(true);
            return thread;
        });
        sampler.scheduleAtFixedRate(this::tickSafely, 1L, 1L, TimeUnit.SECONDS);
    }

    public synchronized void tick() {
        sample(clock.getAsLong(), false);
    }

    /** Forces a final sub-second counter delta into history before atomically persisting it. */
    public synchronized void flush() {
        if (active) sample(clock.getAsLong(), true);
        else persistDirtyFiles();
    }

    public synchronized void stopSession() {
        if (!active) {
            return;
        }
        stopSampler();
        long now = Math.max(clock.getAsLong(), lastSampleAt + 1L);
        sample(now, true);
        RunRecord run = currentRun(now, false, false);
        writeRun(run);
        persistDirtyFiles();
        // If unlinking fails, an explicitly inactive state prevents a false interrupted run.
        State finalState = new State();
        finalState.active = false;
        finalState.runId = runId;
        finalState.runStartedAt = runStartedAt;
        finalState.lastSampleAt = lastSampleAt;
        finalState.metrics = runMetrics.copy();
        writeJson(statePath, finalState);
        active = false;
        try {
            Files.deleteIfExists(statePath);
        } catch (IOException e) {
            LOGGER.warn("[zstdnet-stats] failed to remove active state {}: {}", statePath, e.toString());
        }
    }

    public synchronized boolean isActive() {
        return active;
    }

    private void tickSafely() {
        try {
            tick();
        } catch (RuntimeException e) {
            LOGGER.warn("[zstdnet-stats] background sample failed: {}", e.toString());
        }
    }

    private void stopSampler() {
        ScheduledExecutorService current = sampler;
        sampler = null;
        if (current != null) {
            current.shutdownNow();
        }
    }

    public synchronized String buildReportJson(String range, String mode, String listen) {
        TrafficReport report = buildReport(TrafficReportRange.parse(range), mode, listen, clock.getAsLong());
        return REPORT_GSON.toJson(report);
    }

    public synchronized TrafficReport buildReport(TrafficReportRange range, String mode, String listen, long now) {
        if (active) {
            sample(now, false);
        }

        long from = rangeStart(range, now);
        long bucketSize = range == TrafficReportRange.LAST_30_DAYS ? 60L * 60L * 1000L : BUCKET_MILLIS;
        List<Bucket> sourceBuckets = range == TrafficReportRange.SESSION ? sourceBucketsForSession(now) : readBuckets(from, now);
        List<Bucket> reportBuckets = aggregateBuckets(sourceBuckets, bucketSize, from, now);

        TrafficReport report = new TrafficReport();
        report.schemaVersion = REPORT_SCHEMA_VERSION;
        report.range = range.id();
        report.generatedAt = now;
        report.from = from;
        report.to = now;
        report.server.mode = safe(mode, "unknown");
        report.server.listen = safe(listen, "");
        report.server.timezone = zoneId.getId();
        report.server.sessionId = runId;
        report.server.sessionStartedAt = runStartedAt;
        report.server.sessionActive = active;

        Metrics selectedMetrics = range == TrafficReportRange.SESSION ? runMetrics.copy() : sum(reportBuckets);
        report.selected = totals(selectedMetrics, reportBuckets);

        long todayStart = ZonedDateTime.ofInstant(Instant.ofEpochMilli(now), zoneId)
            .truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli();
        List<Bucket> todayBuckets = readBuckets(todayStart, now);
        report.today = totals(sum(todayBuckets), todayBuckets);

        List<Bucket> sessionBuckets = active ? filterBuckets(sourceBucketsForSession(now), runStartedAt, now) : List.of();
        report.session = totals(runMetrics.copy(), sessionBuckets);

        TrafficStats.Snapshot snapshot = stats.snapshot();
        report.current.activeConnections = snapshot.activeConnections();
        report.current.ingressBytesPerSecond = latestIngressRate;
        report.current.egressBytesPerSecond = latestEgressRate;
        report.current.sampledAt = lastSampleAt;

        for (Bucket bucket : reportBuckets) {
            report.series.add(seriesPoint(bucket));
        }
        report.daily.addAll(buildDailyPoints(from, now));
        report.runs.addAll(buildRunPoints(from, now));
        if (active) {
            report.runs.add(0, runPoint(currentRun(now, true, false)));
        }
        return report;
    }

    private void sample(long now, boolean forcePersist) {
        if (forcePersist && active) now = Math.max(now, lastSampleAt + 1L);
        if (!active || now <= lastSampleAt) {
            if (forcePersist && active) {
                persist(now, true);
            }
            return;
        }
        if (!forcePersist && now - lastSampleAt < SAMPLE_INTERVAL_MILLIS) {
            return;
        }

        TrafficStats.Snapshot current = stats.snapshot();
        Metrics delta = Metrics.delta(previousSnapshot, current);
        long elapsed = Math.max(1L, now - lastSampleAt);
        delta.durationMillis = elapsed;
        latestIngressRate = perSecond(delta.wireIngress(), elapsed);
        latestEgressRate = perSecond(delta.wireEgress(), elapsed);

        // Split a delayed sample at five-minute and local midnight boundaries. This preserves
        // total bytes exactly and never stores the previous day's final bucket in today's file.
        List<SampleSegment> portions = new ArrayList<>();
        long segmentStart = lastSampleAt;
        while (segmentStart < now) {
            LocalDate date = Instant.ofEpochMilli(segmentStart).atZone(zoneId).toLocalDate();
            long midnight = date.plusDays(1L).atStartOfDay(zoneId).toInstant().toEpochMilli();
            long bucketStart = floor(segmentStart, BUCKET_MILLIS);
            long segmentEnd = Math.min(now, Math.min(midnight, bucketStart + BUCKET_MILLIS));
            Metrics portion = delta.portion(segmentStart - lastSampleAt, segmentEnd - lastSampleAt, elapsed);
            // Complete potentially failing file reads before applying any delta in memory.
            // A retry after an unreadable old file must not count already-applied bytes twice.
            dailyFor(date);
            seriesFor(date);
            portions.add(new SampleSegment(date, segmentStart, segmentEnd, bucketStart, portion));
            segmentStart = segmentEnd;
        }
        runMetrics.add(delta);
        for (SampleSegment segment : portions) {
            LocalDate date = segment.date;
            DailyRecord daily = dailyFor(date);
            daily.date = date.toString();
            if (daily.firstSampleAt == 0L) daily.firstSampleAt = segment.start;
            daily.lastSampleAt = segment.end;
            daily.metrics.add(segment.metrics);
            dirtyDaily.put(date, Boolean.TRUE);
            SeriesFile series = seriesFor(date);
            series.date = date.toString();
            series.bucket(segment.bucketStart).metrics.add(segment.metrics);
            sessionBuckets.computeIfAbsent(segment.bucketStart, Bucket::new).metrics.add(segment.metrics);
            dirtySeries.put(date, Boolean.TRUE);
        }

        previousSnapshot = current;
        lastSampleAt = now;
        if (forcePersist || now - lastPersistAt >= PERSIST_INTERVAL_MILLIS) {
            persist(now, true);
        }
    }

    private void persist(long now, boolean includeState) {
        ensureDirectories();
        persistDirtyFiles();
        if (includeState && active) {
            State state = new State();
            state.active = true;
            state.runId = runId;
            state.runStartedAt = runStartedAt;
            state.lastSampleAt = lastSampleAt;
            state.metrics = runMetrics.copy();
            writeJson(statePath, state);
        }
        lastPersistAt = now;
    }

    private void persistDirtyFiles() {
        for (LocalDate date : new ArrayList<>(dirtyDaily.keySet())) {
            if (Boolean.TRUE.equals(dirtyDaily.get(date))) {
                writeJson(dailyDir.resolve(date + ".json"), dailyCache.get(date));
            }
            dirtyDaily.remove(date);
        }
        for (LocalDate date : new ArrayList<>(dirtySeries.keySet())) {
            if (Boolean.TRUE.equals(dirtySeries.get(date))) {
                SeriesFile file = seriesCache.get(date);
                file.buckets.sort(Comparator.comparingLong(bucket -> bucket.start));
                writeJson(seriesDir.resolve(date + ".json"), file);
            }
            dirtySeries.remove(date);
        }
    }

    private void recoverInterruptedSession() {
        if (!Files.isRegularFile(statePath)) {
            return;
        }
        State state = readJson(statePath, State.class);
        if (state != null && state.active && state.runStartedAt > 0L) {
            RunRecord interrupted = new RunRecord();
            interrupted.id = state.runId != null && state.runId.matches("[0-9]+-[a-f0-9]{8}")
                ? state.runId : runId(state.runStartedAt);
            interrupted.startedAt = state.runStartedAt;
            interrupted.endedAt = Math.max(state.runStartedAt, state.lastSampleAt);
            interrupted.interrupted = true;
            interrupted.metrics = state.metrics == null ? new Metrics() : state.metrics;
            writeRun(interrupted);
            LOGGER.info("[zstdnet-stats] recovered interrupted run {}", interrupted.id);
        }
        try {
            Files.deleteIfExists(statePath);
        } catch (IOException e) {
            LOGGER.warn("[zstdnet-stats] failed to clear recovered state: {}", e.toString());
        }
    }

    private RunRecord currentRun(long endedAt, boolean activeRun, boolean interrupted) {
        RunRecord run = new RunRecord();
        run.id = runId;
        run.startedAt = runStartedAt;
        run.endedAt = endedAt;
        run.active = activeRun;
        run.interrupted = interrupted;
        run.metrics = runMetrics.copy();
        return run;
    }

    private void writeRun(RunRecord run) {
        if (run == null || run.id == null || run.id.isBlank()) {
            return;
        }
        writeJson(runsDir.resolve(run.id + ".json"), run);
    }

    private long rangeStart(TrafficReportRange range, long now) {
        return switch (range) {
            case TODAY -> ZonedDateTime.ofInstant(Instant.ofEpochMilli(now), zoneId)
                .truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli();
            case SESSION -> active ? runStartedAt : now;
            case LAST_24_HOURS -> now - 24L * 60L * 60L * 1000L;
            case LAST_7_DAYS -> now - 7L * 24L * 60L * 60L * 1000L;
            case LAST_30_DAYS -> now - 30L * 24L * 60L * 60L * 1000L;
        };
    }

    private List<Bucket> sourceBucketsForSession(long now) {
        List<Bucket> buckets = new ArrayList<>();
        for (Bucket bucket : sessionBuckets.values()) {
            if (bucket.start <= now) buckets.add(bucket.copy());
        }
        buckets.sort(Comparator.comparingLong(bucket -> bucket.start));
        return buckets;
    }

    private List<Bucket> readBuckets(long from, long to) {
        List<Bucket> result = new ArrayList<>();
        LocalDate first = Instant.ofEpochMilli(from).atZone(zoneId).toLocalDate();
        LocalDate last = Instant.ofEpochMilli(to).atZone(zoneId).toLocalDate();
        for (LocalDate date = first; !date.isAfter(last); date = date.plusDays(1L)) {
            SeriesFile file = seriesFor(date);
            for (Bucket bucket : file.buckets) {
                if (bucket.start + BUCKET_MILLIS > from && bucket.start <= to) {
                    result.add(bucket.copy());
                }
            }
        }
        result.sort(Comparator.comparingLong(bucket -> bucket.start));
        return result;
    }

    private List<Bucket> filterBuckets(List<Bucket> buckets, long from, long to) {
        List<Bucket> result = new ArrayList<>();
        for (Bucket bucket : buckets) {
            if (bucket.start + BUCKET_MILLIS > from && bucket.start <= to) {
                result.add(bucket);
            }
        }
        return result;
    }

    private List<Bucket> aggregateBuckets(List<Bucket> source, long targetSize, long from, long to) {
        if (targetSize <= BUCKET_MILLIS) {
            return filterBuckets(source, from, to);
        }
        Map<Long, Bucket> aggregate = new HashMap<>();
        for (Bucket bucket : source) {
            long start = floor(bucket.start, targetSize);
            aggregate.computeIfAbsent(start, Bucket::new).metrics.add(bucket.metrics);
        }
        List<Bucket> result = new ArrayList<>(aggregate.values());
        result.sort(Comparator.comparingLong(bucket -> bucket.start));
        return result;
    }

    private List<TrafficReport.DailyPoint> buildDailyPoints(long from, long to) {
        List<TrafficReport.DailyPoint> points = new ArrayList<>();
        LocalDate first = Instant.ofEpochMilli(from).atZone(zoneId).toLocalDate();
        LocalDate last = Instant.ofEpochMilli(to).atZone(zoneId).toLocalDate();
        for (LocalDate date = first; !date.isAfter(last); date = date.plusDays(1L)) {
            DailyRecord record = dailyFor(date);
            TrafficReport.DailyPoint point = new TrafficReport.DailyPoint();
            copyTotals(totals(record.metrics, seriesFor(date).buckets), point);
            point.date = date.toString();
            point.firstSampleAt = record.firstSampleAt;
            point.lastSampleAt = record.lastSampleAt;
            points.add(point);
        }
        return points;
    }

    private List<TrafficReport.RunPoint> buildRunPoints(long from, long to) {
        List<TrafficReport.RunPoint> points = new ArrayList<>();
        if (!Files.isDirectory(runsDir)) {
            return points;
        }
        try (var paths = Files.list(runsDir)) {
            paths.filter(path -> path.getFileName().toString().endsWith(".json"))
                .sorted(Comparator.comparing(Path::getFileName).reversed())
                .limit(50L)
                .forEach(path -> {
                    RunRecord run = readJson(path, RunRecord.class);
                    if (run != null && run.endedAt >= from && run.startedAt <= to) {
                        points.add(runPoint(run));
                    }
                });
        } catch (IOException e) {
            LOGGER.warn("[zstdnet-stats] failed to list run history: {}", e.toString());
        }
        points.sort(Comparator.comparingLong((TrafficReport.RunPoint point) -> point.startedAt).reversed());
        return points;
    }

    private TrafficReport.RunPoint runPoint(RunRecord run) {
        TrafficReport.RunPoint point = new TrafficReport.RunPoint();
        copyTotals(totals(run.metrics, List.of()), point);
        point.id = run.id;
        point.startedAt = run.startedAt;
        point.endedAt = run.endedAt;
        point.active = run.active;
        point.interrupted = run.interrupted;
        return point;
    }

    private TrafficReport.SeriesPoint seriesPoint(Bucket bucket) {
        TrafficReport.SeriesPoint point = new TrafficReport.SeriesPoint();
        point.timestamp = bucket.start;
        point.durationSeconds = Math.max(1L, bucket.metrics.durationMillis / 1000L);
        point.rawIngressBytes = bucket.metrics.rawIngress;
        point.rawEgressBytes = bucket.metrics.rawEgress;
        point.tcpIngressBytes = bucket.metrics.tcpIngress;
        point.tcpEgressBytes = bucket.metrics.tcpEgress;
        point.udpIngressBytes = bucket.metrics.udpIngress;
        point.udpEgressBytes = bucket.metrics.udpEgress;
        point.ingressBytesPerSecond = perSecond(bucket.metrics.wireIngress(), bucket.metrics.durationMillis);
        point.egressBytesPerSecond = perSecond(bucket.metrics.wireEgress(), bucket.metrics.durationMillis);
        return point;
    }

    private TrafficReport.Totals totals(Metrics metrics, List<Bucket> buckets) {
        TrafficReport.Totals totals = new TrafficReport.Totals();
        long durationMillis = Math.max(0L, metrics.durationMillis);
        totals.durationSeconds = durationMillis == 0L ? 0L : Math.max(1L, durationMillis / 1000L);
        totals.rawIngressBytes = metrics.rawIngress;
        totals.rawEgressBytes = metrics.rawEgress;
        totals.tcpIngressBytes = metrics.tcpIngress;
        totals.tcpEgressBytes = metrics.tcpEgress;
        totals.udpIngressBytes = metrics.udpIngress;
        totals.udpEgressBytes = metrics.udpEgress;
        totals.wireIngressBytes = metrics.wireIngress();
        totals.wireEgressBytes = metrics.wireEgress();
        totals.averageIngressBytesPerSecond = perSecond(totals.wireIngressBytes, durationMillis);
        totals.averageEgressBytesPerSecond = perSecond(totals.wireEgressBytes, durationMillis);

        List<Long> ingressRates = new ArrayList<>();
        List<Long> egressRates = new ArrayList<>();
        for (Bucket bucket : buckets) {
            ingressRates.add(perSecond(bucket.metrics.wireIngress(), bucket.metrics.durationMillis));
            egressRates.add(perSecond(bucket.metrics.wireEgress(), bucket.metrics.durationMillis));
        }
        totals.peakIngressBytesPerSecond = percentile(ingressRates, 1.0D);
        totals.peakEgressBytesPerSecond = percentile(egressRates, 1.0D);
        totals.p95IngressBytesPerSecond = percentile(ingressRates, 0.95D);
        totals.p95EgressBytesPerSecond = percentile(egressRates, 0.95D);
        totals.p99IngressBytesPerSecond = percentile(ingressRates, 0.99D);
        totals.p99EgressBytesPerSecond = percentile(egressRates, 0.99D);

        long raw = metrics.rawIngress + metrics.rawEgress;
        long wireTcp = metrics.tcpIngress + metrics.tcpEgress;
        totals.savedBytes = Math.max(0L, raw - wireTcp);
        totals.wireToRawPercent = raw <= 0L ? 0.0D : (double) wireTcp * 100.0D / (double) raw;
        return totals;
    }

    private void copyTotals(TrafficReport.Totals from, TrafficReport.Totals to) {
        to.durationSeconds = from.durationSeconds;
        to.rawIngressBytes = from.rawIngressBytes;
        to.rawEgressBytes = from.rawEgressBytes;
        to.tcpIngressBytes = from.tcpIngressBytes;
        to.tcpEgressBytes = from.tcpEgressBytes;
        to.udpIngressBytes = from.udpIngressBytes;
        to.udpEgressBytes = from.udpEgressBytes;
        to.wireIngressBytes = from.wireIngressBytes;
        to.wireEgressBytes = from.wireEgressBytes;
        to.averageIngressBytesPerSecond = from.averageIngressBytesPerSecond;
        to.averageEgressBytesPerSecond = from.averageEgressBytesPerSecond;
        to.peakIngressBytesPerSecond = from.peakIngressBytesPerSecond;
        to.peakEgressBytesPerSecond = from.peakEgressBytesPerSecond;
        to.p95IngressBytesPerSecond = from.p95IngressBytesPerSecond;
        to.p95EgressBytesPerSecond = from.p95EgressBytesPerSecond;
        to.p99IngressBytesPerSecond = from.p99IngressBytesPerSecond;
        to.p99EgressBytesPerSecond = from.p99EgressBytesPerSecond;
        to.savedBytes = from.savedBytes;
        to.wireToRawPercent = from.wireToRawPercent;
    }

    private long percentile(List<Long> values, double percentile) {
        if (values.isEmpty()) {
            return 0L;
        }
        values.sort(Long::compareTo);
        int index = (int) Math.ceil(percentile * values.size()) - 1;
        return values.get(Math.max(0, Math.min(values.size() - 1, index)));
    }

    private Metrics sum(List<Bucket> buckets) {
        Metrics result = new Metrics();
        for (Bucket bucket : buckets) {
            result.add(bucket.metrics);
        }
        return result;
    }

    private DailyRecord dailyFor(LocalDate date) {
        return dailyCache.computeIfAbsent(date, key -> {
            DailyRecord record = readJson(dailyDir.resolve(key + ".json"), DailyRecord.class);
            if (record != null && record.metrics == null) throw new IllegalStateException("Invalid daily history for " + key);
            return record == null ? new DailyRecord() : record;
        });
    }

    private SeriesFile seriesFor(LocalDate date) {
        return seriesCache.computeIfAbsent(date, key -> {
            SeriesFile file = readJson(seriesDir.resolve(key + ".json"), SeriesFile.class);
            if (file != null && (file.buckets == null || file.buckets.stream().anyMatch(bucket -> bucket == null || bucket.metrics == null))) {
                throw new IllegalStateException("Invalid series history for " + key);
            }
            return file == null ? new SeriesFile() : file;
        });
    }

    private void ensureDirectories() {
        try {
            Files.createDirectories(dailyDir);
            Files.createDirectories(seriesDir);
            Files.createDirectories(runsDir);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create zstdnet stats directory " + root, e);
        }
    }

    private <T> T readJson(Path path, Class<T> type) {
        if (!Files.isRegularFile(path)) {
            return null;
        }
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            T value = STORAGE_GSON.fromJson(reader, type);
            if (value == null) throw new IOException("Traffic history file is empty or null");
            return value;
        } catch (Exception e) {
            // An unreadable existing file is not an empty history. Keep it intact rather than
            // silently replacing prior records with freshly initialized zero counters.
            throw new IllegalStateException("Unable to read ZstdNet traffic history " + path, e);
        }
    }

    private void writeJson(Path path, Object value) {
        try {
            Files.createDirectories(path.getParent());
            Path temp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(
                temp,
                STORAGE_GSON.toJson(value),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            );
            try {
                Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            // Propagate so callers retain dirty flags and retry rather than acknowledge a write
            // that never happened. On shutdown the live state remains available for recovery.
            throw new IllegalStateException("Unable to persist ZstdNet traffic history " + path, e);
        }
    }

    private static long floor(long value, long size) {
        return Math.floorDiv(value, size) * size;
    }

    private static long perSecond(long bytes, long durationMillis) {
        return durationMillis <= 0L ? 0L : Math.max(0L, Math.round(bytes * 1000.0D / durationMillis));
    }

    private static String runId(long startedAt) {
        return String.format(Locale.ROOT, "%d-%s", startedAt, UUID.randomUUID().toString().substring(0, 8));
    }

    private static String safe(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static final class State {
        boolean active;
        String runId;
        long runStartedAt;
        long lastSampleAt;
        Metrics metrics = new Metrics();
    }

    private record SampleSegment(LocalDate date, long start, long end, long bucketStart, Metrics metrics) { }

    private static final class DailyRecord {
        String date = "";
        long firstSampleAt;
        long lastSampleAt;
        Metrics metrics = new Metrics();
    }

    private static final class SeriesFile {
        String date = "";
        List<Bucket> buckets = new ArrayList<>();

        Bucket bucket(long start) {
            if (!buckets.isEmpty()) {
                Bucket last = buckets.get(buckets.size() - 1);
                if (last.start == start) {
                    return last;
                }
            }
            for (Bucket bucket : buckets) {
                if (bucket.start == start) {
                    return bucket;
                }
            }
            Bucket bucket = new Bucket(start);
            buckets.add(bucket);
            return bucket;
        }
    }

    private static final class RunRecord {
        String id = "";
        long startedAt;
        long endedAt;
        boolean active;
        boolean interrupted;
        Metrics metrics = new Metrics();
    }

    private static final class Bucket {
        long start;
        Metrics metrics = new Metrics();

        Bucket() {
        }

        Bucket(long start) {
            this.start = start;
        }

        Bucket copy() {
            Bucket copy = new Bucket(start);
            copy.metrics = metrics.copy();
            return copy;
        }
    }

    private static final class Metrics {
        long durationMillis;
        long rawIngress;
        long rawEgress;
        long tcpIngress;
        long tcpEgress;
        long udpIngress;
        long udpEgress;

        void add(Metrics other) {
            if (other == null) {
                return;
            }
            durationMillis += Math.max(0L, other.durationMillis);
            rawIngress += Math.max(0L, other.rawIngress);
            rawEgress += Math.max(0L, other.rawEgress);
            tcpIngress += Math.max(0L, other.tcpIngress);
            tcpEgress += Math.max(0L, other.tcpEgress);
            udpIngress += Math.max(0L, other.udpIngress);
            udpEgress += Math.max(0L, other.udpEgress);
        }

        long wireIngress() {
            return tcpIngress + udpIngress;
        }

        long wireEgress() {
            return tcpEgress + udpEgress;
        }

        Metrics copy() {
            Metrics copy = new Metrics();
            copy.durationMillis = durationMillis;
            copy.rawIngress = rawIngress;
            copy.rawEgress = rawEgress;
            copy.tcpIngress = tcpIngress;
            copy.tcpEgress = tcpEgress;
            copy.udpIngress = udpIngress;
            copy.udpEgress = udpEgress;
            return copy;
        }

        Metrics portion(long fromMillis, long toMillis, long totalMillis) {
            Metrics part = new Metrics();
            part.durationMillis = toMillis - fromMillis;
            part.rawIngress = portion(rawIngress, fromMillis, toMillis, totalMillis);
            part.rawEgress = portion(rawEgress, fromMillis, toMillis, totalMillis);
            part.tcpIngress = portion(tcpIngress, fromMillis, toMillis, totalMillis);
            part.tcpEgress = portion(tcpEgress, fromMillis, toMillis, totalMillis);
            part.udpIngress = portion(udpIngress, fromMillis, toMillis, totalMillis);
            part.udpEgress = portion(udpEgress, fromMillis, toMillis, totalMillis);
            return part;
        }

        private static long portion(long bytes, long from, long to, long duration) {
            if (from == 0L && to == duration) return bytes;
            BigInteger value = BigInteger.valueOf(bytes);
            BigInteger divisor = BigInteger.valueOf(duration);
            return value.multiply(BigInteger.valueOf(to)).divide(divisor).longValue()
                - value.multiply(BigInteger.valueOf(from)).divide(divisor).longValue();
        }

        static Metrics delta(TrafficStats.Snapshot previous, TrafficStats.Snapshot current) {
            Metrics delta = new Metrics();
            if (previous == null || current == null) {
                return delta;
            }
            delta.rawIngress = positiveDelta(current.rawIngressBytes(), previous.rawIngressBytes());
            delta.rawEgress = positiveDelta(current.rawEgressBytes(), previous.rawEgressBytes());
            delta.tcpIngress = positiveDelta(current.tcpIngressBytes(), previous.tcpIngressBytes());
            delta.tcpEgress = positiveDelta(current.tcpEgressBytes(), previous.tcpEgressBytes());
            delta.udpIngress = positiveDelta(current.udpIngressBytes(), previous.udpIngressBytes());
            delta.udpEgress = positiveDelta(current.udpEgressBytes(), previous.udpEgressBytes());
            return delta;
        }

        private static long positiveDelta(long current, long previous) {
            return current >= previous ? current - previous : current;
        }
    }
}
