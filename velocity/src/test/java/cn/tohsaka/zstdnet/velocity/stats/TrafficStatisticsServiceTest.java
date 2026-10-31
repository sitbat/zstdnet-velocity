package cn.tohsaka.zstdnet.velocity.stats;

import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class TrafficStatisticsServiceTest {
    @TempDir Path directory;
    private static final ZoneId UTC = ZoneId.of("UTC");

    @Test
    void forcedFlushCapturesSubSecondTailWithoutWaitingForTheSampler() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-10-28T00:01:00Z").toEpochMilli());
        TrafficStats counters = new TrafficStats();
        TrafficStatisticsService service = service(counters, clock);
        service.startSession();
        counters.addRawUp(4096);
        counters.addRawDown(8192);
        counters.addZstdUp(1024);
        counters.addZstdDown(2048);
        counters.activeConn.set(3);
        clock.addAndGet(100);
        service.flush();
        TrafficReport report = service.buildReport(TrafficReportRange.SESSION, "VELOCITY", ":25565", clock.get());
        assertEquals(4096, report.selected.rawIngressBytes);
        assertEquals(8192, report.selected.rawEgressBytes);
        assertEquals(1024, report.selected.wireIngressBytes);
        assertEquals(2048, report.selected.wireEgressBytes);
        assertEquals(3, report.current.activeConnections);
        assertEquals(25.0, report.selected.wireToRawPercent);
        assertEquals("VELOCITY", report.server.mode);
        assertFalse(report.series.isEmpty());
        service.stopSession();
        assertFalse(Files.exists(directory.resolve("state-v1.json")));
    }

    @Test
    void interruptedSessionIsRecoveredWithoutMixingItIntoTheNextSessionBucket() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-10-28T00:01:00Z").toEpochMilli());
        TrafficStats beforeCrash = new TrafficStats();
        TrafficStatisticsService first = service(beforeCrash, clock);
        first.startSession();
        beforeCrash.addRawUp(1000);
        beforeCrash.addZstdUp(100);
        clock.addAndGet(1500);
        first.flush();

        TrafficStats afterRestart = new TrafficStats();
        TrafficStatisticsService recovered = service(afterRestart, clock);
        recovered.startSession();
        afterRestart.addRawUp(200);
        afterRestart.addZstdUp(20);
        clock.addAndGet(500);
        recovered.flush();
        TrafficReport report = recovered.buildReport(TrafficReportRange.SESSION, "VELOCITY", "", clock.get());
        assertEquals(200, report.selected.rawIngressBytes);
        assertEquals(200, report.session.rawIngressBytes);
        assertEquals(1200, report.today.rawIngressBytes);
        assertEquals(200, report.series.stream().mapToLong(point -> point.rawIngressBytes).sum());
        assertTrue(report.runs.stream().anyMatch(run -> run.interrupted && run.rawIngressBytes == 1000));
        try (var files = Files.list(directory.resolve("runs"))) {
            assertEquals(1, files.count());
        }
        recovered.stopSession();
    }

    @Test
    void delayedSampleIsSplitAcrossMidnightWithoutLosingBytes() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-10-27T23:59:59.500Z").toEpochMilli());
        TrafficStats counters = new TrafficStats();
        TrafficStatisticsService service = service(counters, clock);
        service.startSession();
        counters.addRawUp(1000);
        counters.addZstdUp(100);
        clock.addAndGet(1000);
        service.tick();
        TrafficReport today = service.buildReport(TrafficReportRange.TODAY, "VELOCITY", "", clock.get());
        TrafficReport full = service.buildReport(TrafficReportRange.LAST_24_HOURS, "VELOCITY", "", clock.get());
        assertEquals(500, today.selected.rawIngressBytes);
        assertEquals(50, today.selected.tcpIngressBytes);
        assertEquals(1000, full.selected.rawIngressBytes);
        assertEquals(2, full.series.size());
        assertEquals(1000, full.daily.stream().mapToLong(point -> point.rawIngressBytes).sum());
        service.stopSession();
    }

    @Test
    void failedAtomicWriteRemainsDirtyAndIsRetriedWithoutDoubleCounting() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-10-28T00:01:00Z").toEpochMilli());
        TrafficStats counters = new TrafficStats();
        TrafficStatisticsService service = service(counters, clock);
        service.startSession();
        counters.addRawUp(1000);
        counters.addZstdUp(100);
        Path blockedTemporaryFile = directory.resolve("series/2026-10-28.json.tmp");
        Files.createDirectory(blockedTemporaryFile);
        clock.addAndGet(31_000);
        assertThrows(IllegalStateException.class, service::tick);
        assertFalse(Files.exists(directory.resolve("series/2026-10-28.json")));
        Files.delete(blockedTemporaryFile);
        service.flush();
        String series = Files.readString(directory.resolve("series/2026-10-28.json"));
        long raw = JsonParser.parseString(series).getAsJsonObject().getAsJsonArray("buckets")
            .get(0).getAsJsonObject().getAsJsonObject("metrics").get("rawIngress").getAsLong();
        assertEquals(1000, raw);
        TrafficReport report = service.buildReport(TrafficReportRange.TODAY, "VELOCITY", "", clock.get());
        assertEquals(1000, report.selected.rawIngressBytes);
        service.stopSession();
    }

    @Test
    void exportsEverySupportedRangeWithTheOriginalSchema() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-10-28T12:00:00Z").toEpochMilli());
        TrafficStatisticsService service = service(new TrafficStats(), clock);
        service.startSession();
        for (String range : new String[]{"today", "session", "24h", "7d", "30d"}) {
            var report = JsonParser.parseString(service.buildReportJson(range, "VELOCITY", ":25565")).getAsJsonObject();
            assertEquals(1, report.get("schemaVersion").getAsInt());
            assertEquals(range, report.get("range").getAsString());
            assertTrue(report.has("selected") && report.has("today") && report.has("session"));
            assertTrue(report.has("series") && report.has("daily") && report.has("runs"));
        }
        assertThrows(IllegalArgumentException.class, () -> TrafficReportRange.parse("../../other"));
        service.stopSession();
    }

    @Test
    void corruptExistingStateIsPreservedInsteadOfOverwrittenWithAnEmptyRun() throws Exception {
        Files.writeString(directory.resolve("state-v1.json"), "broken-history-json");
        AtomicLong clock = new AtomicLong(Instant.parse("2026-10-28T12:00:00Z").toEpochMilli());
        TrafficStatisticsService service = service(new TrafficStats(), clock);
        assertThrows(IllegalStateException.class, service::startSession);
        assertEquals("broken-history-json", Files.readString(directory.resolve("state-v1.json")));
        assertFalse(service.isActive());
    }

    @Test
    void failedHistoryReadCanBeRetriedWithoutApplyingItsDeltaTwice() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-10-28T00:01:00Z").toEpochMilli());
        TrafficStats counters = new TrafficStats();
        TrafficStatisticsService service = service(counters, clock);
        service.startSession();
        Path existingSeries = directory.resolve("series/2026-10-28.json");
        Files.writeString(existingSeries, "broken-history-json");
        counters.addRawUp(1000);
        counters.addZstdUp(100);
        clock.addAndGet(1000);
        assertThrows(IllegalStateException.class, service::tick);
        Files.writeString(existingSeries, "{\"date\":\"2026-10-28\",\"buckets\":[]}");
        service.flush();
        TrafficReport report = service.buildReport(TrafficReportRange.SESSION, "VELOCITY", "", clock.get());
        assertEquals(1000, report.selected.rawIngressBytes);
        assertEquals(1000, report.today.rawIngressBytes);
        service.stopSession();
    }

    private TrafficStatisticsService service(TrafficStats counters, AtomicLong clock) {
        return new TrafficStatisticsService(directory, UTC, counters, clock::get, false);
    }
}
