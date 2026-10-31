/* Derived from wish131400/zstdnet, Copyright (c) [2026] [wish], MIT License. */
package cn.tohsaka.zstdnet.velocity.stats;

import java.util.ArrayList;
import java.util.List;

/** Versioned, platform-neutral payload sent from the server to a modded client. */
public final class TrafficReport {
    public int schemaVersion = 1;
    public String range = "today";
    public long generatedAt;
    public long from;
    public long to;
    public ServerInfo server = new ServerInfo();
    public Totals selected = new Totals();
    public Totals today = new Totals();
    public Totals session = new Totals();
    public Current current = new Current();
    public List<SeriesPoint> series = new ArrayList<>();
    public List<DailyPoint> daily = new ArrayList<>();
    public List<RunPoint> runs = new ArrayList<>();

    public static class ServerInfo {
        public String mode = "unknown";
        public String listen = "";
        public String timezone = "UTC";
        public String sessionId = "";
        public long sessionStartedAt;
        public boolean sessionActive;
    }

    public static class Totals {
        public long durationSeconds;
        public long rawIngressBytes;
        public long rawEgressBytes;
        public long tcpIngressBytes;
        public long tcpEgressBytes;
        public long udpIngressBytes;
        public long udpEgressBytes;
        public long wireIngressBytes;
        public long wireEgressBytes;
        public long averageIngressBytesPerSecond;
        public long averageEgressBytesPerSecond;
        public long peakIngressBytesPerSecond;
        public long peakEgressBytesPerSecond;
        public long p95IngressBytesPerSecond;
        public long p95EgressBytesPerSecond;
        public long p99IngressBytesPerSecond;
        public long p99EgressBytesPerSecond;
        public long savedBytes;
        public double wireToRawPercent;
    }

    public static class Current {
        public int activeConnections;
        public long ingressBytesPerSecond;
        public long egressBytesPerSecond;
        public long sampledAt;
    }

    public static class SeriesPoint {
        public long timestamp;
        public long durationSeconds;
        public long rawIngressBytes;
        public long rawEgressBytes;
        public long tcpIngressBytes;
        public long tcpEgressBytes;
        public long udpIngressBytes;
        public long udpEgressBytes;
        public long ingressBytesPerSecond;
        public long egressBytesPerSecond;
    }

    public static class DailyPoint extends Totals {
        public String date = "";
        public long firstSampleAt;
        public long lastSampleAt;
    }

    public static class RunPoint extends Totals {
        public String id = "";
        public long startedAt;
        public long endedAt;
        public boolean active;
        public boolean interrupted;
    }
}
