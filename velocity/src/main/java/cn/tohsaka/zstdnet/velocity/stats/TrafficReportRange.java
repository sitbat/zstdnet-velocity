/* Derived from wish131400/zstdnet, Copyright (c) [2026] [wish], MIT License. */
package cn.tohsaka.zstdnet.velocity.stats;

import java.util.Locale;

public enum TrafficReportRange {
    TODAY("today"),
    SESSION("session"),
    LAST_24_HOURS("24h"),
    LAST_7_DAYS("7d"),
    LAST_30_DAYS("30d");

    private final String id;

    TrafficReportRange(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    public static TrafficReportRange parse(String value) {
        String normalized = value == null ? "today" : value.trim().toLowerCase(Locale.ROOT);
        for (TrafficReportRange range : values()) {
            if (range.id.equals(normalized)) {
                return range;
            }
        }
        throw new IllegalArgumentException("Unsupported traffic report range: " + value);
    }
}
