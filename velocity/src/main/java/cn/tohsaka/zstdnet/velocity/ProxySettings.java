package cn.tohsaka.zstdnet.velocity;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.nio.file.AtomicMoveNotSupportedException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

record ProxySettings(boolean enabled, int level, boolean debug, int maxConnectionsPerIp,
                     int maxRequestsPerWindow, Duration requestWindow, Duration banDuration, Set<UUID> reportAdminUuids) {
    ProxySettings {
        reportAdminUuids = Set.copyOf(reportAdminUuids);
    }

    ProxySettings(boolean enabled, int level, boolean debug, int maxConnectionsPerIp,
                  int maxRequestsPerWindow, Duration requestWindow, Duration banDuration) {
        this(enabled, level, debug, maxConnectionsPerIp, maxRequestsPerWindow, requestWindow, banDuration, Set.of());
    }
    static ProxySettings defaults(boolean enabled) {
        return new ProxySettings(enabled, 9, false, 64, 50, Duration.ofSeconds(10), Duration.ofMinutes(1));
    }

    static ProxySettings load(Path directory) throws IOException {
        Files.createDirectories(directory);
        Path file = directory.resolve("zstdnet.properties");
        if (Files.notExists(file)) {
            Files.writeString(file, """
                    # ZstdNet Velocity 2.1: client <-> proxy transport.
                    # Keep Velocity online-mode=true and modern player forwarding enabled.
                    enabled=true
                    # Compression level: 1-22. Higher values use more CPU; the window stays bounded.
                    level=9
                    debug=false
                    # Values <=0 disable the corresponding admission limit or time window.
                    max_conn_per_ip=64
                    max_req_per_window=50
                    request_window=10s
                    ban_duration=1m
                    # Additional report readers by authenticated UUID; empty denies ordinary players.
                    report_admin_uuids=
                    """, StandardCharsets.UTF_8);
        }
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        boolean enabled = strictBoolean(properties, "enabled", true);
        boolean debug = strictBoolean(properties, "debug", false);
        int level = integer(properties, "level", 9);
        if (level < 1 || level > 22) throw new IOException("ZstdNet level must be from 1 to 22");
        int maxConnections = integer(properties, "max_conn_per_ip", 64);
        int maxRequests = integer(properties, "max_req_per_window", 50);
        Duration window = duration(properties.getProperty("request_window", "10s"), "request_window");
        Duration ban = duration(properties.getProperty("ban_duration", "1m"), "ban_duration");
        Set<UUID> reportAdmins = new HashSet<>();
        for (String raw : properties.getProperty("report_admin_uuids", "").split(",")) {
            if (raw.isBlank()) continue;
            try { reportAdmins.add(UUID.fromString(raw.trim())); }
            catch (IllegalArgumentException error) { throw new IOException("report_admin_uuids must contain comma-separated UUIDs", error); }
        }
        Map<String, String> defaults = new LinkedHashMap<>();
        defaults.put("enabled", "true"); defaults.put("level", "9"); defaults.put("debug", "false");
        defaults.put("max_conn_per_ip", "64"); defaults.put("max_req_per_window", "50");
        defaults.put("request_window", "10s"); defaults.put("ban_duration", "1m");
        defaults.put("report_admin_uuids", "");
        StringBuilder missing = new StringBuilder();
        defaults.forEach((key, value) -> {
            if (!properties.containsKey(key)) missing.append(key).append('=').append(value).append('\n');
        });
        if (!missing.isEmpty()) Files.writeString(file, "\n# Added by ZstdNet 2.1\n" + missing,
                StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        return new ProxySettings(enabled, level, debug, maxConnections, maxRequests, window, ban, reportAdmins);
    }

    ProxySettings allowReport(Path directory, UUID player) throws IOException {
        HashSet<UUID> updated = new HashSet<>(reportAdminUuids);
        updated.add(player);
        String values = updated.stream().map(UUID::toString).sorted().collect(java.util.stream.Collectors.joining(","));
        Path file = directory.resolve("zstdnet.properties");
        String text = Files.readString(file, StandardCharsets.UTF_8);
        Pattern property = Pattern.compile("(?m)^\\h*report_admin_uuids\\h*[=:].*$");
        Matcher matcher = property.matcher(text);
        String line = "report_admin_uuids=" + values;
        text = matcher.find() ? matcher.replaceAll(Matcher.quoteReplacement(line)) : text + "\n" + line + "\n";
        Path temporary = directory.resolve("zstdnet.properties." + UUID.randomUUID() + ".tmp");
        try {
            Files.writeString(temporary, text, StandardCharsets.UTF_8);
            try { Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
            catch (AtomicMoveNotSupportedException error) { Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING); }
        } finally { Files.deleteIfExists(temporary); }
        return new ProxySettings(enabled, level, debug, maxConnectionsPerIp, maxRequestsPerWindow, requestWindow, banDuration, updated);
    }

    private static int integer(Properties properties, String name, int fallback) throws IOException {
        try { return Integer.parseInt(properties.getProperty(name, String.valueOf(fallback)).trim()); }
        catch (NumberFormatException error) { throw new IOException("ZstdNet " + name + " must be an integer", error); }
    }

    private static Duration duration(String input, String name) throws IOException {
        String value = input.trim().toLowerCase(java.util.Locale.ROOT);
        long multiplier = 1000;
        String number = value;
        if (value.endsWith("ms")) { multiplier = 1; number = value.substring(0, value.length() - 2); }
        else if (value.endsWith("s")) { number = value.substring(0, value.length() - 1); }
        else if (value.endsWith("m")) { multiplier = 60_000; number = value.substring(0, value.length() - 1); }
        else if (value.endsWith("h")) { multiplier = 3_600_000; number = value.substring(0, value.length() - 1); }
        else if (value.endsWith("d")) { multiplier = 86_400_000; number = value.substring(0, value.length() - 1); }
        try { return Duration.ofMillis(Math.multiplyExact(Long.parseLong(number.trim()), multiplier)); }
        catch (ArithmeticException | NumberFormatException error) {
            throw new IOException("ZstdNet " + name + " must be a duration such as 10s or 1m", error);
        }
    }

    private static boolean strictBoolean(Properties properties, String name, boolean fallback) throws IOException {
        String value = properties.getProperty(name, String.valueOf(fallback)).trim();
        if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
            throw new IOException("ZstdNet " + name + " must be true or false");
        }
        return Boolean.parseBoolean(value);
    }
}
