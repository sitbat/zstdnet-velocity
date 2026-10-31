/* Derived from wish131400/zstdnet, Copyright (c) [2026] [wish], MIT License. */
package cn.tohsaka.zstdnet.report;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Builds a self-contained local HTML report from the template bundled inside the client mod. */
public final class TrafficReportGenerator {
    private static final String TEMPLATE_RESOURCE = "/assets/zstdnet/dashboard/report-template.html";
    private static final String LOGO_RESOURCE = "/assets/zstdnet/dashboard/zstdnet-logo.png";
    private static final String DATA_MARKER = "/*__ZSTDNET_REPORT_JSON__*/";
    private static final String LOGO_MARKER = "/*__ZSTDNET_LOGO__*/";
    private static final int MAX_ARCHIVED_REPORTS = 10;
    private static final Pattern ARCHIVE_FILE = Pattern.compile(
        "zstdnet-report-\\d{8}-\\d{6}(?:-\\d{3})?(?:-\\d+)?\\.html"
    );
    private static final DateTimeFormatter FILE_TIME = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS");

    private TrafficReportGenerator() {
    }

    public static GeneratedReport generate(Path reportsDir, String reportJson) throws IOException {
        return generateInDirectory(reportsDir, reportJson);
    }

    public static GeneratedReport generate(Path reportsRoot, String serverIdentity, String reportJson) throws IOException {
        return generateInDirectory(
            reportsRoot.resolve(serverDirectoryName(serverIdentity)),
            withConnectedAddress(reportJson, serverIdentity)
        );
    }

    private static synchronized GeneratedReport generateInDirectory(Path reportsDir, String reportJson) throws IOException {
        if (reportJson == null || reportJson.isBlank()) {
            throw new IOException("Server returned an empty traffic report");
        }
        Files.createDirectories(reportsDir);
        String template = readTemplate();
        if (!template.contains(DATA_MARKER) || !template.contains(LOGO_MARKER)) {
            throw new IOException("Bundled traffic report template is invalid");
        }

        String safeJson = reportJson
            .replace("<", "\\u003c")
            .replace(">", "\\u003e")
            .replace("&", "\\u0026")
            .replace("\u2028", "\\u2028")
            .replace("\u2029", "\\u2029");
        String html = template
            .replace(DATA_MARKER, safeJson)
            .replace(LOGO_MARKER, resourceBase64(LOGO_RESOURCE));
        String stamp = FILE_TIME.format(Instant.now().atZone(ZoneId.systemDefault()));
        Path archive = nextArchivePath(reportsDir, stamp);
        Path latest = reportsDir.resolve("latest.html").toAbsolutePath().normalize();
        writeAtomically(archive, html);
        writeAtomically(latest, html);
        pruneArchives(reportsDir);
        return new GeneratedReport(latest, archive);
    }

    private static String withConnectedAddress(String reportJson, String serverIdentity) throws IOException {
        if (reportJson == null || reportJson.isBlank() || serverIdentity == null || serverIdentity.isBlank()) {
            return reportJson;
        }
        try {
            JsonElement parsed = JsonParser.parseString(reportJson);
            if (!parsed.isJsonObject()) {
                throw new JsonParseException("Traffic report root must be an object");
            }
            JsonObject report = parsed.getAsJsonObject();
            JsonObject server = report.has("server") && report.get("server").isJsonObject()
                ? report.getAsJsonObject("server")
                : new JsonObject();
            server.addProperty("connectedAddress", serverIdentity.trim());
            report.add("server", server);
            return report.toString();
        } catch (JsonParseException | IllegalStateException e) {
            throw new IOException("Server returned an invalid traffic report", e);
        }
    }

    static String serverDirectoryName(String serverIdentity) {
        String normalized = serverIdentity == null || serverIdentity.isBlank()
            ? "unknown-server"
            : serverIdentity.trim().toLowerCase(Locale.ROOT);
        String slug = normalized
            .replaceAll("[^a-z0-9._-]+", "_")
            .replaceAll("^[._-]+|[._-]+$", "");
        if (slug.isEmpty()) {
            slug = "server";
        } else if (slug.length() > 64) {
            slug = slug.substring(0, 64).replaceAll("[._-]+$", "");
        }
        return slug + "-" + String.format(Locale.ROOT, "%08x", normalized.hashCode());
    }

    private static Path nextArchivePath(Path reportsDir, String stamp) {
        Path archive = reportsDir.resolve("zstdnet-report-" + stamp + ".html").toAbsolutePath().normalize();
        int suffix = 2;
        while (Files.exists(archive)) {
            archive = reportsDir.resolve("zstdnet-report-" + stamp + "-" + suffix++ + ".html")
                .toAbsolutePath()
                .normalize();
        }
        return archive;
    }

    private static void pruneArchives(Path reportsDir) throws IOException {
        List<Path> archives;
        try (Stream<Path> files = Files.list(reportsDir)) {
            archives = files
                .filter(Files::isRegularFile)
                .filter(path -> ARCHIVE_FILE.matcher(path.getFileName().toString()).matches())
                .sorted(Comparator.comparing((Path path) -> path.getFileName().toString()).reversed())
                .toList();
        }
        for (int i = MAX_ARCHIVED_REPORTS; i < archives.size(); i++) {
            Files.deleteIfExists(archives.get(i));
        }
    }

    private static String readTemplate() throws IOException {
        try (InputStream input = TrafficReportGenerator.class.getResourceAsStream(TEMPLATE_RESOURCE)) {
            if (input == null) {
                throw new IOException("Missing bundled resource " + TEMPLATE_RESOURCE);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String resourceBase64(String resource) throws IOException {
        try (InputStream input = TrafficReportGenerator.class.getResourceAsStream(resource)) {
            if (input == null) {
                throw new IOException("Missing bundled resource " + resource);
            }
            return Base64.getEncoder().encodeToString(input.readAllBytes());
        }
    }

    private static void writeAtomically(Path path, String html) throws IOException {
        Path temp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.writeString(
            temp,
            html,
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
    }

    public record GeneratedReport(Path latest, Path archive) {
    }
}
