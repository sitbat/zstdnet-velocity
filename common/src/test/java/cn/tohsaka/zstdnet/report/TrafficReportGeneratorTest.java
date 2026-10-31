package cn.tohsaka.zstdnet.report;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class TrafficReportGeneratorTest {
    private static final String DATA_START = "<script id=\"report-data\" type=\"application/json\">";
    @TempDir Path directory;

    @Test
    void mixedCaseScriptTextIsEscapedWithoutChangingReportDataOrItsSchema() throws Exception {
        JsonObject report = JsonParser.parseString("""
            {"schemaVersion":1,"range":"session","generatedAt":123456,"from":100,"to":200,
             "server":{"mode":"VELOCITY","listen":"localhost:25565","timezone":"Asia/Shanghai","sessionActive":true},
             "selected":{"rawIngressBytes":1000,"wireIngressBytes":1500,"savedBytes":-500,"wireToRawPercent":150},
             "today":{},"session":{},"current":{"activeConnections":3},"daily":[],"runs":[],
             "series":[{"timestamp":100,"durationSeconds":1,"rawIngressBytes":1000,"tcpIngressBytes":1500,"ingressBytesPerSecond":1500}]}
            """).getAsJsonObject();
        String hostileText = "</ScRiPt><script>globalThis.unexpected=1</script><!--&>\u2028\u2029中文";
        report.addProperty("notes", hostileText);
        String rawJson = new GsonBuilder().disableHtmlEscaping().create().toJson(report);
        assertTrue(rawJson.contains("</ScRiPt>"), "the test must exercise literal mixed-case markup");

        var generated = TrafficReportGenerator.generate(directory, rawJson);
        String html = Files.readString(generated.latest(), StandardCharsets.UTF_8);
        String embedded = embeddedData(html);
        assertFalse(embedded.contains("<"));
        assertFalse(embedded.contains(">"));
        assertFalse(embedded.contains("&"));
        assertFalse(embedded.contains("\u2028"));
        assertFalse(embedded.contains("\u2029"));
        assertEquals(report, JsonParser.parseString(embedded));
        assertEquals(hostileText, JsonParser.parseString(embedded).getAsJsonObject().get("notes").getAsString());
        assertFalse(html.contains("/*__ZSTDNET_REPORT_JSON__*/"));
        assertFalse(html.contains("/*__ZSTDNET_LOGO__*/"));
        assertEquals(html, Files.readString(generated.archive(), StandardCharsets.UTF_8));
    }

    @Test
    void generatedDocumentContainsItsLogoAndKeepsTheConnectedServerIdentity() throws Exception {
        String server = "游戏.example:25565";
        var generated = TrafficReportGenerator.generate(directory, server,
            "{\"range\":\"today\",\"server\":{\"listen\":\"0.0.0.0:25565\"},\"series\":[]}");
        String html = Files.readString(generated.latest());
        JsonObject embedded = JsonParser.parseString(embeddedData(html)).getAsJsonObject();
        assertEquals(server, embedded.getAsJsonObject("server").get("connectedAddress").getAsString());
        assertEquals("0.0.0.0:25565", embedded.getAsJsonObject("server").get("listen").getAsString());
        var image = Pattern.compile("data:image/png;base64,([A-Za-z0-9+/=]+)").matcher(html);
        assertTrue(image.find(), "the HTML needs an embedded logo, not a missing external file");
        byte[] logo = Base64.getDecoder().decode(image.group(1));
        assertTrue(logo.length > 8);
        assertArrayEquals(new byte[] {(byte) 137, 80, 78, 71, 13, 10, 26, 10}, java.util.Arrays.copyOf(logo, 8));
        assertFalse(Pattern.compile("<script[^>]+\\bsrc=", Pattern.CASE_INSENSITIVE).matcher(html).find());
        assertTrue(generated.latest().normalize().startsWith(directory.toAbsolutePath().normalize()));
    }

    @Test
    void serverAddressCannotEscapeTheChosenReportRoot() throws Exception {
        var generated = TrafficReportGenerator.generate(directory, "../../outside/../../:evil<>|", "{\"series\":[]}");
        Path normalizedRoot = directory.toAbsolutePath().normalize();
        assertTrue(generated.latest().startsWith(normalizedRoot));
        assertTrue(generated.archive().startsWith(normalizedRoot));
        assertEquals(normalizedRoot, generated.latest().getParent().getParent());
        String folder = generated.latest().getParent().getFileName().toString();
        assertFalse(folder.contains("/"));
        assertFalse(folder.contains("\\"));
        assertFalse(folder.contains(":"));
    }

    @Test
    void invalidOrEmptyRemoteReportsDoNotProduceALatestDocument() {
        assertThrows(IOException.class, () -> TrafficReportGenerator.generate(directory, ""));
        assertThrows(IOException.class, () -> TrafficReportGenerator.generate(directory, null));
        assertThrows(IOException.class, () -> TrafficReportGenerator.generate(directory, "server.example", "[]"));
        assertThrows(IOException.class, () -> TrafficReportGenerator.generate(directory, "server.example", "{broken"));
        assertFalse(Files.exists(directory.resolve("latest.html")));
    }

    @Test
    void archivePruningPreservesOtherFilesAndLatestWhileRemovingOnlyOldReportArchives() throws Exception {
        Files.writeString(directory.resolve("keep-me.html"), "user document");
        for (int i = 0; i < 12; i++) {
            TrafficReportGenerator.generate(directory, "{\"series\":[],\"index\":" + i + "}");
        }
        try (var files = Files.list(directory)) {
            assertEquals(10, files.filter(path -> path.getFileName().toString().startsWith("zstdnet-report-")).count());
        }
        assertEquals("user document", Files.readString(directory.resolve("keep-me.html")));
        assertEquals(11, JsonParser.parseString(embeddedData(Files.readString(directory.resolve("latest.html"))))
            .getAsJsonObject().get("index").getAsInt());
        try (var files = Files.list(directory)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().endsWith(".tmp")));
        }
    }

    private static String embeddedData(String html) {
        int start = html.indexOf(DATA_START);
        assertTrue(start >= 0, "the report-data element must exist");
        start += DATA_START.length();
        int end = html.indexOf("</script>", start);
        assertTrue(end > start, "the report-data element must close");
        return html.substring(start, end);
    }
}
