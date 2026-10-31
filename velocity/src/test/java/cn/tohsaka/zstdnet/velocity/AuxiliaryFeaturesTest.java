package cn.tohsaka.zstdnet.velocity;

import cn.tohsaka.zstdnet.transport.TransportStats;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class AuxiliaryFeaturesTest {
    @TempDir Path temporary;

    @Test
    void rejectedAdmissionsAndRepeatedReleaseCannotDecrementSomeoneElsesConnection() {
        ConnectionLimiter limiter = new ConnectionLimiter(new ProxySettings(true, 9, false, 1, 0, Duration.ZERO, Duration.ZERO));
        long now = System.currentTimeMillis();
        var first = limiter.acquire("127.0.0.1", now);
        assertNotNull(first);
        assertNull(limiter.acquire("127.0.0.1", now));
        assertNull(limiter.acquire("127.0.0.1", now));
        first.close();
        var second = limiter.acquire("127.0.0.1", now);
        assertNotNull(second);
        first.close();
        assertNull(limiter.acquire("127.0.0.1", now));
        second.close();
        assertNotNull(limiter.acquire("127.0.0.1", now));
    }

    @Test
    void requestWindowBansAndThenExpiresWithoutReservingConnections() {
        ConnectionLimiter limiter = new ConnectionLimiter(new ProxySettings(true, 9, false, 1, 2, Duration.ofSeconds(10), Duration.ofSeconds(1)));
        long now = System.currentTimeMillis();
        assertTrue(limiter.request("ip", now));
        assertTrue(limiter.request("ip", now + 1));
        assertFalse(limiter.request("ip", now + 2));
        assertFalse(limiter.request("ip", now + 500));
        assertTrue(limiter.request("ip", now + 10_002));
        assertNotNull(limiter.acquire("ip", now + 10_003));
    }

    @Test
    void disabledAdmissionLimitsAllowIndependentLeases() {
        ConnectionLimiter limiter = new ConnectionLimiter(new ProxySettings(true, 9, false, -1, -1, Duration.ZERO, Duration.ZERO));
        for (int index = 0; index < 100; index++) {
            assertTrue(limiter.request("ip", index));
            assertNotNull(limiter.acquire("ip", index));
        }
    }

    @Test
    void existingLevelAndCommentsSurviveConfigurationUpgrade() throws Exception {
        Path file = temporary.resolve("zstdnet.properties");
        Files.writeString(file, "# User preference\nenabled=true\nlevel=6\ndebug=false\n");
        ProxySettings settings = ProxySettings.load(temporary);
        assertEquals(6, settings.level()); assertEquals(64, settings.maxConnectionsPerIp());
        assertEquals(Duration.ofSeconds(10), settings.requestWindow());
        String upgraded = Files.readString(file);
        assertTrue(upgraded.contains("# User preference"));
        assertFalse(upgraded.contains("level=9"));
        ProxySettings.load(temporary);
        assertEquals(upgraded, Files.readString(file));
    }

    @Test
    void highCompressionLevelAndDisabledWindowAreAccepted() throws Exception {
        Files.writeString(temporary.resolve("zstdnet.properties"), "level=22\nrequest_window=0\nban_duration=-1s\n");
        ProxySettings settings = ProxySettings.load(temporary);
        assertEquals(22, settings.level()); assertTrue(settings.requestWindow().isZero());
        assertTrue(settings.banDuration().isNegative());
    }

    @Test
    void reportUuidAuthorizationPersistsWithoutChangingOtherSettings() throws Exception {
        Files.writeString(temporary.resolve("zstdnet.properties"), "# Keep this\nlevel=6\n");
        ProxySettings settings = ProxySettings.load(temporary);
        UUID player = UUID.randomUUID();
        assertTrue(settings.reportAdminUuids().isEmpty());
        ProxySettings updated = settings.allowReport(temporary, player);
        assertTrue(updated.reportAdminUuids().contains(player));
        assertTrue(ProxySettings.load(temporary).reportAdminUuids().contains(player));
        assertTrue(Files.readString(temporary.resolve("zstdnet.properties")).contains("# Keep this\nlevel=6"));
        assertEquals(6, updated.level());
    }

    @Test
    void requestSchemaRequiresUuidRangeAndNoTrailingData() {
        UUID id = UUID.randomUUID();
        ByteBuf buffer = Unpooled.buffer().writeLong(id.getMostSignificantBits()).writeLong(id.getLeastSignificantBits());
        try {
            buffer.writeByte(7).writeCharSequence("session", StandardCharsets.UTF_8);
            byte[] payload = new byte[buffer.readableBytes()]; buffer.getBytes(0, payload);
            assertEquals(new AuxiliaryProtocol.Request(id, "session"), AuxiliaryProtocol.request(payload));
            byte[] trailing = java.util.Arrays.copyOf(payload, payload.length + 1);
            assertThrows(IllegalArgumentException.class, () -> AuxiliaryProtocol.request(trailing));
        } finally { buffer.release(); }
    }

    @Test
    void responseChunkMetadataAndBodyMatchClientProtocol() {
        UUID id = UUID.randomUUID();
        byte[] report = new byte[AuxiliaryProtocol.CHUNK_BYTES + 1];
        report[report.length - 1] = 42;
        ByteBuf second = AuxiliaryProtocol.reportChunk(id, true, 1, 2, report);
        try {
            assertEquals(id.getMostSignificantBits(), second.readLong());
            assertEquals(id.getLeastSignificantBits(), second.readLong()); assertTrue(second.readBoolean());
            assertEquals(1, varInt(second)); assertEquals(2, varInt(second));
            assertEquals(report.length, varInt(second)); assertEquals(1, varInt(second));
            assertEquals(42, second.readUnsignedByte()); assertFalse(second.isReadable());
        } finally { second.release(); }
        assertThrows(IllegalArgumentException.class, () -> AuxiliaryProtocol.reportChunk(id, true, 0, 1, report));
    }

    @Test
    void detachedConnectionTailIsCollectedExactlyOnce() {
        TrafficRuntime runtime = new TrafficRuntime(temporary, LoggerFactory.getLogger(getClass()), () -> 25565, () -> { });
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            TransportStats counters = new TransportStats();
            runtime.track(channel, counters, channel::isActive, ignored -> { });
            counters.addRxRaw(100); counters.addRxWire(20); runtime.collectAll();
            counters.addRxRaw(7); counters.addRxWire(3); counters.addTxRaw(41); counters.addTxWire(9);
            runtime.untrack(channel); runtime.untrack(channel); runtime.collectAll();
            var totals = runtime.snapshot();
            assertEquals(107, totals.rawIngressBytes()); assertEquals(23, totals.tcpIngressBytes());
            assertEquals(41, totals.rawEgressBytes()); assertEquals(9, totals.tcpEgressBytes());
            assertEquals(0, totals.activeConnections());
        } finally { channel.finishAndReleaseAll(); runtime.close(); }
    }

    private static int varInt(ByteBuf input) {
        int result = 0;
        for (int shift = 0; shift < 35; shift += 7) {
            int next = input.readUnsignedByte(); result |= (next & 0x7f) << shift;
            if ((next & 0x80) == 0) return result;
        }
        throw new AssertionError("Invalid test varint");
    }
}
