package cn.tohsaka.zstdnet.velocity;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

final class AuxiliaryProtocol {
    static final String HUD = "zstdnet:proxy_hud";
    static final String REPORT_REQUEST = "zstdnet:proxy_report_request";
    static final String REPORT_RESPONSE = "zstdnet:proxy_report_response";
    static final int MAX_REPORT_BYTES = 4 * 1024 * 1024;
    static final int CHUNK_BYTES = 24576;
    static final int MAX_CHUNKS = 256;
    private AuxiliaryProtocol() { }

    static boolean reserved(String channel) {
        return ControlMessage.CHANNEL.equals(channel) || HUD.equals(channel)
                || REPORT_REQUEST.equals(channel) || REPORT_RESPONSE.equals(channel);
    }

    static Request request(byte[] bytes) {
        if (bytes == null || bytes.length < 18 || bytes.length > 65) throw new IllegalArgumentException("Invalid report request length");
        ByteBuf input = Unpooled.wrappedBuffer(bytes);
        try {
            UUID id = new UUID(input.readLong(), input.readLong());
            int length = readVarInt(input);
            if (length < 1 || length > 48 || length > input.readableBytes()) throw new IllegalArgumentException("Invalid report range");
            String range = input.readCharSequence(length, StandardCharsets.UTF_8).toString();
            if (range.length() > 16 || input.isReadable()) throw new IllegalArgumentException("Invalid report range");
            return new Request(id, range);
        } finally { input.release(); }
    }

    static byte[] utfPayload(String text, int maxCharacters) {
        ByteBuf output = Unpooled.buffer();
        try {
            if (text.length() > maxCharacters) throw new IllegalArgumentException("UTF payload too large");
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            if (bytes.length > maxCharacters * 3) throw new IllegalArgumentException("UTF payload too large");
            writeVarInt(output, bytes.length); output.writeBytes(bytes);
            byte[] result = new byte[output.readableBytes()]; output.readBytes(result); return result;
        } finally { output.release(); }
    }

    static ByteBuf reportChunk(UUID id, boolean success, int index, int chunks, byte[] report) {
        if (report.length > MAX_REPORT_BYTES || chunks < 1 || chunks > MAX_CHUNKS || index < 0 || index >= chunks
                || chunks != Math.max(1, (report.length + CHUNK_BYTES - 1) / CHUNK_BYTES)) throw new IllegalArgumentException("Invalid report chunk metadata");
        int offset = index * CHUNK_BYTES;
        int length = Math.min(CHUNK_BYTES, report.length - offset);
        ByteBuf output = Unpooled.buffer(length + 40);
        output.writeLong(id.getMostSignificantBits()).writeLong(id.getLeastSignificantBits()).writeBoolean(success);
        writeVarInt(output, index); writeVarInt(output, chunks); writeVarInt(output, report.length);
        writeVarInt(output, length); output.writeBytes(report, offset, length);
        return output;
    }

    private static int readVarInt(ByteBuf input) {
        int result = 0;
        for (int index = 0; index < 5 && input.isReadable(); index++) {
            int next = input.readUnsignedByte();
            if (index == 4 && (next & 0xf0) != 0) throw new IllegalArgumentException("Report varint overflow");
            result |= (next & 0x7f) << (index * 7);
            if ((next & 0x80) == 0) return result;
        }
        throw new IllegalArgumentException("Truncated report varint");
    }

    private static void writeVarInt(ByteBuf output, int value) {
        do { int next = value & 0x7f; value >>>= 7; output.writeByte(value == 0 ? next : next | 0x80); } while (value != 0);
    }
    record Request(UUID id, String range) { }
}
