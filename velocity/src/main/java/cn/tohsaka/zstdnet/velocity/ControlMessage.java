package cn.tohsaka.zstdnet.velocity;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/** A small, strict versioned control protocol; never a player identity credential. */
record ControlMessage(int opcode, int version) {
    static final String CHANNEL = "zstdnet:proxy_transport";
    static final int VERSION = 2;
    static final int HELLO = 0;
    static final int PREPARE = 1;
    static final int READY = 2;
    static final int ACTIVATE = 3;

    static ControlMessage decode(byte[] data) {
        if (data == null || data.length < 2 || data.length > 10) {
            throw new IllegalArgumentException("invalid control length");
        }
        ByteBuf buffer = Unpooled.wrappedBuffer(data);
        try {
            int opcode = readVarInt(buffer);
            int version = readVarInt(buffer);
            if (buffer.isReadable() || opcode < HELLO || opcode > ACTIVATE || version < 0) {
                throw new IllegalArgumentException("invalid control payload");
            }
            return new ControlMessage(opcode, version);
        } finally {
            buffer.release();
        }
    }

    static ByteBuf encode(int opcode) {
        // Protocol version and all defined opcodes fit in one byte.
        return Unpooled.buffer(2, 2).writeByte(opcode).writeByte(VERSION);
    }

    private static int readVarInt(ByteBuf buffer) {
        int value = 0;
        for (int index = 0; index < 5; index++) {
            if (!buffer.isReadable()) {
                throw new IllegalArgumentException("truncated control varint");
            }
            int next = buffer.readUnsignedByte();
            if (index == 4 && (next & 0xf0) != 0) {
                throw new IllegalArgumentException("control varint overflow");
            }
            value |= (next & 0x7f) << (index * 7);
            if ((next & 0x80) == 0) {
                if (index > 0 && (next & 0x7f) == 0) {
                    throw new IllegalArgumentException("noncanonical control varint");
                }
                return value;
            }
        }
        throw new IllegalArgumentException("oversized control varint");
    }
}
