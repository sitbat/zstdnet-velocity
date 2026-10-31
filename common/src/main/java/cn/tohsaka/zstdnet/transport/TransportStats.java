package cn.tohsaka.zstdnet.transport;

import java.util.concurrent.atomic.LongAdder;

/** Per-front-connection counters; raw bytes include Minecraft length prefixes. */
public final class TransportStats {
    private final LongAdder rxRaw = new LongAdder();
    private final LongAdder rxWire = new LongAdder();
    private final LongAdder txRaw = new LongAdder();
    private final LongAdder txWire = new LongAdder();

    public long rxRaw() { return rxRaw.sum(); }
    public long rxWire() { return rxWire.sum(); }
    public long txRaw() { return txRaw.sum(); }
    public long txWire() { return txWire.sum(); }
    public void addRxRaw(long bytes) { rxRaw.add(bytes); }
    public void addRxWire(long bytes) { rxWire.add(bytes); }
    public void addTxRaw(long bytes) { txRaw.add(bytes); }
    public void addTxWire(long bytes) { txWire.add(bytes); }
}
