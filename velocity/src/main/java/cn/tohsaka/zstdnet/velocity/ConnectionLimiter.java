/* Inspired by wish131400/zstdnet ConnectionFloodGuard, Copyright (c) [2026] [wish], MIT. */
package cn.tohsaka.zstdnet.velocity;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Request admission and authenticated connection leases are deliberately separate. */
final class ConnectionLimiter {
    private final ProxySettings settings;
    private final Map<String, Entry> entries = new HashMap<>();
    ConnectionLimiter(ProxySettings settings) { this.settings = settings; }

    synchronized boolean request(String ip, long now) {
        if (settings.maxRequestsPerWindow() <= 0 || settings.requestWindow().isNegative()
                || settings.requestWindow().isZero()) return true;
        Entry entry = entries.computeIfAbsent(ip, ignored -> new Entry());
        prune(entry, now);
        if (entry.bannedUntil > now) return false;
        if (entry.requests.size() >= settings.maxRequestsPerWindow()) {
            long ban = Math.max(0, settings.banDuration().toMillis());
            entry.bannedUntil = ban > Long.MAX_VALUE - now ? Long.MAX_VALUE : now + ban;
            return false;
        }
        entry.requests.addLast(now);
        return true;
    }

    synchronized Lease acquire(String ip, long now) {
        Entry entry = entries.computeIfAbsent(ip, ignored -> new Entry());
        prune(entry, now);
        if (entry.bannedUntil > now || (settings.maxConnectionsPerIp() > 0
                && entry.connections >= settings.maxConnectionsPerIp())) return null;
        entry.connections++;
        return new Lease(this, ip);
    }

    private synchronized void release(String ip) {
        Entry entry = entries.get(ip);
        if (entry == null) return;
        entry.connections = Math.max(0, entry.connections - 1);
        prune(entry, System.currentTimeMillis());
        if (entry.connections == 0 && entry.requests.isEmpty() && entry.bannedUntil <= System.currentTimeMillis()) entries.remove(ip);
    }

    synchronized void sweep(long now) {
        entries.entrySet().removeIf(item -> {
            Entry entry = item.getValue(); prune(entry, now);
            return entry.connections == 0 && entry.requests.isEmpty() && entry.bannedUntil <= now;
        });
    }

    private void prune(Entry entry, long now) {
        long window = Math.max(0, settings.requestWindow().toMillis());
        while (!entry.requests.isEmpty() && now - entry.requests.peekFirst() >= window) entry.requests.removeFirst();
    }

    static final class Lease implements AutoCloseable {
        private final ConnectionLimiter owner;
        private final String ip;
        private final AtomicBoolean released = new AtomicBoolean();
        private Lease(ConnectionLimiter owner, String ip) { this.owner = owner; this.ip = ip; }
        @Override public void close() { if (released.compareAndSet(false, true)) owner.release(ip); }
    }

    private static final class Entry {
        private int connections;
        private long bannedUntil;
        private final ArrayDeque<Long> requests = new ArrayDeque<>();
    }
}
