package cn.tohsaka.zstdnet.velocity;

import cn.tohsaka.zstdnet.transport.TransportConnection;
import cn.tohsaka.zstdnet.transport.TransportStats;
import cn.tohsaka.zstdnet.velocity.stats.TrafficReportRange;
import com.google.inject.Inject;
import com.velocitypowered.api.command.SimpleCommand;
import com.velocitypowered.api.event.PostOrder;
import com.velocitypowered.api.event.EventTask;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.DisconnectEvent;
import com.velocitypowered.api.event.connection.LoginEvent;
import com.velocitypowered.api.event.connection.PluginMessageEvent;
import com.velocitypowered.api.event.connection.PostLoginEvent;
import com.velocitypowered.api.event.connection.PreLoginEvent;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.event.player.ServerPreConnectEvent;
import com.velocitypowered.api.network.ProtocolState;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.proxy.messages.MinecraftChannelIdentifier;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import net.kyori.adventure.text.Component;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.charset.StandardCharsets;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

/** Changes only authenticated front connections; backend authentication/forwarding is untouched. */
public final class ZstdNetVelocityPlugin {
    private static final String INTERCEPTOR = "zstdnet-proxy-control";
    private static final MinecraftChannelIdentifier IDENTIFIER = MinecraftChannelIdentifier.from(ControlMessage.CHANNEL);
    private static final MinecraftChannelIdentifier[] RESERVED = {
            IDENTIFIER, MinecraftChannelIdentifier.from(AuxiliaryProtocol.HUD),
            MinecraftChannelIdentifier.from(AuxiliaryProtocol.REPORT_REQUEST),
            MinecraftChannelIdentifier.from(AuxiliaryProtocol.REPORT_RESPONSE)};
    private static final String INITIAL_GATE = "zstdnet-initial-sync-gate";
    private static final AttributeKey<Session> SESSION = AttributeKey.valueOf("zstdnet-proxy-session");
    private static final TransportConnection.Layout LAYOUT = new TransportConnection.Layout(
            "frame-decoder", "minecraft-encoder", "compression-decoder", "compression-encoder",
            "frame-encoder", 2 * 1024 * 1024);
    private static final int MAX_CONTROL_MESSAGES = 8;
    private static final long HANDSHAKE_TIMEOUT_SECONDS = 10;
    private static final Component REQUIRED_CLIENT_MESSAGE = Component.text(
            "此服务器要求安装兼容的 ZstdNet 客户端模组，并成功启用 Zstd 压缩。\n"
            + "请安装或更新对应游戏版本的模组后重新连接：\nhttps://github.com/sitbat/zstdnet-client/releases");

    private final ProxyServer proxy;
    private final Logger logger;
    private final Path directory;
    private final Map<Channel, Session> sessions = new ConcurrentHashMap<>();
    private final Map<Player, ConnectionLimiter.Lease> leases = new ConcurrentHashMap<>();
    private final ThreadPoolExecutor reportWorker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(32), task -> {
                Thread thread = new Thread(task, "zstdnet-proxy-report"); thread.setDaemon(true); return thread;
            });
    private volatile ProxySettings settings = ProxySettings.defaults(false);
    private volatile ConnectionLimiter limiter;
    private volatile TrafficRuntime traffic;
    private volatile VelocityAccess access;
    private volatile boolean enabled;

    @Inject
    public ZstdNetVelocityPlugin(ProxyServer proxy, Logger logger, @DataDirectory Path directory) {
        this.proxy = proxy;
        this.logger = logger;
        this.directory = directory;
    }

    @Subscribe
    public void initialize(ProxyInitializeEvent event) {
        // Register even when disabled: reserved transport messages must never reach a backend.
        proxy.getChannelRegistrar().register(RESERVED);
        proxy.getCommandManager().register(proxy.getCommandManager().metaBuilder("zstdnetproxy")
                .plugin(this).build(), new StatusCommand());
        try {
            settings = ProxySettings.startupPolicy(directory);
            settings = ProxySettings.load(directory);
            limiter = new ConnectionLimiter(settings);
            access = new VelocityAccess();
            if (!proxy.getVersion().getVersion().startsWith("4.3.0")) {
                logger.warn("ZstdNet transport disabled: this build is verified for Velocity 4.3.0 build 44; running {}.",
                        proxy.getVersion().getVersion());
            } else if (!proxy.getConfiguration().isOnlineMode()) {
                logger.error("ZstdNet transport disabled: Velocity online-mode=true is required.");
            } else if (!access.modern(proxy.getConfiguration())) {
                logger.error("ZstdNet transport disabled: player-info-forwarding-mode=modern is required.");
            } else {
                enabled = settings.enabled();
            }
            if (enabled) {
                traffic = new TrafficRuntime(directory, logger, this::listenPort,
                        () -> limiter.sweep(System.currentTimeMillis()));
                traffic.start();
            }
            logger.info("ZstdNet 2.1.2 loaded: transport={}, level={}, require_client={}, target=Velocity 4.3.0 build 44; native account verification retained.",
                    enabled, settings.level(), settings.requireClient());
        } catch (IOException | ReflectiveOperationException | RuntimeException error) {
            enabled = false;
            logger.error(settings.requireClient()
                    ? "ZstdNet transport disabled; require_client=true rejects new connections."
                    : "ZstdNet transport disabled; vanilla networking remains available.", error);
        }
    }

    @Subscribe(order = PostOrder.FIRST)
    public void preLogin(PreLoginEvent event) {
        if (settings.requireClient() && !enabled) {
            event.setResult(PreLoginEvent.PreLoginComponentResult.denied(Component.text(
                    "服务器要求 ZstdNet，但代理压缩功能当前不可用，请联系管理员。")));
            return;
        }
        ConnectionLimiter current = limiter;
        if (enabled && current != null && !current.request(ip(event.getConnection().getRemoteAddress()), System.currentTimeMillis())) {
            event.setResult(PreLoginEvent.PreLoginComponentResult.denied(Component.text("Too many connection requests. Please try again later.")));
        }
    }

    @Subscribe(order = PostOrder.FIRST)
    public void postLogin(PostLoginEvent event) {
        if (!enabled) {
            if (settings.requireClient()) event.getPlayer().disconnect(REQUIRED_CLIENT_MESSAGE);
            return;
        }
        Player player = event.getPlayer();
        ConnectionLimiter.Lease lease = limiter.acquire(ip(player.getRemoteAddress()), System.currentTimeMillis());
        if (lease == null) {
            player.disconnect(Component.text("Too many connections from your address. Please try again later."));
            return;
        }
        if (!enabled) { lease.close(); return; }
        ConnectionLimiter.Lease existing = leases.putIfAbsent(player, lease);
        if (existing != null) lease.close();
        attach(player, null);
    }

    @Subscribe(order = PostOrder.LAST)
    public void login(LoginEvent event) {
        // Queue installation before AuthSessionHandler queues LoginSuccess/CONFIG on this event loop.
        // A CONFIG HELLO arriving before PostLogin would otherwise be dropped without a backend.
        if (enabled && event.getResult().isAllowed()) attach(event.getPlayer(), null);
    }

    @Subscribe(order = PostOrder.LAST)
    public EventTask preConnect(ServerPreConnectEvent event) {
        Player player = event.getPlayer();
        // Existing online players are not evicted by a reload, including on backend switches.
        if (!settings.requireClient() || event.getPreviousServer() != null || player.getCurrentServer().isPresent()
                || event.getResult().getServer().isEmpty()) return null;
        if (!enabled || access == null) {
            event.setResult(ServerPreConnectEvent.ServerResult.denied());
            player.disconnect(REQUIRED_CLIENT_MESSAGE);
            return null;
        }
        CompletableFuture<Void> checked = new CompletableFuture<>();
        try {
            Channel channel = access.channel(player);
            attach(player, null);
            channel.eventLoop().execute(() -> {
                Session session = channel.attr(SESSION).get();
                if (session == null || !channel.isActive()) {
                    event.setResult(ServerPreConnectEvent.ServerResult.denied());
                    if (channel.isActive()) player.disconnect(REQUIRED_CLIENT_MESSAGE);
                    checked.complete(null);
                    return;
                }
                session.awaitBackendAdmission(event, checked);
            });
        } catch (ReflectiveOperationException | RuntimeException error) {
            event.setResult(ServerPreConnectEvent.ServerResult.denied());
            player.disconnect(REQUIRED_CLIENT_MESSAGE);
            checked.complete(null);
        }
        return EventTask.resumeWhenComplete(checked);
    }

    private void reloadSettings() throws IOException {
        ProxySettings updated = ProxySettings.load(directory);
        if (updated.enabled() != settings.enabled()) {
            throw new IOException("Changing enabled requires a Velocity restart; no settings were applied.");
        }
        // Preserve live connection leases, bans and request history while changing their limits.
        ConnectionLimiter current = limiter;
        if (current != null) current.reconfigure(updated);
        settings = updated;
        for (Session session : sessions.values()) {
            try { session.channel.eventLoop().execute(() -> session.configurationReloaded(updated)); }
            catch (RejectedExecutionException ignored) { /* The connection's event loop has already stopped. */ }
        }
    }

    @Subscribe(order = PostOrder.FIRST)
    public void pluginMessage(PluginMessageEvent event) {
        String channel = event.getIdentifier().getId();
        if (!AuxiliaryProtocol.reserved(channel)) return;
        event.setResult(PluginMessageEvent.ForwardResult.handled());
        // A backend never controls the compression of the player's front connection.
        if (!(event.getSource() instanceof Player player) || !enabled) return;
        if (AuxiliaryProtocol.REPORT_REQUEST.equals(channel)) {
            requestReport(player, event.getData());
            return;
        }
        if (!ControlMessage.CHANNEL.equals(channel)) return;
        // Normally the Netty interceptor consumes these synchronously before any API event.
        // This is a guarded fallback for a HELLO that races listener installation.
        byte[] data = event.getData();
        attach(player, data);
    }

    @Subscribe
    public void disconnected(DisconnectEvent event) {
        releaseLease(event.getPlayer());
        VelocityAccess current = access;
        if (current == null) return;
        try {
            Channel channel = current.channel(event.getPlayer());
            Session session = sessions.get(channel);
            if (session != null) channel.eventLoop().execute(() -> {
                session.cancelTimeout();
                if (!channel.isActive()) session.finish();
            });
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            // channelInactive owns normal cleanup, including if Velocity has torn down the player.
        }
    }

    @Subscribe
    public void shutdown(ProxyShutdownEvent event) {
        enabled = false;
        proxy.getChannelRegistrar().unregister(RESERVED);
        proxy.getCommandManager().unregister("zstdnetproxy");
        reportWorker.shutdown();
        ArrayList<CompletableFuture<Void>> closed = new ArrayList<>();
        for (Session session : sessions.values()) {
            CompletableFuture<Void> complete = new CompletableFuture<>();
            closed.add(complete);
            session.channel.eventLoop().execute(() -> {
                session.cancelTimeout();
                // Removing an active codec would corrupt an existing stream. Shutdown closes it.
                session.channel.close().addListener(result -> { session.finish(); complete.complete(null); });
            });
        }
        try { CompletableFuture.allOf(closed.toArray(CompletableFuture[]::new)).get(5, TimeUnit.SECONDS); }
        catch (Exception error) { logger.warn("ZstdNet shutdown channel drain did not finish in time.", error); }
        try { if (!reportWorker.awaitTermination(3, TimeUnit.SECONDS)) reportWorker.shutdownNow(); }
        catch (InterruptedException error) { Thread.currentThread().interrupt(); reportWorker.shutdownNow(); }
        TrafficRuntime history = traffic;
        if (history != null) history.close();
        for (ConnectionLimiter.Lease lease : leases.values()) lease.close();
        leases.clear();
        sessions.clear();
    }

    private void attach(Player player, byte[] firstMessage) {
        VelocityAccess current = access;
        if (!enabled || current == null) return;
        try {
            Channel channel = current.channel(player);
            channel.eventLoop().execute(() -> {
                try {
                    if (!enabled || !channel.isActive()) return;
                    Session session = channel.attr(SESSION).get();
                    if (session == null) {
                        if (!supportedPipeline(channel)) {
                            if (settings.requireClient()) { player.disconnect(REQUIRED_CLIENT_MESSAGE); return; }
                            logger.warn("ZstdNet skipped unsupported pipeline for {}; using vanilla networking.", player.getUsername());
                            return;
                        }
                        session = new Session(player, channel, current);
                        channel.attr(SESSION).set(session);
                        sessions.put(channel, session);
                        Session attached = session;
                        TrafficRuntime runtime = traffic;
                        if (runtime != null) runtime.track(channel, session.statistics,
                                () -> attached.stage == Stage.ACTIVE && channel.isActive(),
                                payload -> attached.sendAuxiliary(AuxiliaryProtocol.HUD, payload));
                        channel.pipeline().addBefore("minecraft-encoder", INITIAL_GATE, session.gate);
                        channel.pipeline().addBefore("handler", INTERCEPTOR, new ControlInterceptor(session, current));
                    }
                    if (firstMessage != null) session.receive(firstMessage, false);
                } catch (RuntimeException error) {
                    logger.warn("ZstdNet could not attach the transport handlers.", error);
                    if (settings.requireClient()) {
                        Session failed = channel.attr(SESSION).get();
                        if (failed != null) failed.rejectRequiredClient();
                        else player.disconnect(REQUIRED_CLIENT_MESSAGE);
                    }
                }
            });
        } catch (ReflectiveOperationException | RuntimeException error) {
            if (settings.requireClient()) player.disconnect(REQUIRED_CLIENT_MESSAGE);
            if (settings.debug()) logger.warn("ZstdNet cannot inspect player connection; using vanilla networking.", error);
        }
    }

    private int listenPort() {
        InetSocketAddress address = proxy.getBoundAddress();
        return address == null ? 0 : address.getPort();
    }

    private static String ip(InetSocketAddress address) {
        return address.getAddress() == null ? address.getHostString() : address.getAddress().getHostAddress();
    }

    private void releaseLease(Player player) {
        ConnectionLimiter.Lease lease = leases.remove(player);
        if (lease != null) lease.close();
    }

    private void requestReport(Player player, byte[] payload) {
        VelocityAccess current = access;
        if (current == null) return;
        AuxiliaryProtocol.Request request;
        Session session;
        try {
            request = AuxiliaryProtocol.request(payload);
            session = sessions.get(current.channel(player));
        } catch (ReflectiveOperationException | IllegalArgumentException error) {
            return;
        }
        if (session == null || session.stage != Stage.ACTIVE || !session.reportBusy.compareAndSet(false, true)) return;
        long now = System.nanoTime();
        long previous = session.lastReportRequest.getAndSet(now);
        if (previous != 0 && now - previous < TimeUnit.SECONDS.toNanos(15)) {
            session.sendReport(request.id(), false, "Please wait 15 seconds between report requests.".getBytes(StandardCharsets.UTF_8));
            return;
        }
        if (!canReport(player)) {
            session.sendReport(request.id(), false, "Missing permission: zstdnet.report".getBytes(StandardCharsets.UTF_8));
            return;
        }
        TrafficReportRange range;
        try { range = TrafficReportRange.parse(request.range()); }
        catch (IllegalArgumentException error) {
            session.sendReport(request.id(), false, "Unsupported report range.".getBytes(StandardCharsets.UTF_8));
            return;
        }
        try {
            reportWorker.execute(() -> {
                try {
                    TrafficRuntime history = traffic;
                    if (history == null) throw new IllegalStateException("Traffic history is unavailable.");
                    byte[] report = history.report(range.id()).getBytes(StandardCharsets.UTF_8);
                    if (report.length == 0 || report.length > AuxiliaryProtocol.MAX_REPORT_BYTES) {
                        throw new IllegalStateException("The report exceeds the 4 MiB transfer limit.");
                    }
                    session.sendReport(request.id(), true, report);
                } catch (RuntimeException error) {
                    logger.warn("ZstdNet report generation failed for {}.", player.getUsername(), error);
                    session.sendReport(request.id(), false, "Could not generate the report. Check the proxy log.".getBytes(StandardCharsets.UTF_8));
                }
            });
        } catch (RejectedExecutionException error) {
            session.sendReport(request.id(), false, "Report service is busy. Please try again later.".getBytes(StandardCharsets.UTF_8));
        }
    }

    private boolean canReport(Player player) {
        return player.hasPermission("zstdnet.report") || player.hasPermission("zstdnet.admin")
                || (player.isOnlineMode() && settings.reportAdminUuids().contains(player.getUniqueId()));
    }

    private void allowReport(SimpleCommand.Invocation invocation, String name) {
        Player target = proxy.getPlayer(name).orElse(null);
        if (target == null || !target.isOnlineMode() || !proxy.getConfiguration().isOnlineMode()) {
            invocation.source().sendMessage(Component.text("The target must be online and authenticated by Velocity."));
            return;
        }
        UUID authenticatedId = target.getUniqueId();
        String authenticatedName = target.getUsername();
        try {
            reportWorker.execute(() -> {
                try {
                    settings = settings.allowReport(directory, authenticatedId);
                    invocation.source().sendMessage(Component.text("Report access granted to " + authenticatedName + " (" + authenticatedId + ")."));
                } catch (IOException | RuntimeException error) {
                    logger.warn("ZstdNet report authorization could not be saved.", error);
                    invocation.source().sendMessage(Component.text("Could not save report authorization; check the proxy log."));
                }
            });
        } catch (RejectedExecutionException error) {
            invocation.source().sendMessage(Component.text("Report service is busy; try again later."));
        }
    }

    private void exportReport(SimpleCommand.Invocation invocation, String range) {
        TrafficReportRange selected;
        try { selected = TrafficReportRange.parse(range); }
        catch (IllegalArgumentException error) {
            invocation.source().sendMessage(Component.text("Ranges: today, session, 24h, 7d, 30d"));
            return;
        }
        try {
            reportWorker.execute(() -> {
                Path temporary = null;
                try {
                    TrafficRuntime history = traffic;
                    if (history == null) throw new IllegalStateException("Traffic history is unavailable.");
                    String json = history.report(selected.id());
                    Path reports = directory.resolve("reports");
                    Files.createDirectories(reports);
                    String name = "traffic-" + selected.id() + "-" + System.currentTimeMillis() + "-"
                            + UUID.randomUUID().toString().substring(0, 8) + ".json";
                    Path output = reports.resolve(name);
                    temporary = reports.resolve(name + ".tmp");
                    Files.writeString(temporary, json, StandardCharsets.UTF_8);
                    try { Files.move(temporary, output, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
                    catch (AtomicMoveNotSupportedException error) { Files.move(temporary, output, StandardCopyOption.REPLACE_EXISTING); }
                    invocation.source().sendMessage(Component.text("Traffic report saved: " + output.toAbsolutePath()));
                } catch (IOException | RuntimeException error) {
                    logger.warn("ZstdNet traffic report export failed.", error);
                    invocation.source().sendMessage(Component.text("Could not export the report; check the proxy log."));
                } finally {
                    if (temporary != null) {
                        try { Files.deleteIfExists(temporary); } catch (IOException ignored) { }
                    }
                }
            });
            invocation.source().sendMessage(Component.text("Generating traffic report for " + selected.id() + "..."));
        } catch (RejectedExecutionException error) {
            invocation.source().sendMessage(Component.text("Report service is busy; try again later."));
        }
    }

    private static boolean supportedPipeline(Channel channel) {
        return channel.pipeline().get("frame-decoder") instanceof ByteToMessageDecoder
                && channel.pipeline().get("minecraft-encoder") != null
                && channel.pipeline().get("minecraft-decoder") != null
                && channel.pipeline().get("handler") != null
                && (channel.pipeline().get("compression-encoder") != null
                    || channel.pipeline().get("frame-encoder") != null);
    }

    private final class ControlInterceptor extends ChannelInboundHandlerAdapter {
        private final Session session;
        private final VelocityAccess adapter;

        private ControlInterceptor(Session session, VelocityAccess adapter) {
            this.session = session;
            this.adapter = adapter;
        }

        @Override
        public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
            if (!adapter.controlPacket(message)) {
                context.fireChannelRead(message);
                return;
            }
            try {
                session.receive(adapter.payload(message), true);
            } catch (RuntimeException error) {
                session.fail("invalid transport control message", error);
            } finally {
                ReferenceCountUtil.release(message);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) throws Exception {
            session.finish();
            context.fireChannelInactive();
        }
    }

    private enum Stage { VANILLA, WAIT_READY, PREPARING, WAIT_PLAY, ACTIVE, REJECTED }

    private final class Session {
        private final Player player;
        private final Channel channel;
        private final VelocityAccess adapter;
        private final TransportStats statistics = new TransportStats();
        private volatile Stage stage = Stage.VANILLA;
        private int messages;
        private boolean hello;
        private boolean requiredClient = settings.requireClient();
        private boolean admissionStarted;
        private final CompletableFuture<Boolean> backendAdmission = new CompletableFuture<>();
        private ScheduledFuture<?> admissionTimeout;
        private ScheduledFuture<?> timeout;
        private ScheduledFuture<?> retry;
        private IntConsumer inboundAccount;
        private final InitialSyncGate gate;
        private final AtomicBoolean finished = new AtomicBoolean();
        private final AtomicBoolean reportBusy = new AtomicBoolean();
        private final AtomicLong lastReportRequest = new AtomicLong();

        private Session(Player player, Channel channel, VelocityAccess adapter) {
            this.player = player;
            this.channel = channel;
            this.adapter = adapter;
            gate = new InitialSyncGate("minecraft-encoder",
                    () -> hello && stage != Stage.ACTIVE && stage != Stage.REJECTED && playing(),
                    this::tryPrepare, this::fallback);
        }

        private boolean authorized() {
            return enabled && proxy.getConfiguration().isOnlineMode() && player.isOnlineMode()
                    && channel.pipeline().get("cipher-decoder") != null
                    && channel.pipeline().get("cipher-encoder") != null;
        }

        private boolean playing() {
            try {
                return channel.isActive() && player.getProtocolState() == ProtocolState.PLAY
                        && !VelocityAccess.hasPendingPlayPackets(channel);
            } catch (RuntimeException error) {
                return false;
            }
        }

        private void awaitBackendAdmission(ServerPreConnectEvent event, CompletableFuture<Void> checked) {
            if (!channel.isActive() || finished.get() || stage == Stage.REJECTED || !authorized()) {
                event.setResult(ServerPreConnectEvent.ServerResult.denied());
                if (channel.isActive()) rejectRequiredClient();
                checked.complete(null);
                return;
            }
            backendAdmission.thenAccept(allowed -> {
                if (!allowed) event.setResult(ServerPreConnectEvent.ServerResult.denied());
                checked.complete(null);
            });
            admissionStarted = true;
            requiredClient = settings.requireClient();
            if (!requiredClient) { resolveAdmission(true); return; }
            if (stage == Stage.REJECTED || !authorized()) { rejectRequiredClient(); return; }
            if (hello || stage == Stage.ACTIVE) { resolveAdmission(true); return; }
            if (admissionTimeout == null) {
                admissionTimeout = channel.eventLoop().schedule(this::rejectRequiredClient,
                        HANDSHAKE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        }

        private void resolveAdmission(boolean allowed) {
            if (admissionTimeout != null) { admissionTimeout.cancel(false); admissionTimeout = null; }
            backendAdmission.complete(allowed);
        }

        private void configurationReloaded(ProxySettings updated) {
            // Disabling the requirement releases pending initial connects without restarting their clocks.
            if (!updated.requireClient() && stage != Stage.ACTIVE) requiredClient = false;
            if (!admissionStarted && !backendAdmission.isDone()) requiredClient = updated.requireClient();
            if (!updated.requireClient() && admissionStarted && !backendAdmission.isDone()) {
                requiredClient = false;
                resolveAdmission(true);
            }
        }

        private void rejectBeforeNegotiation() {
            if (requiredClient) { rejectRequiredClient(); return; }
            stage = Stage.REJECTED;
            cancelTimeout();
        }

        private void rejectRequiredClient() {
            stage = Stage.REJECTED;
            cancelTimeout();
            // Failing queued backend promises first makes Velocity send its generic internal error.
            // Settle them only once the guided disconnect has flushed and the channel is closed.
            gate.discardOnClose();
            player.disconnect(REQUIRED_CLIENT_MESSAGE);
            resolveAdmission(false);
        }

        private void receive(byte[] bytes, boolean synchronous) {
            if (++messages > MAX_CONTROL_MESSAGES) {
                fail("too many transport control messages", null);
                return;
            }
            if (stage == Stage.REJECTED) return;
            ControlMessage message;
            try {
                message = ControlMessage.decode(bytes);
            } catch (IllegalArgumentException error) {
                if (stage == Stage.VANILLA) { rejectBeforeNegotiation(); return; }
                fail("invalid transport control message", error);
                return;
            }
            if (message.version() != ControlMessage.VERSION) {
                if (stage == Stage.VANILLA) { rejectBeforeNegotiation(); return; }
                fail("transport protocol version changed", null);
                return;
            }
            if (stage == Stage.ACTIVE) return;
            if (!authorized()) {
                rejectBeforeNegotiation();
                return;
            }
            if (message.opcode() == ControlMessage.HELLO && stage == Stage.VANILLA) {
                hello = true;
                resolveAdmission(true);
                tryPrepare();
            } else if (message.opcode() == ControlMessage.READY && stage == Stage.WAIT_READY && synchronous) {
                stage = Stage.WAIT_PLAY;
                tryActivate();
            } else if (message.opcode() != ControlMessage.HELLO) {
                // READY must never cross an asynchronous plugin-event boundary.
                fail("unexpected transport control sequence", null);
            }
        }

        private void tryPrepare() {
            if (!hello || stage != Stage.VANILLA || !channel.isActive()) return;
            if (!playing()) { retry(this::tryPrepare); return; }
            if (!authorized()) { rejectBeforeNegotiation(); return; }
            ensureTimeout();
            try {
                inboundAccount = adapter.inboundAccount(channel);
            } catch (ReflectiveOperationException | RuntimeException error) {
                rejectBeforeNegotiation();
                logger.warn("ZstdNet unsupported native packet limiter for {}; require_client={}.",
                        player.getUsername(), settings.requireClient());
                return;
            }
            stage = Stage.WAIT_READY;
            writeControl(ControlMessage.PREPARE);
        }

        private void tryActivate() {
            if (stage != Stage.WAIT_PLAY || !channel.isActive()) return;
            if (!playing()) { retry(this::tryActivate); return; }
            if (!authorized()) { fail("authentication state changed during transport negotiation", null); return; }
            stage = Stage.PREPARING;
            TransportConnection.prepare(channel, LAYOUT, statistics, inboundAccount, () -> {
                if (stage == Stage.REJECTED || finished.get() || !channel.isActive()) return;
                if (!playing()) { stage = Stage.WAIT_PLAY; retry(this::tryActivate); return; }
                if (!authorized()) { fail("authentication state changed during transport negotiation", null); return; }
                try {
                    // writeAndFlush runs the existing vanilla packet/zlib/cipher encoders now.
                    // Waiting for the socket future would let later packets overtake this boundary.
                    Object activation = adapter.packet(ControlMessage.ACTIVATE);
                    ChannelFuture[] activationWrite = new ChannelFuture[1];
                    gate.allow(() -> activationWrite[0] = channel.writeAndFlush(activation));
                    ChannelFuture finalVanillaWrite = activationWrite[0];
                    if (finalVanillaWrite.isDone() && !finalVanillaWrite.isSuccess()) {
                        throw new IllegalStateException("activation write failed", finalVanillaWrite.cause());
                    }
                    TransportConnection.enableOutbound(channel, settings.level(), LAYOUT, statistics);
                    stage = Stage.ACTIVE;
                    cancelTimeout();
                    gate.release(true);
                    finalVanillaWrite.addListener(result -> {
                        if (!result.isSuccess()) fail("activation write failed", result.cause());
                    });
                    if (settings.debug()) logger.info("ZstdNet transport active for {}.", player.getUsername());
                    TrafficRuntime runtime = traffic;
                    if (runtime != null) runtime.sendImmediate(channel);
                } catch (ReflectiveOperationException | RuntimeException error) {
                    fail("could not activate transport", error);
                }
            });
        }

        private void writeControl(int opcode) {
            try {
                Object packet = adapter.packet(opcode);
                gate.allow(() -> channel.writeAndFlush(packet).addListener(result -> {
                    if (!result.isSuccess()) fail("transport control write failed", result.cause());
                }));
            } catch (ReflectiveOperationException | RuntimeException error) {
                fail("transport control write failed", error);
            }
        }

        private void ensureTimeout() {
            if (timeout == null) {
                timeout = channel.eventLoop().schedule(() -> {
                    if (stage != Stage.ACTIVE && stage != Stage.REJECTED) fallback();
                }, HANDSHAKE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        }

        private void retry(Runnable task) {
            if (retry == null || retry.isDone()) {
                retry = channel.eventLoop().schedule(() -> { retry = null; task.run(); }, 50, TimeUnit.MILLISECONDS);
            }
        }

        private void cancelTimeout() {
            if (timeout != null) { timeout.cancel(false); timeout = null; }
            if (retry != null) { retry.cancel(false); retry = null; }
        }

        private void fail(String reason, Throwable error) {
            if (requiredClient && stage != Stage.PREPARING && stage != Stage.ACTIVE) {
                if (settings.debug()) logger.warn("ZstdNet {}: {}", player.getUsername(), reason, error);
                rejectRequiredClient();
                return;
            }
            stage = Stage.REJECTED;
            cancelTimeout();
            if (settings.debug()) logger.warn("ZstdNet {}: {}", player.getUsername(), reason, error);
            // No stream rollback: closing is the only safe response after a negotiated failure.
            channel.close();
            resolveAdmission(false);
        }

        private void fallback() {
            if (stage == Stage.ACTIVE) return;
            if (requiredClient) { rejectRequiredClient(); return; }
            stage = Stage.REJECTED;
            cancelTimeout();
            resolveAdmission(false);
            gate.release(false);
            if (settings.debug()) logger.info("ZstdNet negotiation fell back to vanilla for {}.", player.getUsername());
        }

        private void finish() {
            if (!finished.compareAndSet(false, true)) return;
            cancelTimeout();
            resolveAdmission(false);
            TrafficRuntime runtime = traffic;
            if (runtime != null) runtime.untrack(channel);
            sessions.remove(channel, this);
            releaseLease(player);
            reportBusy.set(false);
        }

        private void sendAuxiliary(String channelName, byte[] payload) {
            channel.eventLoop().execute(() -> {
                if (!enabled || stage != Stage.ACTIVE || !playing() || !channel.isActive()) return;
                try {
                    channel.writeAndFlush(adapter.packet(channelName, Unpooled.wrappedBuffer(payload)))
                            .addListener(result -> { if (!result.isSuccess()) channel.close(); });
                } catch (ReflectiveOperationException | RuntimeException error) {
                    fail("could not send proxy HUD", error);
                }
            });
        }

        private void sendReport(UUID id, boolean success, byte[] bytes) {
            int chunks = Math.max(1, (bytes.length + AuxiliaryProtocol.CHUNK_BYTES - 1) / AuxiliaryProtocol.CHUNK_BYTES);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
            channel.eventLoop().execute(() -> sendReportChunk(id, success, bytes, chunks, 0, deadline));
        }

        private void sendReportChunk(UUID id, boolean success, byte[] bytes, int chunks, int index, long deadline) {
            if (!enabled || stage != Stage.ACTIVE || !channel.isActive() || finished.get() || System.nanoTime() > deadline) {
                reportBusy.set(false);
                return;
            }
            if (!playing() || !channel.isWritable()) {
                channel.eventLoop().schedule(() -> sendReportChunk(id, success, bytes, chunks, index, deadline), 50, TimeUnit.MILLISECONDS);
                return;
            }
            try {
                ByteBuf payload = AuxiliaryProtocol.reportChunk(id, success, index, chunks, bytes);
                channel.writeAndFlush(adapter.packet(AuxiliaryProtocol.REPORT_RESPONSE, payload)).addListener(result -> {
                    if (!result.isSuccess()) { reportBusy.set(false); channel.close(); }
                    else if (index + 1 < chunks) {
                        channel.eventLoop().execute(() -> sendReportChunk(id, success, bytes, chunks, index + 1, deadline));
                    } else reportBusy.set(false);
                });
            } catch (ReflectiveOperationException | RuntimeException error) {
                reportBusy.set(false);
                fail("could not send traffic report", error);
            }
        }
    }

    private final class StatusCommand implements SimpleCommand {
        @Override
        public boolean hasPermission(Invocation invocation) {
            boolean report = invocation.arguments().length > 0 && (invocation.arguments()[0].equalsIgnoreCase("report")
                    || invocation.arguments()[0].equalsIgnoreCase("export"));
            return invocation.source().hasPermission("zstdnet.admin")
                    || (report && (invocation.source().hasPermission("zstdnet.report")
                    || (invocation.source() instanceof Player player && canReport(player))));
        }

        @Override
        public List<String> suggest(Invocation invocation) {
            return invocation.arguments().length > 1 ? List.of("today", "session", "24h", "7d", "30d")
                    : List.of("status", "statistics", "report", "export", "allowreport", "reload");
        }

        @Override
        public void execute(Invocation invocation) {
            if (invocation.arguments().length == 1 && invocation.arguments()[0].equalsIgnoreCase("reload")) {
                if (!invocation.source().hasPermission("zstdnet.admin")) return;
                try {
                    reportWorker.execute(() -> {
                        try {
                            reloadSettings();
                            invocation.source().sendMessage(Component.text("ZstdNet 配置已重载，require_client="
                                    + settings.requireClient() + "；新登录按新设置检查，已在线玩家保持连接。"));
                        } catch (IOException | RuntimeException error) {
                            logger.warn("ZstdNet configuration reload failed; previous settings retained.", error);
                            invocation.source().sendMessage(Component.text("ZstdNet 重载失败，原设置保留：" + error.getMessage()));
                        }
                    });
                } catch (RejectedExecutionException error) {
                    invocation.source().sendMessage(Component.text("Report service is busy; try again later."));
                }
                return;
            }
            if (invocation.arguments().length >= 1 && invocation.arguments()[0].equalsIgnoreCase("allowreport")) {
                if (!invocation.source().hasPermission("zstdnet.admin")) return;
                if (invocation.arguments().length != 2) invocation.source().sendMessage(Component.text("Usage: /zstdnetproxy allowreport <online player>"));
                else allowReport(invocation, invocation.arguments()[1]);
                return;
            }
            if (invocation.arguments().length >= 1 && (invocation.arguments()[0].equalsIgnoreCase("report")
                    || invocation.arguments()[0].equalsIgnoreCase("export"))) {
                if (invocation.arguments().length > 2) {
                    invocation.source().sendMessage(Component.text("Usage: /zstdnetproxy report [today|session|24h|7d|30d]"));
                } else exportReport(invocation, invocation.arguments().length == 2 ? invocation.arguments()[1] : "today");
                return;
            }
            if (invocation.arguments().length > 1 || (invocation.arguments().length == 1
                    && !List.of("status", "statistics").contains(invocation.arguments()[0].toLowerCase(Locale.ROOT)))) {
                invocation.source().sendMessage(Component.text("Usage: /zstdnetproxy [status|statistics|report <range>|reload]"));
                return;
            }
            long active = sessions.values().stream().filter(session -> session.stage == Stage.ACTIVE).count();
            invocation.source().sendMessage(Component.text("ZstdNet 2.1.2: " + (enabled ? "enabled" : "disabled")
                    + ", active=" + active + ", level=" + settings.level() + ", require_client=" + settings.requireClient()
                    + ", target=Velocity 4.3.0 build 44"));
            if (invocation.arguments().length == 1 && invocation.arguments()[0].equalsIgnoreCase("statistics")) {
                TrafficRuntime history = traffic;
                if (history != null) {
                    history.collectAll();
                    var data = history.snapshot();
                    invocation.source().sendMessage(Component.text("This proxy run, bytes: client->proxy raw=" + data.rawIngressBytes()
                            + " wire=" + data.tcpIngressBytes() + "; proxy->client raw=" + data.rawEgressBytes() + " wire=" + data.tcpEgressBytes()));
                }
            }
        }
    }
}
