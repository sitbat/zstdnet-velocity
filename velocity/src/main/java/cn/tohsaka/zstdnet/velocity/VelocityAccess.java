package cn.tohsaka.zstdnet.velocity;

import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.config.ProxyConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.Channel;
import io.netty.handler.codec.DecoderException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.function.IntConsumer;

/** All unsupported Velocity internals are confined to this version-checked adapter. */
final class VelocityAccess {
    private final Class<?> playerType;
    private final Class<?> packetType;
    private final Method playerConnection;
    private final Method connectionChannel;
    private final Method packetChannel;
    private final Constructor<?> packetConstructor;
    private final Field framePacketLimiter;
    private final Field compressionPacketLimiter;
    private final Method limiterAccount;

    VelocityAccess() throws ReflectiveOperationException {
        ClassLoader loader = Player.class.getClassLoader();
        playerType = Class.forName("com.velocitypowered.proxy.connection.client.ConnectedPlayer", false, loader);
        Class<?> connectionType = Class.forName("com.velocitypowered.proxy.connection.MinecraftConnection", false, loader);
        packetType = Class.forName("com.velocitypowered.proxy.protocol.packet.PluginMessagePacket", false, loader);
        playerConnection = playerType.getMethod("getConnection");
        connectionChannel = connectionType.getMethod("getChannel");
        packetChannel = packetType.getMethod("getChannel");
        packetConstructor = packetType.getConstructor(String.class, ByteBuf.class);
        Class<?> frameType = Class.forName("com.velocitypowered.proxy.protocol.netty.MinecraftVarintFrameDecoder", false, loader);
        Class<?> compressionType = Class.forName("com.velocitypowered.proxy.protocol.netty.MinecraftCompressDecoder", false, loader);
        Class<?> limiterType = Class.forName("com.velocitypowered.proxy.network.limiter.PacketLimiter", false, loader);
        framePacketLimiter = frameType.getDeclaredField("packetLimiter");
        compressionPacketLimiter = compressionType.getDeclaredField("packetLimiter");
        framePacketLimiter.setAccessible(true);
        compressionPacketLimiter.setAccessible(true);
        limiterAccount = limiterType.getMethod("account", int.class);
    }

    Channel channel(Player player) throws ReflectiveOperationException {
        if (!playerType.isInstance(player)) {
            throw new IllegalArgumentException("unsupported Velocity Player implementation");
        }
        return (Channel) connectionChannel.invoke(playerConnection.invoke(player));
    }

    boolean switchingConfiguration(Player player) throws ReflectiveOperationException {
        return hasPendingPlayPackets(channel(player));
    }

    static boolean hasPendingPlayPackets(Channel channel) {
        // pendingConfigurationSwitch is sticky in 4.3.0-44; the actual queue is removed on PLAY.
        return channel.pipeline().get("play-packet-queue-outbound") != null;
    }

    boolean modern(ProxyConfig config) throws ReflectiveOperationException {
        Object mode = config.getClass().getMethod("getPlayerInfoForwardingMode").invoke(config);
        return "MODERN".equals(String.valueOf(mode));
    }

    IntConsumer inboundAccount(Channel channel) throws ReflectiveOperationException {
        Object frame = channel.pipeline().get("frame-decoder");
        Object compression = channel.pipeline().get("compression-decoder");
        Object frameLimiter = framePacketLimiter.get(frame);
        Object compressionLimiter = compression == null ? null : compressionPacketLimiter.get(compression);
        return bytes -> {
            try {
                // Using raw packet bytes for the old wire-byte limiter is conservative; the
                // decompressed-byte limiter and native packet counter keep their configured state.
                if (frameLimiter != null && !((Boolean) limiterAccount.invoke(frameLimiter, bytes))) {
                    throw new DecoderException("Velocity packet/wire byte limit exceeded");
                }
                if (compressionLimiter != null && !((Boolean) limiterAccount.invoke(compressionLimiter, bytes))) {
                    throw new DecoderException("Velocity decompressed byte limit exceeded");
                }
            } catch (ReflectiveOperationException error) {
                throw new DecoderException("could not account Velocity transport packet", error);
            }
        };
    }

    boolean controlPacket(Object message) throws ReflectiveOperationException {
        return packetType.isInstance(message) && ControlMessage.CHANNEL.equals(packetChannel.invoke(message));
    }

    byte[] payload(Object message) {
        ByteBuf content = ((ByteBufHolder) message).content();
        if (content.readableBytes() > 10) {
            throw new IllegalArgumentException("oversized control packet");
        }
        byte[] bytes = new byte[content.readableBytes()];
        content.getBytes(content.readerIndex(), bytes);
        return bytes;
    }

    Object packet(int opcode) throws ReflectiveOperationException {
        return packet(ControlMessage.CHANNEL, ControlMessage.encode(opcode));
    }

    Object packet(String channel, ByteBuf payload) throws ReflectiveOperationException {
        try {
            return packetConstructor.newInstance(channel, payload);
        } catch (ReflectiveOperationException | RuntimeException error) {
            payload.release();
            throw error;
        }
    }
}
