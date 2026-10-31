package cn.tohsaka.zstdnet.velocity;

import com.velocitypowered.api.event.connection.PluginMessageEvent;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ServerConnection;
import com.velocitypowered.api.proxy.messages.MinecraftChannelIdentifier;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ControlMessageTest {
    @Test
    void decodesOnlyCompleteCanonicalControlMessages() {
        assertEquals(new ControlMessage(0, 2), ControlMessage.decode(new byte[]{0, 2}));
        assertEquals(new ControlMessage(2, 2), ControlMessage.decode(new byte[]{2, 2}));
        byte[][] invalid = {
                {}, {0}, {0, 2, 0}, {4, 2}, {(byte) 0x80, 0, 2},
                {0, (byte) 0x82, 0}, {0, (byte) 0x80},
                {0, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f}
        };
        for (byte[] payload : invalid) {
            assertThrows(IllegalArgumentException.class, () -> ControlMessage.decode(payload));
        }
    }

    @Test
    void resolvesRealTargetPacketAndCopiesPayloadWithoutConsumingIt() throws Exception {
        VelocityAccess access = new VelocityAccess();
        Object packet = access.packet(ControlMessage.ACTIVATE);
        try {
            assertTrue(access.controlPacket(packet));
            assertArrayEquals(new byte[]{3, 2}, access.payload(packet));
            assertArrayEquals(new byte[]{3, 2}, access.payload(packet));
            assertEquals(1, ((ByteBufHolder) packet).refCnt());
        } finally {
            ReferenceCountUtil.release(packet);
        }
    }

    @Test
    void consumesBackendControlMessagesAndLeavesOtherChannelsAlone() {
        ZstdNetVelocityPlugin plugin = new ZstdNetVelocityPlugin(null,
                LoggerFactory.getLogger(ControlMessageTest.class), Path.of("unused-test-directory"));
        ServerConnection backend = fake(ServerConnection.class);
        Player player = fake(Player.class);
        PluginMessageEvent reserved = new PluginMessageEvent(backend, player,
                MinecraftChannelIdentifier.from(ControlMessage.CHANNEL), new byte[]{1, 2});
        plugin.pluginMessage(reserved);
        assertFalse(reserved.getResult().isAllowed());
        for (String channel : new String[]{AuxiliaryProtocol.HUD, AuxiliaryProtocol.REPORT_REQUEST, AuxiliaryProtocol.REPORT_RESPONSE}) {
            PluginMessageEvent injected = new PluginMessageEvent(backend, player,
                    MinecraftChannelIdentifier.from(channel), new byte[]{1, 2});
            plugin.pluginMessage(injected);
            assertFalse(injected.getResult().isAllowed(), "Backend injection must be consumed: " + channel);
        }

        PluginMessageEvent unrelated = new PluginMessageEvent(backend, player,
                MinecraftChannelIdentifier.from("example:other"), new byte[]{1, 2});
        plugin.pluginMessage(unrelated);
        assertTrue(unrelated.getResult().isAllowed());
    }

    @SuppressWarnings("unchecked")
    private static <T> T fake(Class<T> type) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (proxy, method, args) -> {
                    if (method.getName().equals("toString")) return "test-" + type.getSimpleName();
                    if (method.getName().equals("hashCode")) return System.identityHashCode(proxy);
                    if (method.getName().equals("equals")) return proxy == args[0];
                    return null;
                });
    }
}
