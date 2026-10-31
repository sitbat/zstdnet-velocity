package cn.tohsaka.zstdnet.velocity;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.netty.MinecraftDecoder;
import com.velocitypowered.proxy.protocol.netty.MinecraftEncoder;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.*;

class ConfigurationStateRegressionTest {
    @ParameterizedTest(name = "CONFIG to writable PLAY: {0}")
    @EnumSource(value = ProtocolVersion.class, names = {"MINECRAFT_1_21_11", "MINECRAFT_26_3"})
    void targetRuntimeReturnsToWritablePlayDespiteItsStickyConfigurationFlag(ProtocolVersion version) {
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            channel.pipeline().addLast("minecraft-decoder", new MinecraftDecoder(ProtocolUtils.Direction.SERVERBOUND));
            channel.pipeline().addLast("minecraft-encoder", new MinecraftEncoder(ProtocolUtils.Direction.CLIENTBOUND));
            MinecraftConnection connection = new MinecraftConnection(channel, null);
            connection.setProtocolVersion(version);
            connection.setState(StateRegistry.PLAY);
            connection.pendingConfigurationSwitch = true;
            connection.setState(StateRegistry.CONFIG);
            connection.addPlayPacketQueueOutboundHandler();
            assertTrue(VelocityAccess.hasPendingPlayPackets(channel));

            connection.setState(StateRegistry.PLAY);
            assertTrue(connection.pendingConfigurationSwitch, "the native 4.3.0-44 flag remains true");
            assertEquals(StateRegistry.PLAY, connection.getState());
            assertFalse(VelocityAccess.hasPendingPlayPackets(channel), "HUD/report/activation must resume after native queue removal");
        } finally { channel.finishAndReleaseAll(); }
    }
}
