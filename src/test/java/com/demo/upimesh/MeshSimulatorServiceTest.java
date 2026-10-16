package com.demo.upimesh;

import com.demo.upimesh.model.MeshPacket;
import com.demo.upimesh.service.MeshSimulatorService;
import com.demo.upimesh.service.MeshSimulatorService.BridgeUpload;
import com.demo.upimesh.service.MeshSimulatorService.GossipResult;
import com.demo.upimesh.service.VirtualDevice;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MeshSimulatorServiceTest {

    private MeshSimulatorService mesh;

    @BeforeEach
    void setUp() {
        mesh = new MeshSimulatorService();
    }

    private MeshPacket createTestPacket(String packetId, int ttl) {
        MeshPacket packet = new MeshPacket();
        packet.setPacketId(packetId != null ? packetId : UUID.randomUUID().toString());
        packet.setTtl(ttl);
        packet.setCreatedAt(System.currentTimeMillis());
        packet.setCiphertext("test-ciphertext-" + UUID.randomUUID());
        return packet;
    }

    // =========================================================================
    // A. Packet injection
    // =========================================================================

    @Test
    void injectValidPacketIntoValidDeviceSuccessfully() {
        MeshPacket packet = createTestPacket(null, 5);

        mesh.inject("phone-alice", packet);

        VirtualDevice alice = mesh.getDevice("phone-alice");
        assertNotNull(alice, "phone-alice should exist in default mesh");
        assertTrue(alice.holds(packet.getPacketId()), "phone-alice must hold the injected packet");
        assertEquals(1, alice.packetCount(), "phone-alice packet count must be 1");

        MeshPacket held = alice.getHeldPackets().iterator().next();
        assertEquals(packet.getPacketId(), held.getPacketId());
        assertEquals(5, held.getTtl());
        assertEquals(packet.getCiphertext(), held.getCiphertext());
    }

    @Test
    void injectIntoInvalidDeviceIdThrowsIllegalArgumentException() {
        MeshPacket packet = createTestPacket(null, 5);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                mesh.inject("unknown-device-xyz", packet));

        assertTrue(ex.getMessage().contains("Unknown device: unknown-device-xyz"));
    }

    @Test
    void injectPacketWithShortIdDoesNotThrowException() {
        MeshPacket packet = createTestPacket("pkt1", 3);

        assertDoesNotThrow(() -> mesh.inject("phone-alice", packet));

        VirtualDevice alice = mesh.getDevice("phone-alice");
        assertTrue(alice.holds("pkt1"));
        assertEquals(1, alice.packetCount());
    }

    // =========================================================================
    // B. Packet gossip and forwarding
    // =========================================================================

    @Test
    void gossipForwardsPacketsToConnectedDevices() {
        MeshPacket packet = createTestPacket(null, 5);
        mesh.inject("phone-alice", packet);

        // Before gossip: only alice holds it
        assertEquals(1, mesh.getDevice("phone-alice").packetCount());
        assertEquals(0, mesh.getDevice("phone-stranger1").packetCount());
        assertEquals(0, mesh.getDevice("phone-stranger2").packetCount());
        assertEquals(0, mesh.getDevice("phone-stranger3").packetCount());
        assertEquals(0, mesh.getDevice("phone-bridge").packetCount());

        GossipResult result = mesh.gossipOnce();

        // In 5-node complete mesh, 1 source gossips to 4 destinations = 4 transfers
        assertEquals(4, result.transfers(), "Gossip should transfer packet to the other 4 devices");

        for (VirtualDevice d : mesh.getDevices()) {
            assertTrue(d.holds(packet.getPacketId()),
                    "Device " + d.getDeviceId() + " must hold the forwarded packet");
            assertEquals(1, d.packetCount(),
                    "Device " + d.getDeviceId() + " should hold exactly 1 packet");
        }

        assertEquals(5, result.deviceCounts().size());
        result.deviceCounts().values().forEach(count -> assertEquals(1, count));
    }

    @Test
    void gossipDecreasesTtlOnForwardedCopies() {
        MeshPacket packet = createTestPacket(null, 5);
        mesh.inject("phone-alice", packet);

        mesh.gossipOnce();

        // Copies on other devices must have decremented TTL (5 - 1 = 4)
        VirtualDevice bridge = mesh.getDevice("phone-bridge");
        MeshPacket bridgePacket = bridge.getHeldPackets().iterator().next();
        assertEquals(4, bridgePacket.getTtl(), "Forwarded copy on phone-bridge should have TTL decremented to 4");

        VirtualDevice stranger1 = mesh.getDevice("phone-stranger1");
        MeshPacket strangerPacket = stranger1.getHeldPackets().iterator().next();
        assertEquals(4, strangerPacket.getTtl(), "Forwarded copy on stranger device should have TTL decremented to 4");
    }

    @Test
    void gossipPreservesOriginalPacketWithoutModification() {
        MeshPacket packet = createTestPacket(null, 5);
        mesh.inject("phone-alice", packet);

        mesh.gossipOnce();

        VirtualDevice alice = mesh.getDevice("phone-alice");
        MeshPacket original = alice.getHeldPackets().iterator().next();
        assertEquals(5, original.getTtl(), "Original packet on source device must keep its original TTL");
        assertEquals(packet.getPacketId(), original.getPacketId());
        assertEquals(packet.getCiphertext(), original.getCiphertext());
        assertEquals(packet.getCreatedAt(), original.getCreatedAt());
    }

    @Test
    void packetsWithTtlZeroAreNotForwarded() {
        MeshPacket packet = createTestPacket(null, 0);
        mesh.inject("phone-alice", packet);

        GossipResult result = mesh.gossipOnce();

        assertEquals(0, result.transfers(), "Packets with TTL=0 must not be forwarded");
        assertEquals(1, mesh.getDevice("phone-alice").packetCount(), "Source should still hold packet");

        // Verify other devices received nothing
        for (VirtualDevice d : mesh.getDevices()) {
            if (!d.getDeviceId().equals("phone-alice")) {
                assertEquals(0, d.packetCount(), "Device " + d.getDeviceId() + " should not receive packet with TTL=0");
                assertFalse(d.holds(packet.getPacketId()));
            }
        }
    }

    @Test
    void packetsWithNegativeTtlAreNotForwarded() {
        MeshPacket packet = createTestPacket(null, -1);
        mesh.inject("phone-alice", packet);

        GossipResult result = mesh.gossipOnce();

        assertEquals(0, result.transfers(), "Packets with negative TTL must not be forwarded");
        for (VirtualDevice d : mesh.getDevices()) {
            if (!d.getDeviceId().equals("phone-alice")) {
                assertEquals(0, d.packetCount());
            }
        }
    }

    // =========================================================================
    // C. Duplicate packet delivery
    // =========================================================================

    @Test
    void duplicatePacketDeliveryToSameDeviceDoesNotCreateDuplicates() {
        MeshPacket packet = createTestPacket(null, 5);
        VirtualDevice alice = mesh.getDevice("phone-alice");

        alice.hold(packet);
        assertEquals(1, alice.packetCount());

        // Hold the exact same instance again
        alice.hold(packet);
        assertEquals(1, alice.packetCount(), "Re-holding the same packet instance must not increase packetCount");

        // Hold a separate packet instance with the same packetId
        MeshPacket duplicate = new MeshPacket();
        duplicate.setPacketId(packet.getPacketId());
        duplicate.setTtl(3);
        duplicate.setCreatedAt(packet.getCreatedAt());
        duplicate.setCiphertext(packet.getCiphertext());

        alice.hold(duplicate);
        assertEquals(1, alice.packetCount(), "Holding another packet with the same packetId must not create duplicate");
    }

    @Test
    void subsequentGossipRoundsDoNotReForwardAlreadyHeldPackets() {
        MeshPacket packet = createTestPacket(null, 5);
        mesh.inject("phone-alice", packet);

        // Round 1: 4 transfers
        GossipResult round1 = mesh.gossipOnce();
        assertEquals(4, round1.transfers());

        // Round 2: all devices already hold this packetId -> 0 transfers
        GossipResult round2 = mesh.gossipOnce();
        assertEquals(0, round2.transfers(), "Subsequent round must not re-transfer already held packets");

        // Round 3: still 0 transfers
        GossipResult round3 = mesh.gossipOnce();
        assertEquals(0, round3.transfers());

        for (VirtualDevice d : mesh.getDevices()) {
            assertEquals(1, d.packetCount(), "Device " + d.getDeviceId() + " must maintain count of 1");
        }
    }

    // =========================================================================
    // D. Bridge upload collection
    // =========================================================================

    @Test
    void collectBridgeUploadsSelectsOnlyPacketsFromOnlineDevices() {
        MeshPacket packet = createTestPacket(null, 5);
        mesh.inject("phone-alice", packet);

        // Run gossip so packet reaches phone-bridge (hasInternet = true)
        mesh.gossipOnce();

        List<BridgeUpload> uploads = mesh.collectBridgeUploads();
        assertEquals(1, uploads.size(), "Only online bridge device should produce an upload");

        BridgeUpload upload = uploads.get(0);
        assertEquals("phone-bridge", upload.bridgeNodeId(), "Upload should come from phone-bridge");
        assertEquals(packet.getPacketId(), upload.packet().getPacketId());
        assertEquals(4, upload.packet().getTtl(), "Uploaded packet copy should have decremented TTL");
    }

    @Test
    void offlineDevicesDoNotContributeToBridgeUploads() {
        MeshPacket packet = createTestPacket(null, 5);
        mesh.inject("phone-alice", packet);

        // DO NOT run gossip. The packet is only on phone-alice (hasInternet = false)
        List<BridgeUpload> uploads = mesh.collectBridgeUploads();
        assertTrue(uploads.isEmpty(), "Offline devices must not contribute to bridge uploads");

        VirtualDevice alice = mesh.getDevice("phone-alice");
        assertFalse(alice.hasInternet(), "phone-alice should be offline");
        assertEquals(1, alice.packetCount(), "phone-alice holds packet but cannot upload");
    }

    // =========================================================================
    // E. Mesh reset
    // =========================================================================

    @Test
    void resetMeshClearsHeldPacketsAcrossAllDevices() {
        MeshPacket packet1 = createTestPacket(null, 5);
        MeshPacket packet2 = createTestPacket(null, 4);

        mesh.inject("phone-alice", packet1);
        mesh.inject("phone-stranger1", packet2);
        mesh.gossipOnce();

        // Verify devices hold packets
        for (VirtualDevice d : mesh.getDevices()) {
            assertTrue(d.packetCount() > 0);
        }
        assertFalse(mesh.collectBridgeUploads().isEmpty());

        mesh.resetMesh();

        // Verify all devices are cleared
        for (VirtualDevice d : mesh.getDevices()) {
            assertEquals(0, d.packetCount(), "Device " + d.getDeviceId() + " should have 0 packets after reset");
            assertTrue(d.getHeldPackets().isEmpty());
            assertFalse(d.holds(packet1.getPacketId()));
            assertFalse(d.holds(packet2.getPacketId()));
        }

        assertTrue(mesh.collectBridgeUploads().isEmpty(), "Bridge uploads must be empty after reset");
        mesh.snapshotMap().values().forEach(count -> assertEquals(0, count));
    }

    // =========================================================================
    // F. VirtualDevice unit tests
    // =========================================================================

    @Test
    void virtualDeviceBasicOperations() {
        VirtualDevice device = new VirtualDevice("test-device", false);

        assertEquals("test-device", device.getDeviceId());
        assertFalse(device.hasInternet());
        assertEquals(0, device.packetCount());
        assertFalse(device.holds("nonexistent"));

        MeshPacket packet = createTestPacket("pkt-vd-1", 5);
        device.hold(packet);

        assertEquals(1, device.packetCount());
        assertTrue(device.holds("pkt-vd-1"));
        assertEquals(1, device.getHeldPackets().size());

        device.clear();
        assertEquals(0, device.packetCount());
        assertFalse(device.holds("pkt-vd-1"));
        assertTrue(device.getHeldPackets().isEmpty());
    }
}
