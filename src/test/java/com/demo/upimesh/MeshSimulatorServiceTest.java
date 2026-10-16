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
    // Packet Injection
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
    // Packet Gossip and Forwarding
    // =========================================================================

    @Test
    void gossipForwardsPacketsToConnectedDevicesAndDecrementsTtl() {
        MeshPacket packet = createTestPacket(null, 5);
        mesh.inject("phone-alice", packet);

        GossipResult result = mesh.gossipOnce();

        // In 5-node mesh, 1 source gossips to 4 destinations = 4 transfers
        assertEquals(4, result.transfers(), "Gossip should transfer packet to the other 4 devices");

        for (VirtualDevice d : mesh.getDevices()) {
            assertTrue(d.holds(packet.getPacketId()), "Device " + d.getDeviceId() + " must hold packet");
            assertEquals(1, d.packetCount());
            if (!d.getDeviceId().equals("phone-alice")) {
                MeshPacket copy = d.getHeldPackets().iterator().next();
                assertEquals(4, copy.getTtl(), "Forwarded copy on " + d.getDeviceId() + " must have TTL=4");
            }
        }
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

        for (VirtualDevice d : mesh.getDevices()) {
            if (!d.getDeviceId().equals("phone-alice")) {
                assertEquals(0, d.packetCount(), "Device " + d.getDeviceId() + " should not receive packet with TTL=0");
                assertFalse(d.holds(packet.getPacketId()));
            }
        }
    }

    // =========================================================================
    // Duplicate Packet Handling
    // =========================================================================

    @Test
    void duplicatePacketDeliveryToSameDeviceDoesNotCreateDuplicates() {
        MeshPacket packet = createTestPacket(null, 5);
        VirtualDevice alice = mesh.getDevice("phone-alice");

        alice.hold(packet);
        assertEquals(1, alice.packetCount());

        // Re-hold same instance
        alice.hold(packet);
        assertEquals(1, alice.packetCount(), "Re-holding the same packet instance must not increase packetCount");

        // Hold separate instance with identical packetId
        MeshPacket duplicate = new MeshPacket();
        duplicate.setPacketId(packet.getPacketId());
        duplicate.setTtl(3);
        duplicate.setCreatedAt(packet.getCreatedAt());
        duplicate.setCiphertext(packet.getCiphertext());

        alice.hold(duplicate);
        assertEquals(1, alice.packetCount(), "Holding another packet with same packetId must not create duplicate");
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

        for (VirtualDevice d : mesh.getDevices()) {
            assertEquals(1, d.packetCount(), "Device " + d.getDeviceId() + " must maintain count of 1");
        }
    }

    // =========================================================================
    // Bridge Upload Collection
    // =========================================================================

    @Test
    void collectBridgeUploadsSelectsOnlyPacketsFromOnlineDevices() {
        MeshPacket packet = createTestPacket(null, 5);
        mesh.inject("phone-alice", packet);

        // Before gossip: packet is only on phone-alice (hasInternet = false) -> no uploads
        assertTrue(mesh.collectBridgeUploads().isEmpty(), "Offline devices must not contribute to uploads");

        // Run gossip so packet reaches phone-bridge (hasInternet = true)
        mesh.gossipOnce();

        List<BridgeUpload> uploads = mesh.collectBridgeUploads();
        assertEquals(1, uploads.size(), "Only online bridge device should produce an upload");

        BridgeUpload upload = uploads.get(0);
        assertEquals("phone-bridge", upload.bridgeNodeId());
        assertEquals(packet.getPacketId(), upload.packet().getPacketId());
        assertEquals(4, upload.packet().getTtl(), "Uploaded packet copy should have decremented TTL");
    }

    // =========================================================================
    // Mesh Reset
    // =========================================================================

    @Test
    void resetMeshClearsHeldPacketsAcrossAllDevices() {
        MeshPacket packet1 = createTestPacket(null, 5);
        MeshPacket packet2 = createTestPacket(null, 4);

        mesh.inject("phone-alice", packet1);
        mesh.inject("phone-stranger1", packet2);
        mesh.gossipOnce();

        mesh.resetMesh();

        for (VirtualDevice d : mesh.getDevices()) {
            assertEquals(0, d.packetCount(), "Device " + d.getDeviceId() + " should have 0 packets after reset");
            assertTrue(d.getHeldPackets().isEmpty());
            assertFalse(d.holds(packet1.getPacketId()));
            assertFalse(d.holds(packet2.getPacketId()));
        }

        assertTrue(mesh.collectBridgeUploads().isEmpty(), "Bridge uploads must be empty after reset");
    }
}
