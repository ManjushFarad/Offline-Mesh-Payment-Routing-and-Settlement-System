package com.demo.upimesh.service;

import com.demo.upimesh.crypto.HybridCryptoService;
import com.demo.upimesh.model.MeshPacket;
import com.demo.upimesh.model.PaymentInstruction;
import com.demo.upimesh.model.Transaction;
import com.demo.upimesh.model.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Orchestrates the full server-side pipeline for one inbound packet from a
 * bridge node:
 *
 *   1. Hash the ciphertext.
 *   2. Try to claim that hash via the idempotency cache.
 *      - If already claimed: this is a duplicate. Drop it.
 *   3. Decrypt the ciphertext with the server's private key.
 *      - If decryption fails: tampered or junk. Reject.
 *   4. Check freshness — reject if signedAt is too old (replay protection).
 *   5. Durable duplicate detection via database lookup.
 *   6. Hand off to SettlementService with bounded optimistic-lock retry.
 */
@Service
public class BridgeIngestionService {

    private static final Logger log = LoggerFactory.getLogger(BridgeIngestionService.class);

    @Autowired private HybridCryptoService crypto;
    @Autowired private IdempotencyService idempotency;
    @Autowired private SettlementService settlement;
    @Autowired private TransactionRepository transactionRepository;

    @Value("${upi.mesh.packet-max-age-seconds:86400}")
    private long maxAgeSeconds;

    private volatile long resetTimestamp = 0;

    public void recordReset() {
        this.resetTimestamp = System.currentTimeMillis();
        log.info("Reset recorded at timestamp {}", resetTimestamp);
    }

    public long getResetTimestamp() {
        return resetTimestamp;
    }

    public void clearResetTimestamp() {
        this.resetTimestamp = 0;
    }

    public IngestResult ingest(MeshPacket packet, String bridgeNodeId, int hopCount) {
        String packetHash;
        try {
            packetHash = crypto.hashCiphertext(packet.getCiphertext());
        } catch (Exception e) {
            log.error("Hashing failed: {}", e.getMessage());
            return IngestResult.invalid("?", "hashing_failed");
        }

        // ---- Idempotency gate ----
        if (!idempotency.claim(packetHash)) {
            log.info("DUPLICATE packet {} from bridge {} — dropped",
                    packetHash.substring(0, 12) + "...", bridgeNodeId);
            return IngestResult.duplicate(packetHash);
        }

        // ---- Decrypt ----
        PaymentInstruction instruction;
        try {
            instruction = crypto.decrypt(packet.getCiphertext());
        } catch (Exception e) {
            log.warn("Decryption failed for packet {}: {}",
                    packetHash.substring(0, 12) + "...", e.getMessage());
            return IngestResult.invalid(packetHash, "decryption_failed");
        }

        // ---- Freshness check (replay protection) ----
        long ageSeconds = (Instant.now().toEpochMilli() - instruction.getSignedAt()) / 1000;
        if (ageSeconds > maxAgeSeconds) {
            log.warn("Packet {} too old ({}s), rejected",
                    packetHash.substring(0, 12) + "...", ageSeconds);
            return IngestResult.invalid(packetHash, "stale_packet");
        }
        if (ageSeconds < -300) { // small clock-skew tolerance
            return IngestResult.invalid(packetHash, "future_dated");
        }

        // ---- Invalidate packets signed or created prior to system reset ----
        if ((packet.getCreatedAt() != null && packet.getCreatedAt() <= resetTimestamp)
                || (instruction.getSignedAt() != null && instruction.getSignedAt() <= resetTimestamp)) {
            log.warn("Packet {} created before system reset (createdAt={}, signedAt={}, resetAt={}), rejected",
                    packetHash.substring(0, Math.min(12, packetHash.length())) + "...",
                    packet.getCreatedAt(), instruction.getSignedAt(), resetTimestamp);
            return IngestResult.invalid(packetHash, "stale_packet");
        }

        // ---- Durable duplicate detection ----
        try {
            Optional<IngestResult> existing = findPersistedResult(packetHash);
            if (existing.isPresent()) {
                log.info("Packet {} already persisted in DB with outcome {}",
                        packetHash.substring(0, 12) + "...", existing.get().outcome());
                return existing.get();
            }
        } catch (Exception e) {
            log.error("Database lookup failed for packet {}: {}", packetHash, e.getMessage());
            return IngestResult.invalid(packetHash, "indeterminate_database_state");
        }

        // ---- Bounded settlement attempts ----
        int maxAttempts = 3;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                Transaction tx = settlement.settle(instruction, packetHash, bridgeNodeId, hopCount);
                return toIngestResult(packetHash, tx);

            } catch (ObjectOptimisticLockingFailureException oclfe) {
                log.warn("Optimistic lock conflict on packet {} (attempt {}/{}): {}",
                        packetHash.substring(0, 12) + "...", attempt, maxAttempts, oclfe.getMessage());

                // Verify if the transaction committed despite the exception
                try {
                    Optional<IngestResult> committed = findPersistedResult(packetHash);
                    if (committed.isPresent()) {
                        return committed.get();
                    }
                } catch (Exception lookupEx) {
                    log.error("Database lookup failed after lock conflict for packet {}: {}", packetHash, lookupEx.getMessage());
                    return IngestResult.invalid(packetHash, "indeterminate_database_state");
                }

                if (attempt == maxAttempts) {
                    // All retries exhausted and confirmed absent from DB -> safe to release in-flight claim
                    idempotency.unclaim(packetHash);
                    return IngestResult.invalid(packetHash, "concurrency_conflict");
                }

                // Short jittered delay before retrying in a fresh transaction
                try {
                    Thread.sleep(20 + ThreadLocalRandom.current().nextInt(30));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return IngestResult.invalid(packetHash, "interrupted_during_retry");
                }

            } catch (DataIntegrityViolationException dive) {
                log.warn("Data integrity violation for packet {}: {}", packetHash.substring(0, 12) + "...", dive.getMessage());
                try {
                    Optional<IngestResult> committed = findPersistedResult(packetHash);
                    if (committed.isPresent()) {
                        return committed.get();
                    }
                } catch (Exception lookupEx) {
                    log.error("Lookup failed after DataIntegrityViolationException for packet {}: {}", packetHash, lookupEx.getMessage());
                    return IngestResult.invalid(packetHash, "indeterminate_database_state");
                }
                return IngestResult.invalid(packetHash, "data_integrity_violation");

            } catch (IllegalArgumentException iae) {
                // Permanent business validation failure (e.g. unknown VPA, non-positive amount)
                log.warn("Validation error during settlement for packet {}: {}", packetHash.substring(0, 12) + "...", iae.getMessage());
                return IngestResult.invalid(packetHash, "invalid_payment: " + iae.getMessage());

            } catch (Exception e) {
                log.error("Unexpected error during settlement for packet {}: {}", packetHash.substring(0, 12) + "...", e.getMessage(), e);
                // Check if the transaction actually committed before reporting error
                try {
                    Optional<IngestResult> committed = findPersistedResult(packetHash);
                    if (committed.isPresent()) {
                        return committed.get();
                    }
                } catch (Exception lookupEx) {
                    log.error("Database lookup failed after unexpected error: {}", lookupEx.getMessage());
                }
                return IngestResult.invalid(packetHash, "indeterminate_database_state");
            }
        }

        return IngestResult.invalid(packetHash, "concurrency_conflict");
    }

    private IngestResult toIngestResult(String packetHash, Transaction tx) {
        if (tx.getStatus() == Transaction.Status.REJECTED) {
            String reason = tx.getRejectionReason();
            if (reason == null || reason.isBlank()) {
                if (tx.getSenderVpa() != null && tx.getSenderVpa().equalsIgnoreCase(tx.getReceiverVpa())) {
                    reason = "self_transfer";
                } else {
                    reason = "insufficient_balance";
                }
            }
            return IngestResult.rejected(packetHash, reason, tx);
        }
        return IngestResult.settled(packetHash, tx);
    }

    private Optional<IngestResult> findPersistedResult(String packetHash) {
        return transactionRepository.findByPacketHash(packetHash)
                .map(tx -> toIngestResult(packetHash, tx));
    }

    public record IngestResult(String outcome, String packetHash, String reason, Long transactionId) {
        public static IngestResult settled(String hash, Transaction tx) {
            return new IngestResult("SETTLED", hash, null, tx.getId());
        }
        public static IngestResult rejected(String hash, String reason, Transaction tx) {
            return new IngestResult("REJECTED", hash, reason, tx != null ? tx.getId() : null);
        }
        public static IngestResult duplicate(String hash) {
            return new IngestResult("DUPLICATE_DROPPED", hash, null, null);
        }
        public static IngestResult invalid(String hash, String reason) {
            return new IngestResult("INVALID", hash, reason, null);
        }
    }
}
