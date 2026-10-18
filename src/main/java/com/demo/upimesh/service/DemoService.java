package com.demo.upimesh.service;

import com.demo.upimesh.crypto.HybridCryptoService;
import com.demo.upimesh.crypto.ServerKeyHolder;
import com.demo.upimesh.model.Account;
import com.demo.upimesh.model.AccountRepository;
import com.demo.upimesh.model.MeshPacket;
import com.demo.upimesh.model.PaymentInstruction;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.UUID;

import com.demo.upimesh.model.LedgerEntryRepository;
import com.demo.upimesh.model.TransactionRepository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

/**
 * Helper service that:
 *   - seeds demo accounts on startup
 *   - simulates "sender phone creates an encrypted packet" flow
 *   - resets system state to initial configured demo state
 */
@Service
public class DemoService {

    private static final Logger log = LoggerFactory.getLogger(DemoService.class);

    public static final Map<String, BigDecimal> INITIAL_BALANCES = Map.of(
            "alice@demo", new BigDecimal("5000.00"),
            "bob@demo",   new BigDecimal("1000.00"),
            "carol@demo", new BigDecimal("2500.00"),
            "dave@demo",  new BigDecimal("500.00")
    );

    public static final Map<String, String> INITIAL_HOLDERS = Map.of(
            "alice@demo", "Alice",
            "bob@demo",   "Bob",
            "carol@demo", "Carol",
            "dave@demo",  "Dave"
    );

    @Autowired private AccountRepository accounts;
    @Autowired private TransactionRepository transactions;
    @Autowired private LedgerEntryRepository ledgerEntries;
    @Autowired private MeshSimulatorService mesh;
    @Autowired private IdempotencyService idempotency;
    @Autowired private BridgeIngestionService bridge;
    @Autowired private HybridCryptoService crypto;
    @Autowired private ServerKeyHolder serverKey;

    @PostConstruct
    public void seedAccounts() {
        if (accounts.count() == 0) {
            INITIAL_BALANCES.forEach((vpa, bal) ->
                    accounts.save(new Account(vpa, INITIAL_HOLDERS.get(vpa), bal)));
            log.info("Seeded 4 demo accounts");
        }
    }

    /**
     * Resets the entire demonstration state:
     *   1. Deletes all ledger entries (respecting foreign-key order).
     *   2. Deletes all transaction records.
     *   3. Restores all demo accounts to their configured initial balances without changing identities.
     *   4. Clears in-memory mesh device packet queues.
     *   5. Clears in-memory idempotency claims.
     *   6. Records reset timestamp to invalidate and prevent old packets from settling.
     */
    @Transactional
    public void resetAllState() {
        ledgerEntries.deleteAllInBatch();
        transactions.deleteAllInBatch();

        INITIAL_BALANCES.forEach((vpa, bal) -> {
            if (vpa == null) {
                return;
            }
            Account acc = accounts.findById(vpa).orElse(null);
            if (acc != null) {
                acc.setBalance(bal);
                accounts.save(acc);
            } else {
                accounts.save(new Account(vpa, INITIAL_HOLDERS.get(vpa), bal));
            }
        });

        mesh.resetMesh();
        idempotency.clear();
        bridge.recordReset();
        log.info("System state reset complete: accounts restored, ledger/transactions cleared, mesh/idempotency cleared");
    }

    /**
     * Simulates the sender's phone:
     *   1. Build a PaymentInstruction with a fresh nonce + signedAt timestamp.
     *   2. Encrypt with the server's public key (hybrid RSA+AES).
     *   3. Wrap in a MeshPacket with TTL.
     *
     * In a real Android app, this exact code (minus the server-side reference)
     * would run on the phone. The phone would have already cached the server's
     * public key during a previous online session.
     */
    public MeshPacket createPacket(String senderVpa, String receiverVpa,
                                   BigDecimal amount, String pin, int ttl) throws Exception {
        long signedAt = Instant.now().toEpochMilli();
        if (bridge != null && signedAt <= bridge.getResetTimestamp()) {
            signedAt = bridge.getResetTimestamp() + 1;
        }

        PaymentInstruction instruction = new PaymentInstruction(
                senderVpa,
                receiverVpa,
                amount,
                sha256Hex(pin),
                UUID.randomUUID().toString(),       // nonce — guarantees uniqueness
                signedAt                            // signedAt — for freshness check
        );

        String ciphertext = crypto.encrypt(instruction, serverKey.getPublicKey());

        MeshPacket packet = new MeshPacket();
        packet.setPacketId(UUID.randomUUID().toString());
        packet.setTtl(ttl);
        packet.setCreatedAt(signedAt);
        packet.setCiphertext(ciphertext);
        return packet;
    }

    private String sha256Hex(String input) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder();
        for (byte b : hash) hex.append(String.format("%02x", b));
        return hex.toString();
    }
}
