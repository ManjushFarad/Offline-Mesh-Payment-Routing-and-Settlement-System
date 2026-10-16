package com.demo.upimesh;

import com.demo.upimesh.crypto.HybridCryptoService;
import com.demo.upimesh.crypto.ServerKeyHolder;
import com.demo.upimesh.model.Account;
import com.demo.upimesh.model.AccountRepository;
import com.demo.upimesh.model.LedgerEntry;
import com.demo.upimesh.model.LedgerEntryRepository;
import com.demo.upimesh.model.MeshPacket;
import com.demo.upimesh.model.PaymentInstruction;
import com.demo.upimesh.model.Transaction;
import com.demo.upimesh.model.TransactionRepository;
import com.demo.upimesh.service.BridgeIngestionService;
import com.demo.upimesh.service.DemoService;
import com.demo.upimesh.service.IdempotencyService;
import com.demo.upimesh.controller.ApiController;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.demo.upimesh.service.MeshSimulatorService;
import com.demo.upimesh.service.VirtualDevice;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
    "spring.datasource.url=jdbc:h2:mem:upimesh;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "spring.datasource.username=sa",
    "spring.datasource.password=",
    "spring.jpa.hibernate.ddl-auto=create-drop",
    "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
    "spring.jpa.open-in-view=false"
})
class IdempotencyConcurrencyTest {

    @Autowired private MockMvc mockMvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private DemoService demoService;
    @Autowired private BridgeIngestionService bridge;
    @Autowired private IdempotencyService idempotency;
    @Autowired private MeshSimulatorService mesh;
    @Autowired private AccountRepository accounts;
    @Autowired private TransactionRepository transactions;
    @SpyBean private LedgerEntryRepository ledgerEntries;
    @Autowired private HybridCryptoService crypto;
    @Autowired private ServerKeyHolder serverKey;

    @BeforeEach
    void clear() {
        idempotency.clear();
        reset(ledgerEntries);
        bridge.clearResetTimestamp();
    }

    // =========================================================================
    // 1. Successful Payment & Balanced Ledger
    // =========================================================================

    @Test
    void successfulPaymentUpdatesBalancesAndCreatesBalancedLedgerEntries() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobBefore = accounts.findById("bob@demo").orElseThrow().getBalance();
        BigDecimal sendAmount = new BigDecimal("50.00");

        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", sendAmount, "1234", 5);

        BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-success", 2);

        assertEquals("SETTLED", r.outcome(), "Ingestion should return outcome SETTLED");
        assertNull(r.reason());
        assertNotNull(r.transactionId());

        // Verify transaction record
        Transaction tx = transactions.findById(r.transactionId()).orElse(null);
        assertNotNull(tx);
        assertEquals(Transaction.Status.SETTLED, tx.getStatus());
        assertEquals(0, sendAmount.compareTo(tx.getAmount()));
        assertEquals("alice@demo", tx.getSenderVpa());
        assertEquals("bob@demo", tx.getReceiverVpa());

        // Verify account balance updates
        BigDecimal aliceAfter = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobAfter = accounts.findById("bob@demo").orElseThrow().getBalance();
        assertEquals(0, aliceBefore.subtract(sendAmount).compareTo(aliceAfter), "Sender balance should be debited");
        assertEquals(0, bobBefore.add(sendAmount).compareTo(bobAfter), "Receiver balance should be credited");

        // Verify double-entry ledger entries
        List<LedgerEntry> entries = ledgerEntries.findByPacketHash(r.packetHash());
        assertEquals(2, entries.size(), "Should create exactly two ledger entries for the payment");

        LedgerEntry debit = entries.stream()
                .filter(e -> e.getEntryType() == LedgerEntry.EntryType.DEBIT)
                .findFirst().orElse(null);
        LedgerEntry credit = entries.stream()
                .filter(e -> e.getEntryType() == LedgerEntry.EntryType.CREDIT)
                .findFirst().orElse(null);

        assertNotNull(debit, "One entry must be DEBIT");
        assertNotNull(credit, "One entry must be CREDIT");
        assertEquals("alice@demo", debit.getAccountVpa(), "DEBIT entry must belong to sender");
        assertEquals("bob@demo", credit.getAccountVpa(), "CREDIT entry must belong to receiver");
        assertEquals(0, sendAmount.compareTo(debit.getAmount()));
        assertEquals(0, sendAmount.compareTo(credit.getAmount()));
    }

    // =========================================================================
    // 2. Insufficient Balance Handling
    // =========================================================================

    @Test
    void insufficientBalanceRejectsPaymentWithoutBalanceOrLedgerChanges() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobBefore = accounts.findById("bob@demo").orElseThrow().getBalance();
        BigDecimal excessAmount = aliceBefore.add(new BigDecimal("1000.00"));

        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", excessAmount, "1234", 5);

        BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-insufficient", 1);

        assertEquals("REJECTED", r.outcome());
        assertEquals("insufficient_balance", r.reason());
        assertNotNull(r.transactionId());

        // Verify rejected transaction record
        Transaction tx = transactions.findById(r.transactionId()).orElse(null);
        assertNotNull(tx);
        assertEquals(Transaction.Status.REJECTED, tx.getStatus());
        assertEquals(0, excessAmount.compareTo(tx.getAmount()));

        // Balances remain untouched
        BigDecimal aliceAfter = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobAfter = accounts.findById("bob@demo").orElseThrow().getBalance();
        assertEquals(0, aliceBefore.compareTo(aliceAfter), "Sender balance must remain unchanged");
        assertEquals(0, bobBefore.compareTo(bobAfter), "Receiver balance must remain unchanged");

        // No ledger entries created
        List<LedgerEntry> entries = ledgerEntries.findByPacketHash(r.packetHash());
        assertTrue(entries.isEmpty(), "Rejected payment must create zero ledger entries");
    }

    // =========================================================================
    // 3. Self-Transfer Handling
    // =========================================================================

    @Test
    void selfTransferIsRejectedWithoutBalanceOrLedgerChanges() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        MeshPacket packet = demoService.createPacket(
                "alice@demo", "alice@demo", new BigDecimal("50.00"), "1234", 5);

        BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-self-test", 1);

        assertEquals("REJECTED", r.outcome());
        assertEquals("self_transfer", r.reason());
        assertNotNull(r.transactionId());

        Transaction tx = transactions.findById(r.transactionId()).orElse(null);
        assertNotNull(tx);
        assertEquals(Transaction.Status.REJECTED, tx.getStatus());
        assertEquals("self_transfer", tx.getRejectionReason());

        // Balance unchanged
        BigDecimal aliceAfter = accounts.findById("alice@demo").orElseThrow().getBalance();
        assertEquals(0, aliceBefore.compareTo(aliceAfter), "Sender balance must not change on self-transfer");

        // Zero ledger entries created
        List<LedgerEntry> entries = ledgerEntries.findByPacketHash(r.packetHash());
        assertTrue(entries.isEmpty(), "Rejected self-transfer must not create any ledger entries");
    }

    // =========================================================================
    // 4. Duplicate Payment Prevention (In-Memory & Durable DB)
    // =========================================================================

    @Test
    void singlePacketDeliveredByThreeBridgesSettlesExactlyOnce() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobBefore = accounts.findById("bob@demo").orElseThrow().getBalance();

        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", new BigDecimal("100.00"), "1234", 5);

        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger settled = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();

        Future<?>[] futures = new Future[3];
        for (int i = 0; i < 3; i++) {
            final String node = "bridge-" + i;
            futures[i] = pool.submit(() -> {
                try {
                    start.await();
                    BridgeIngestionService.IngestResult r = bridge.ingest(packet, node, 3);
                    if ("SETTLED".equals(r.outcome())) settled.incrementAndGet();
                    else if ("DUPLICATE_DROPPED".equals(r.outcome())) duplicates.incrementAndGet();
                } catch (Exception e) { throw new RuntimeException(e); }
            });
        }

        start.countDown();
        for (Future<?> f : futures) f.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(1, settled.get(), "Exactly one bridge upload should settle");
        assertEquals(2, duplicates.get(), "The other two concurrent uploads should be dropped as duplicates");

        BigDecimal aliceAfter = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobAfter = accounts.findById("bob@demo").orElseThrow().getBalance();
        assertEquals(aliceBefore.subtract(new BigDecimal("100.00")), aliceAfter);
        assertEquals(bobBefore.add(new BigDecimal("100.00")), bobAfter);
    }

    @Test
    void alreadyPersistedSettledTransactionReturnedWithoutSecondDebit() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobBefore = accounts.findById("bob@demo").orElseThrow().getBalance();
        BigDecimal sendAmount = new BigDecimal("40.00");

        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", sendAmount, "1234", 5);

        // 1. Initial ingestion settles
        BridgeIngestionService.IngestResult firstResult = bridge.ingest(packet, "bridge-1", 1);
        assertEquals("SETTLED", firstResult.outcome());
        Long txId = firstResult.transactionId();
        assertNotNull(txId);

        BigDecimal aliceAfterFirst = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobAfterFirst = accounts.findById("bob@demo").orElseThrow().getBalance();

        // 2. Clear in-memory claim to force durable lookup from database
        idempotency.clear();

        // 3. Second ingestion with same packet
        BridgeIngestionService.IngestResult secondResult = bridge.ingest(packet, "bridge-2", 2);
        assertEquals("SETTLED", secondResult.outcome());
        assertEquals(txId, secondResult.transactionId(), "Should return existing transaction ID");

        // 4. Balances must be untouched (no double-debit)
        BigDecimal aliceAfterSecond = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobAfterSecond = accounts.findById("bob@demo").orElseThrow().getBalance();
        assertEquals(0, aliceAfterFirst.compareTo(aliceAfterSecond), "Sender balance must not change on duplicate");
        assertEquals(0, bobAfterFirst.compareTo(bobAfterSecond), "Receiver balance must not change on duplicate");
    }

    @Test
    void alreadyPersistedRejectedTransactionReturnedWithoutSettlement() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal excessAmount = aliceBefore.add(new BigDecimal("500.00"));

        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", excessAmount, "1234", 5);

        // 1. Initial ingestion rejected due to insufficient balance
        BridgeIngestionService.IngestResult firstResult = bridge.ingest(packet, "bridge-1", 1);
        assertEquals("REJECTED", firstResult.outcome());
        Long txId = firstResult.transactionId();

        // 2. Alice's balance is increased later
        Account alice = accounts.findById("alice@demo").orElseThrow();
        alice.setBalance(new BigDecimal("20000.00"));
        accounts.save(alice);

        // 3. Clear in-memory cache to force durable lookup from database
        idempotency.clear();

        // 4. Second ingestion with the same rejected packet
        BridgeIngestionService.IngestResult secondResult = bridge.ingest(packet, "bridge-2", 2);
        assertEquals("REJECTED", secondResult.outcome());
        assertEquals(txId, secondResult.transactionId());

        // 5. Balance must remain 20000.00 (not debited by the old rejected packet)
        BigDecimal aliceCurrent = accounts.findById("alice@demo").orElseThrow().getBalance();
        assertEquals(0, new BigDecimal("20000.00").compareTo(aliceCurrent));
    }

    // =========================================================================
    // 5. Transaction Rollback
    // =========================================================================

    @Test
    void settlementRollsBackWhenLedgerWriteFails() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobBefore = accounts.findById("bob@demo").orElseThrow().getBalance();
        BigDecimal sendAmount = new BigDecimal("50.00");

        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", sendAmount, "1234", 5);
        String packetHash = crypto.hashCiphertext(packet.getCiphertext());

        // Simulate failure during credit ledger entry save
        doThrow(new DataIntegrityViolationException("Simulated credit ledger write failure"))
                .when(ledgerEntries).save(argThat(entry -> entry != null && entry.getEntryType() == LedgerEntry.EntryType.CREDIT));

        try {
            BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-fail", 1);
            assertEquals("INVALID", r.outcome(), "Outcome should be INVALID due to ledger write failure");
            assertEquals("data_integrity_violation", r.reason());

            // Balances remain unchanged after rollback
            BigDecimal aliceAfter = accounts.findById("alice@demo").orElseThrow().getBalance();
            BigDecimal bobAfter = accounts.findById("bob@demo").orElseThrow().getBalance();
            assertEquals(0, aliceBefore.compareTo(aliceAfter), "Sender balance must remain unchanged after rollback");
            assertEquals(0, bobBefore.compareTo(bobAfter), "Receiver balance must remain unchanged after rollback");

            // No partial ledger entries or transaction records remain
            List<LedgerEntry> entries = ledgerEntries.findByPacketHash(packetHash);
            assertTrue(entries.isEmpty(), "No partial ledger entries must remain after rollback");
            Optional<Transaction> txOpt = transactions.findByPacketHash(packetHash);
            assertTrue(txOpt.isEmpty(), "No transaction record must remain after rollback");

        } finally {
            reset(ledgerEntries);
        }
    }

    // =========================================================================
    // 6. Concurrent Settlement & Optimistic Locking
    // =========================================================================

    @Test
    void concurrentPaymentsFromSameSenderBothSettleWithoutLoss() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobBefore = accounts.findById("bob@demo").orElseThrow().getBalance();
        BigDecimal carolBefore = accounts.findById("carol@demo").orElseThrow().getBalance();

        BigDecimal amount1 = new BigDecimal("75.00");
        BigDecimal amount2 = new BigDecimal("125.00");

        MeshPacket packet1 = demoService.createPacket("alice@demo", "bob@demo", amount1, "1234", 5);
        MeshPacket packet2 = demoService.createPacket("alice@demo", "carol@demo", amount2, "1234", 5);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        Future<BridgeIngestionService.IngestResult> future1 = executor.submit(() -> {
            startLatch.await();
            return bridge.ingest(packet1, "bridge-1", 1);
        });

        Future<BridgeIngestionService.IngestResult> future2 = executor.submit(() -> {
            startLatch.await();
            return bridge.ingest(packet2, "bridge-2", 1);
        });

        startLatch.countDown(); // release both threads simultaneously to trigger optimistic lock conflict

        BridgeIngestionService.IngestResult r1 = future1.get(5, TimeUnit.SECONDS);
        BridgeIngestionService.IngestResult r2 = future2.get(5, TimeUnit.SECONDS);
        executor.shutdown();

        assertEquals("SETTLED", r1.outcome(), "Payment 1 should settle");
        assertEquals("SETTLED", r2.outcome(), "Payment 2 should settle via optimistic lock retry");

        BigDecimal aliceAfter = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobAfter = accounts.findById("bob@demo").orElseThrow().getBalance();
        BigDecimal carolAfter = accounts.findById("carol@demo").orElseThrow().getBalance();

        assertEquals(0, aliceBefore.subtract(amount1).subtract(amount2).compareTo(aliceAfter), "Alice should be debited for both payments");
        assertEquals(0, bobBefore.add(amount1).compareTo(bobAfter), "Bob should be credited for payment 1");
        assertEquals(0, carolBefore.add(amount2).compareTo(carolAfter), "Carol should be credited for payment 2");
    }

    @Test
    void insufficientBalanceIsNotRetried() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal excessAmount = aliceBefore.add(new BigDecimal("999.00"));

        long start = System.currentTimeMillis();
        MeshPacket packet = demoService.createPacket("alice@demo", "bob@demo", excessAmount, "1234", 5);
        BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-1", 1);
        long duration = System.currentTimeMillis() - start;

        assertEquals("REJECTED", r.outcome());
        assertEquals("insufficient_balance", r.reason());
        assertTrue(duration < 1000, "Insufficient balance should reject immediately without retry delay");

        assertNotNull(r.transactionId());
        Transaction tx = transactions.findById(r.transactionId()).orElseThrow();
        assertEquals(Transaction.Status.REJECTED, tx.getStatus());
    }

    // =========================================================================
    // 7. Cryptography & Tamper Detection
    // =========================================================================

    @Test
    void tamperedCiphertextIsRejected() throws Exception {
        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", new BigDecimal("50.00"), "1234", 5);

        // Flip a character in the ciphertext
        char[] chars = packet.getCiphertext().toCharArray();
        chars[chars.length / 2] = chars[chars.length / 2] == 'A' ? 'B' : 'A';
        packet.setCiphertext(new String(chars));

        BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-x", 1);
        assertEquals("INVALID", r.outcome());
        assertEquals("decryption_failed", r.reason());
    }

    @Test
    void encryptDecryptRoundTrip() throws Exception {
        PaymentInstruction original = new PaymentInstruction(
                "alice@demo", "bob@demo", new BigDecimal("123.45"),
                "abcdef", "nonce-1", System.currentTimeMillis());

        String ct = crypto.encrypt(original, serverKey.getPublicKey());
        PaymentInstruction decrypted = crypto.decrypt(ct);

        assertEquals(original.getSenderVpa(), decrypted.getSenderVpa());
        assertEquals(original.getReceiverVpa(), decrypted.getReceiverVpa());
        assertEquals(0, original.getAmount().compareTo(decrypted.getAmount()));
        assertEquals(original.getNonce(), decrypted.getNonce());
    }

    // =========================================================================
    // 8. Request Validation
    // =========================================================================

    @Test
    void missingOrBlankRequiredPaymentFieldsRejected() throws Exception {
        // Missing senderVpa
        ApiController.DemoSendRequest missingSender = new ApiController.DemoSendRequest();
        missingSender.receiverVpa = "bob@demo";
        missingSender.amount = new BigDecimal("100.00");
        missingSender.pin = "1234";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(missingSender)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation failed"))
                .andExpect(jsonPath("$.errors.senderVpa").value("Sender VPA is required"));

        // Missing receiverVpa
        ApiController.DemoSendRequest missingReceiver = new ApiController.DemoSendRequest();
        missingReceiver.senderVpa = "alice@demo";
        missingReceiver.amount = new BigDecimal("100.00");
        missingReceiver.pin = "1234";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(missingReceiver)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation failed"))
                .andExpect(jsonPath("$.errors.receiverVpa").value("Receiver VPA is required"));

        // Missing amount
        ApiController.DemoSendRequest missingAmount = new ApiController.DemoSendRequest();
        missingAmount.senderVpa = "alice@demo";
        missingAmount.receiverVpa = "bob@demo";
        missingAmount.pin = "1234";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(missingAmount)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation failed"))
                .andExpect(jsonPath("$.errors.amount").value("Amount is required"));

        // Missing PIN
        ApiController.DemoSendRequest missingPin = new ApiController.DemoSendRequest();
        missingPin.senderVpa = "alice@demo";
        missingPin.receiverVpa = "bob@demo";
        missingPin.amount = new BigDecimal("100.00");

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(missingPin)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation failed"))
                .andExpect(jsonPath("$.errors.pin").value("PIN is required"));
    }

    @Test
    void invalidPinOrAmountRejected() throws Exception {
        // Non-digit PIN
        ApiController.DemoSendRequest nonDigitsPin = new ApiController.DemoSendRequest();
        nonDigitsPin.senderVpa = "alice@demo";
        nonDigitsPin.receiverVpa = "bob@demo";
        nonDigitsPin.amount = new BigDecimal("100.00");
        nonDigitsPin.pin = "12ab";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(nonDigitsPin)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.pin").value("PIN must be 4 to 6 digits"));

        // Too short PIN (< 4 digits)
        ApiController.DemoSendRequest shortPin = new ApiController.DemoSendRequest();
        shortPin.senderVpa = "alice@demo";
        shortPin.receiverVpa = "bob@demo";
        shortPin.amount = new BigDecimal("100.00");
        shortPin.pin = "123";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(shortPin)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.pin").value("PIN must be 4 to 6 digits"));

        // Zero amount
        ApiController.DemoSendRequest zeroAmount = new ApiController.DemoSendRequest();
        zeroAmount.senderVpa = "alice@demo";
        zeroAmount.receiverVpa = "bob@demo";
        zeroAmount.amount = BigDecimal.ZERO;
        zeroAmount.pin = "1234";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(zeroAmount)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.amount").value("Amount must be greater than zero"));

        // Negative amount
        ApiController.DemoSendRequest negativeAmount = new ApiController.DemoSendRequest();
        negativeAmount.senderVpa = "alice@demo";
        negativeAmount.receiverVpa = "bob@demo";
        negativeAmount.amount = new BigDecimal("-50.00");
        negativeAmount.pin = "1234";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(negativeAmount)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.amount").value("Amount must be greater than zero"));
    }

    @Test
    void validPaymentRequestAccepted() throws Exception {
        // Valid 4-digit PIN request
        ApiController.DemoSendRequest req4 = new ApiController.DemoSendRequest();
        req4.senderVpa = "alice@demo";
        req4.receiverVpa = "bob@demo";
        req4.amount = new BigDecimal("75.00");
        req4.pin = "1234";
        req4.ttl = 5;
        req4.startDevice = "phone-alice";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req4)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.packetId").isNotEmpty())
                .andExpect(jsonPath("$.ciphertextPreview").isNotEmpty())
                .andExpect(jsonPath("$.ttl").value(5))
                .andExpect(jsonPath("$.injectedAt").value("phone-alice"));

        // Valid 6-digit PIN request
        ApiController.DemoSendRequest req6 = new ApiController.DemoSendRequest();
        req6.senderVpa = "alice@demo";
        req6.receiverVpa = "bob@demo";
        req6.amount = new BigDecimal("35.00");
        req6.pin = "654321";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req6)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.packetId").isNotEmpty());
    }

    // =========================================================================
    // 9. Demo State Reset
    // =========================================================================

    @Test
    void resetStateRestoresBalancesClearsTablesAndClearsMeshAndIdempotency() throws Exception {
        // 1. Perform a transaction that modifies state
        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", new BigDecimal("100.00"), "1234", 5);
        bridge.ingest(packet, "bridge-1", 1);

        // Inject packet into mesh and gossip
        mesh.inject("phone-alice", packet);
        mesh.gossipOnce();

        // Verify state is modified before reset
        BigDecimal aliceModified = accounts.findById("alice@demo").orElseThrow().getBalance();
        assertTrue(new BigDecimal("5000.00").compareTo(aliceModified) > 0);
        assertTrue(transactions.count() > 0, "Transactions should exist before reset");
        assertTrue(ledgerEntries.count() > 0, "Ledger entries should exist before reset");
        assertTrue(idempotency.size() > 0, "Idempotency claims should exist before reset");
        assertTrue(mesh.getDevice("phone-alice").packetCount() > 0, "Mesh device should hold packets");

        // 2. Call reset endpoint
        mockMvc.perform(post("/api/demo/reset"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"));

        // 3. Verify balances restored to configured initial amounts
        assertEquals(0, new BigDecimal("5000.00").compareTo(accounts.findById("alice@demo").orElseThrow().getBalance()));
        assertEquals(0, new BigDecimal("1000.00").compareTo(accounts.findById("bob@demo").orElseThrow().getBalance()));
        assertEquals(0, new BigDecimal("2500.00").compareTo(accounts.findById("carol@demo").orElseThrow().getBalance()));
        assertEquals(0, new BigDecimal("500.00").compareTo(accounts.findById("dave@demo").orElseThrow().getBalance()));

        // 4. Verify transaction and ledger tables are empty
        assertEquals(0, transactions.count(), "Transactions table must be empty after reset");
        assertEquals(0, ledgerEntries.count(), "Ledger table must be empty after reset");

        // 5. Verify idempotency claims cleared
        assertEquals(0, idempotency.size(), "Idempotency claims must be cleared after reset");

        // 6. Verify in-memory mesh device queues cleared
        for (VirtualDevice device : mesh.getDevices()) {
            assertEquals(0, device.packetCount(), "Device " + device.getDeviceId() + " must have 0 packets");
        }
    }

    @Test
    void oldPacketsCreatedBeforeResetArePreventedFromSettlingAfterReset() throws Exception {
        // 1. Create packet before reset
        MeshPacket oldPacket = demoService.createPacket(
                "alice@demo", "bob@demo", new BigDecimal("100.00"), "1234", 5);

        // 2. Reset system state
        demoService.resetAllState();

        // 3. Ingest old packet after reset
        BridgeIngestionService.IngestResult rOld = bridge.ingest(oldPacket, "bridge-old", 1);
        assertEquals("INVALID", rOld.outcome(), "Old packet must be rejected after reset");
        assertEquals("stale_packet", rOld.reason());

        // Balances and tables remain unchanged
        assertEquals(0, new BigDecimal("5000.00").compareTo(accounts.findById("alice@demo").orElseThrow().getBalance()));
        assertEquals(0, new BigDecimal("1000.00").compareTo(accounts.findById("bob@demo").orElseThrow().getBalance()));
        assertEquals(0, transactions.count());
        assertEquals(0, ledgerEntries.count());

        // 4. Create and ingest fresh packet after reset
        MeshPacket newPacket = demoService.createPacket(
                "alice@demo", "bob@demo", new BigDecimal("100.00"), "1234", 5);
        BridgeIngestionService.IngestResult rNew = bridge.ingest(newPacket, "bridge-new", 1);
        assertEquals("SETTLED", rNew.outcome(), "New packet created after reset should settle normally");

        assertEquals(0, new BigDecimal("4900.00").compareTo(accounts.findById("alice@demo").orElseThrow().getBalance()));
        assertEquals(0, new BigDecimal("1100.00").compareTo(accounts.findById("bob@demo").orElseThrow().getBalance()));
        assertEquals(1, transactions.count());
        assertEquals(2, ledgerEntries.count());
    }
}
