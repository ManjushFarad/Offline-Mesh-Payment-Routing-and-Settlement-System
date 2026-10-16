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

/**
 * The killer test: simulates the "three bridges deliver at the same instant"
 * scenario the user explicitly cared about.
 */
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
    @Autowired private AccountRepository accounts;
    @Autowired private TransactionRepository transactions;
    @SpyBean private LedgerEntryRepository ledgerEntries;
    @Autowired private HybridCryptoService crypto;
    @Autowired private ServerKeyHolder serverKey;

    @BeforeEach
    void clear() {
        idempotency.clear();
        reset(ledgerEntries);
    }

    @Test
    void singlePacketDeliveredByThreeBridgesSettlesExactlyOnce() throws Exception {
        // Capture starting balances
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobBefore = accounts.findById("bob@demo").orElseThrow().getBalance();

        // One packet, but we'll deliver it from 3 "bridges" simultaneously
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

        start.countDown(); // release all 3 threads at once
        for (Future<?> f : futures) f.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(1, settled.get(), "exactly one bridge should settle");
        assertEquals(2, duplicates.get(), "the other two should be duplicates");

        // Balance moved exactly once
        BigDecimal aliceAfter = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobAfter = accounts.findById("bob@demo").orElseThrow().getBalance();
        assertEquals(aliceBefore.subtract(new BigDecimal("100.00")), aliceAfter);
        assertEquals(bobBefore.add(new BigDecimal("100.00")), bobAfter);
    }

    @Test
    void tamperedCiphertextIsRejected() throws Exception {
        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", new BigDecimal("50.00"), "1234", 5);

        // Flip a byte in the middle of the ciphertext
        char[] chars = packet.getCiphertext().toCharArray();
        chars[chars.length / 2] = chars[chars.length / 2] == 'A' ? 'B' : 'A';
        packet.setCiphertext(new String(chars));

        BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-x", 1);
        assertEquals("INVALID", r.outcome());
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

    @Test
    void insufficientBalanceReturnsRejectedOutcomeAndPersistsRecord() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobBefore = accounts.findById("bob@demo").orElseThrow().getBalance();

        // Dynamically compute an amount strictly greater than Alice's current balance
        BigDecimal excessAmount = aliceBefore.add(new BigDecimal("1000.00"));
        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", excessAmount, "1234", 5);

        BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-insufficient", 1);

        assertEquals("REJECTED", r.outcome(), "Ingestion should return outcome REJECTED");
        assertEquals("insufficient_balance", r.reason(), "Rejection reason should be 'insufficient_balance'");
        assertNotNull(r.transactionId(), "Transaction ID must be returned for persisted rejected transaction");

        Transaction tx = transactions.findById(r.transactionId()).orElse(null);
        assertNotNull(tx, "Persisted transaction record must exist in database");
        assertEquals(Transaction.Status.REJECTED, tx.getStatus(), "Transaction status must be REJECTED");
        assertEquals(0, excessAmount.compareTo(tx.getAmount()), "Persisted transaction amount should match");
        assertEquals("alice@demo", tx.getSenderVpa());
        assertEquals("bob@demo", tx.getReceiverVpa());

        BigDecimal aliceAfter = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobAfter = accounts.findById("bob@demo").orElseThrow().getBalance();
        assertEquals(0, aliceBefore.compareTo(aliceAfter), "Sender balance must remain unchanged");
        assertEquals(0, bobBefore.compareTo(bobAfter), "Receiver balance must remain unchanged");
    }

    @Test
    void successfulPaymentReturnsSettledOutcomeAndUpdatesBalances() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobBefore = accounts.findById("bob@demo").orElseThrow().getBalance();

        BigDecimal sendAmount = new BigDecimal("50.00");
        assertTrue(aliceBefore.compareTo(sendAmount) >= 0, "Alice must have sufficient balance for test");

        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", sendAmount, "1234", 5);

        BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-success", 2);

        assertEquals("SETTLED", r.outcome(), "Ingestion should return outcome SETTLED");
        assertNull(r.reason(), "Settled transaction should have no error reason");
        assertNotNull(r.transactionId(), "Transaction ID must be returned for settled transaction");

        Transaction tx = transactions.findById(r.transactionId()).orElse(null);
        assertNotNull(tx, "Persisted transaction record must exist in database");
        assertEquals(Transaction.Status.SETTLED, tx.getStatus(), "Transaction status must be SETTLED");
        assertEquals(0, sendAmount.compareTo(tx.getAmount()));
        assertEquals("alice@demo", tx.getSenderVpa());
        assertEquals("bob@demo", tx.getReceiverVpa());

        BigDecimal aliceAfter = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobAfter = accounts.findById("bob@demo").orElseThrow().getBalance();
        assertEquals(0, aliceBefore.subtract(sendAmount).compareTo(aliceAfter), "Sender balance should be debited");
        assertEquals(0, bobBefore.add(sendAmount).compareTo(bobAfter), "Receiver balance should be credited");
    }

    @Test
    void successfulPaymentCreatesBalancedLedgerEntries() throws Exception {
        BigDecimal sendAmount = new BigDecimal("50.00");
        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", sendAmount, "1234", 5);

        BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-ledger", 1);

        assertEquals("SETTLED", r.outcome(), "Payment outcome should be SETTLED");
        assertNotNull(r.packetHash(), "Packet hash must be present in result");

        List<LedgerEntry> entries = ledgerEntries.findByPacketHash(r.packetHash());
        assertEquals(2, entries.size(), "Should create exactly two ledger entries for the payment");

        LedgerEntry debit = entries.stream()
                .filter(e -> e.getEntryType() == LedgerEntry.EntryType.DEBIT)
                .findFirst()
                .orElse(null);

        LedgerEntry credit = entries.stream()
                .filter(e -> e.getEntryType() == LedgerEntry.EntryType.CREDIT)
                .findFirst()
                .orElse(null);

        assertNotNull(debit, "One entry must be DEBIT");
        assertNotNull(credit, "One entry must be CREDIT");

        assertEquals("alice@demo", debit.getAccountVpa(), "DEBIT entry must belong to sender (alice@demo)");
        assertEquals("bob@demo", credit.getAccountVpa(), "CREDIT entry must belong to receiver (bob@demo)");

        assertEquals(0, sendAmount.compareTo(debit.getAmount()), "DEBIT entry amount must equal 50.00");
        assertEquals(0, sendAmount.compareTo(credit.getAmount()), "CREDIT entry amount must equal 50.00");
    }

    @Test
    void rejectedPaymentCreatesNoLedgerEntries() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal excessAmount = aliceBefore.add(new BigDecimal("1000.00"));

        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", excessAmount, "1234", 5);

        BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-reject-ledger", 1);

        assertEquals("REJECTED", r.outcome(), "Payment outcome should be REJECTED");
        assertNotNull(r.packetHash(), "Packet hash must be present in result");

        List<LedgerEntry> entries = ledgerEntries.findByPacketHash(r.packetHash());
        assertTrue(entries.isEmpty(), "Rejected payment must create zero ledger entries");
        assertEquals(0, entries.size(), "Ledger entries list size should be 0");
    }

    @Test
    void settlementRollsBackWhenLedgerWriteFails() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobBefore = accounts.findById("bob@demo").orElseThrow().getBalance();

        BigDecimal sendAmount = new BigDecimal("50.00");
        MeshPacket packet = demoService.createPacket(
                "alice@demo", "bob@demo", sendAmount, "1234", 5);
        String packetHash = crypto.hashCiphertext(packet.getCiphertext());

        // Simulate failure during credit ledger entry save (after debit entry save was invoked)
        doThrow(new DataIntegrityViolationException("Simulated credit ledger write failure"))
                .when(ledgerEntries).save(argThat(entry -> entry != null && entry.getEntryType() == LedgerEntry.EntryType.CREDIT));

        try {
            BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-fail", 1);
            assertEquals("INVALID", r.outcome(), "Outcome should be INVALID due to ledger write failure");
            assertEquals("data_integrity_violation", r.reason());

            // 1. Sender balance is unchanged
            BigDecimal aliceAfter = accounts.findById("alice@demo").orElseThrow().getBalance();
            assertEquals(0, aliceBefore.compareTo(aliceAfter), "Sender balance must remain unchanged after rollback");

            // 2. Receiver balance is unchanged
            BigDecimal bobAfter = accounts.findById("bob@demo").orElseThrow().getBalance();
            assertEquals(0, bobBefore.compareTo(bobAfter), "Receiver balance must remain unchanged after rollback");

            // 3. No partial ledger entries remain
            List<LedgerEntry> entries = ledgerEntries.findByPacketHash(packetHash);
            assertTrue(entries.isEmpty(), "No partial ledger entries must remain after rollback");
            assertEquals(0, entries.size());

            // 4. No successful transaction record remains
            Optional<Transaction> txOpt = transactions.findByPacketHash(packetHash);
            assertTrue(txOpt.isEmpty(), "No transaction record must remain after rollback");

        } finally {
            reset(ledgerEntries);
        }
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
        assertEquals(0, aliceBefore.subtract(sendAmount).compareTo(aliceAfterFirst));
        assertEquals(0, bobBefore.add(sendAmount).compareTo(bobAfterFirst));

        // 2. Clear in-memory claim to force durable lookup from database
        idempotency.clear();

        // 3. Second ingestion with same packet
        BridgeIngestionService.IngestResult secondResult = bridge.ingest(packet, "bridge-2", 2);
        assertEquals("SETTLED", secondResult.outcome());
        assertEquals(txId, secondResult.transactionId(), "Should return the existing transaction ID");

        // 4. Balances must be untouched (no double-debit)
        BigDecimal aliceAfterSecond = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobAfterSecond = accounts.findById("bob@demo").orElseThrow().getBalance();
        assertEquals(0, aliceAfterFirst.compareTo(aliceAfterSecond), "Sender balance must not change on second ingestion");
        assertEquals(0, bobAfterFirst.compareTo(bobAfterSecond), "Receiver balance must not change on second ingestion");
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
        assertEquals("insufficient_balance", firstResult.reason());
        Long txId = firstResult.transactionId();
        assertNotNull(txId);

        // 2. Alice's balance is increased later (e.g. deposit 20,000)
        Account alice = accounts.findById("alice@demo").orElseThrow();
        alice.setBalance(new BigDecimal("20000.00"));
        accounts.save(alice);

        // 3. Clear in-memory cache to force durable lookup from database
        idempotency.clear();

        // 4. Second ingestion with the same rejected packet
        BridgeIngestionService.IngestResult secondResult = bridge.ingest(packet, "bridge-2", 2);
        assertEquals("REJECTED", secondResult.outcome());
        assertEquals("insufficient_balance", secondResult.reason());
        assertEquals(txId, secondResult.transactionId());

        // 5. Alice's balance must remain 20000.00 (not debited by the old rejected packet)
        BigDecimal aliceCurrent = accounts.findById("alice@demo").orElseThrow().getBalance();
        assertEquals(0, new BigDecimal("20000.00").compareTo(aliceCurrent));
    }

    @Test
    void concurrentPaymentsFromSameSenderBothSettleWithoutLoss() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();
        BigDecimal bobBefore = accounts.findById("bob@demo").orElseThrow().getBalance();
        BigDecimal carolBefore = accounts.findById("carol@demo").orElseThrow().getBalance();

        BigDecimal amount1 = new BigDecimal("75.00");
        BigDecimal amount2 = new BigDecimal("125.00");
        assertTrue(aliceBefore.compareTo(amount1.add(amount2)) >= 0, "Alice must have enough funds for both payments");

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

        // Verify only one transaction record is stored
        assertNotNull(r.transactionId());
        Transaction tx = transactions.findById(r.transactionId()).orElseThrow();
        assertEquals(Transaction.Status.REJECTED, tx.getStatus());
    }

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

        // Blank senderVpa
        ApiController.DemoSendRequest blankSender = new ApiController.DemoSendRequest();
        blankSender.senderVpa = "   ";
        blankSender.receiverVpa = "bob@demo";
        blankSender.amount = new BigDecimal("100.00");
        blankSender.pin = "1234";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(blankSender)))
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

        // Blank receiverVpa
        ApiController.DemoSendRequest blankReceiver = new ApiController.DemoSendRequest();
        blankReceiver.senderVpa = "alice@demo";
        blankReceiver.receiverVpa = "   ";
        blankReceiver.amount = new BigDecimal("100.00");
        blankReceiver.pin = "1234";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(blankReceiver)))
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

        // Blank PIN
        ApiController.DemoSendRequest blankPin = new ApiController.DemoSendRequest();
        blankPin.senderVpa = "alice@demo";
        blankPin.receiverVpa = "bob@demo";
        blankPin.amount = new BigDecimal("100.00");
        blankPin.pin = "   ";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(blankPin)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation failed"))
                .andExpect(jsonPath("$.errors.pin").isNotEmpty());
    }

    @Test
    void nonDigitOrInvalidLengthPinRejected() throws Exception {
        // Non-digits in PIN
        ApiController.DemoSendRequest nonDigitsPin = new ApiController.DemoSendRequest();
        nonDigitsPin.senderVpa = "alice@demo";
        nonDigitsPin.receiverVpa = "bob@demo";
        nonDigitsPin.amount = new BigDecimal("100.00");
        nonDigitsPin.pin = "12ab";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(nonDigitsPin)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation failed"))
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
                .andExpect(jsonPath("$.error").value("Validation failed"))
                .andExpect(jsonPath("$.errors.pin").value("PIN must be 4 to 6 digits"));

        // Too long PIN (> 6 digits)
        ApiController.DemoSendRequest longPin = new ApiController.DemoSendRequest();
        longPin.senderVpa = "alice@demo";
        longPin.receiverVpa = "bob@demo";
        longPin.amount = new BigDecimal("100.00");
        longPin.pin = "1234567";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(longPin)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation failed"))
                .andExpect(jsonPath("$.errors.pin").value("PIN must be 4 to 6 digits"));
    }

    @Test
    void zeroOrNegativePaymentAmountsRejected() throws Exception {
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
                .andExpect(jsonPath("$.error").value("Validation failed"))
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
                .andExpect(jsonPath("$.error").value("Validation failed"))
                .andExpect(jsonPath("$.errors.amount").value("Amount must be greater than zero"));
    }

    @Test
    void validPaymentRequestAccepted() throws Exception {
        ApiController.DemoSendRequest validRequest = new ApiController.DemoSendRequest();
        validRequest.senderVpa = "alice@demo";
        validRequest.receiverVpa = "bob@demo";
        validRequest.amount = new BigDecimal("75.00");
        validRequest.pin = "1234";
        validRequest.ttl = 5;
        validRequest.startDevice = "phone-alice";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(validRequest)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.packetId").isNotEmpty())
                .andExpect(jsonPath("$.ciphertextPreview").isNotEmpty())
                .andExpect(jsonPath("$.ttl").value(5))
                .andExpect(jsonPath("$.injectedAt").value("phone-alice"));
    }

    @Test
    void validFourAndSixDigitPinsAccepted() throws Exception {
        // Valid 4-digit PIN
        ApiController.DemoSendRequest valid4Digit = new ApiController.DemoSendRequest();
        valid4Digit.senderVpa = "alice@demo";
        valid4Digit.receiverVpa = "bob@demo";
        valid4Digit.amount = new BigDecimal("25.00");
        valid4Digit.pin = "1234";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(valid4Digit)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.packetId").isNotEmpty());

        // Valid 6-digit PIN
        ApiController.DemoSendRequest valid6Digit = new ApiController.DemoSendRequest();
        valid6Digit.senderVpa = "alice@demo";
        valid6Digit.receiverVpa = "bob@demo";
        valid6Digit.amount = new BigDecimal("35.00");
        valid6Digit.pin = "654321";

        mockMvc.perform(post("/api/demo/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(valid6Digit)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.packetId").isNotEmpty());
    }

    @Test
    void selfTransferIsRejected() throws Exception {
        MeshPacket packet = demoService.createPacket(
                "alice@demo", "alice@demo", new BigDecimal("50.00"), "1234", 5);

        BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-self-test", 1);

        assertEquals("REJECTED", r.outcome(), "Self-transfer should return outcome REJECTED");
        assertEquals("self_transfer", r.reason(), "Rejection reason should be 'self_transfer'");
        assertNotNull(r.transactionId(), "Transaction ID must be returned for persisted rejected self-transfer");

        Transaction tx = transactions.findById(r.transactionId()).orElse(null);
        assertNotNull(tx, "Persisted transaction record must exist");
        assertEquals(Transaction.Status.REJECTED, tx.getStatus(), "Transaction status must be REJECTED");
        assertEquals("self_transfer", tx.getRejectionReason(), "Transaction rejectionReason must be 'self_transfer'");
        assertEquals("alice@demo", tx.getSenderVpa());
        assertEquals("alice@demo", tx.getReceiverVpa());
        assertEquals(0, new BigDecimal("50.00").compareTo(tx.getAmount()));
    }

    @Test
    void selfTransferCreatesNoBalanceChangesAndNoLedgerEntries() throws Exception {
        BigDecimal aliceBefore = accounts.findById("alice@demo").orElseThrow().getBalance();

        MeshPacket packet = demoService.createPacket(
                "alice@demo", "alice@demo", new BigDecimal("150.00"), "1234", 5);

        BridgeIngestionService.IngestResult r = bridge.ingest(packet, "bridge-self-ledger", 1);

        assertEquals("REJECTED", r.outcome());
        assertEquals("self_transfer", r.reason());

        // 1. Account balance must remain completely unchanged
        BigDecimal aliceAfter = accounts.findById("alice@demo").orElseThrow().getBalance();
        assertEquals(0, aliceBefore.compareTo(aliceAfter), "Account balance must not change after self-transfer rejection");

        // 2. Zero debit or credit ledger entries must be created
        List<LedgerEntry> entries = ledgerEntries.findByPacketHash(r.packetHash());
        assertTrue(entries.isEmpty(), "Rejected self-transfer must not create any debit/credit ledger entries");
        assertEquals(0, entries.size(), "Ledger entry list size must be 0");
    }
}
