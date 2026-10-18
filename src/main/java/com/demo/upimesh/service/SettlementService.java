package com.demo.upimesh.service;

import com.demo.upimesh.model.Account;
import com.demo.upimesh.model.AccountRepository;
import com.demo.upimesh.model.LedgerEntryRepository;
import com.demo.upimesh.model.PaymentInstruction;
import com.demo.upimesh.model.Transaction;
import com.demo.upimesh.model.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.demo.upimesh.model.LedgerEntry;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Where the actual ledger update happens. Wrapped in a DB transaction so either
 * BOTH the debit and credit happen, or neither does.
 *
 * The @Version column on Account gives us optimistic locking — if two threads
 * somehow get past idempotency and both try to debit the same account, the
 * second one will fail with OptimisticLockException rather than corrupting
 * the balance. (In a demo the idempotency layer should always catch this first,
 * but defense in depth.)
 */
@Service
public class SettlementService {

    private static final Logger log = LoggerFactory.getLogger(SettlementService.class);

    @Autowired
    private AccountRepository accounts;
    @Autowired
    private TransactionRepository transactions;
    @Autowired
    private LedgerEntryRepository ledgerEntries;

    @Transactional
    public Transaction settle(PaymentInstruction instruction, String packetHash,
            String bridgeNodeId, int hopCount) {

        String senderVpa = instruction.getSenderVpa();
        if (senderVpa == null || senderVpa.isBlank()) {
            throw new IllegalArgumentException("Sender VPA is required");
        }
        String receiverVpa = instruction.getReceiverVpa();
        if (receiverVpa == null || receiverVpa.isBlank()) {
            throw new IllegalArgumentException("Receiver VPA is required");
        }

        BigDecimal amount = instruction.getAmount();
        if (amount == null || amount.signum() <= 0) {
            throw new IllegalArgumentException("Amount must be positive");
        }

        Account sender = accounts.findById(senderVpa)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown sender VPA: " + senderVpa));

        Account receiver = accounts.findById(receiverVpa)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown receiver VPA: " + receiverVpa));

        if (sender.getVpa().equalsIgnoreCase(receiver.getVpa())) {
            log.warn("Self-transfer rejected: sender and receiver are the same account ({})", sender.getVpa());
            return recordRejected(instruction, packetHash, bridgeNodeId, hopCount, "self_transfer");
        }

        if (sender.getBalance().compareTo(amount) < 0) {
            log.warn("Insufficient balance: {} has ₹{}, tried to send ₹{}",
                    sender.getVpa(), sender.getBalance(), amount);
            return recordRejected(instruction, packetHash, bridgeNodeId, hopCount, "insufficient_balance");
        }

        sender.setBalance(sender.getBalance().subtract(amount));
        receiver.setBalance(receiver.getBalance().add(amount));
        accounts.save(sender);
        accounts.save(receiver);

        LedgerEntry debit = new LedgerEntry();
        debit.setPacketHash(packetHash);
        debit.setAccountVpa(sender.getVpa());
        debit.setEntryType(LedgerEntry.EntryType.DEBIT);
        debit.setAmount(amount);
        debit.setCreatedAt(Instant.now());

        LedgerEntry credit = new LedgerEntry();
        credit.setPacketHash(packetHash);
        credit.setAccountVpa(receiver.getVpa());
        credit.setEntryType(LedgerEntry.EntryType.CREDIT);
        credit.setAmount(amount);
        credit.setCreatedAt(Instant.now());

        ledgerEntries.save(debit);
        ledgerEntries.save(credit);

        Transaction tx = new Transaction();
        tx.setPacketHash(packetHash);
        tx.setSenderVpa(instruction.getSenderVpa());
        tx.setReceiverVpa(instruction.getReceiverVpa());
        tx.setAmount(amount);
        tx.setSignedAt(instruction.getSignedAt() != null ? Instant.ofEpochMilli(instruction.getSignedAt()) : Instant.now());
        tx.setSettledAt(Instant.now());
        tx.setBridgeNodeId(bridgeNodeId);
        tx.setHopCount(hopCount);
        tx.setStatus(Transaction.Status.SETTLED);
        transactions.save(tx);

        log.info("SETTLED ₹{} from {} to {} (packetHash={}, bridge={}, hops={})",
                amount, sender.getVpa(), receiver.getVpa(),
                packetHash.substring(0, 12) + "...", bridgeNodeId, hopCount);

        return tx;
    }

    private Transaction recordRejected(PaymentInstruction instruction, String packetHash,
            String bridgeNodeId, int hopCount, String reason) {
        Transaction tx = new Transaction();
        tx.setPacketHash(packetHash);
        tx.setSenderVpa(instruction.getSenderVpa());
        tx.setReceiverVpa(instruction.getReceiverVpa());
        tx.setAmount(instruction.getAmount());
        tx.setSignedAt(instruction.getSignedAt() != null ? Instant.ofEpochMilli(instruction.getSignedAt()) : Instant.now());
        tx.setSettledAt(Instant.now());
        tx.setBridgeNodeId(bridgeNodeId);
        tx.setHopCount(hopCount);
        tx.setStatus(Transaction.Status.REJECTED);
        tx.setRejectionReason(reason);
        return transactions.save(tx);
    }
}
