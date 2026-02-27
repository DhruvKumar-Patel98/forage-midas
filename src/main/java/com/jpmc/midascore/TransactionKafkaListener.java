package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.service.IncentiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class TransactionKafkaListener {

    private static final Logger logger =
            LoggerFactory.getLogger(TransactionKafkaListener.class);

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final IncentiveService incentiveService;

    private int transactionCount = 0;
    private final float[] firstFourAmounts = new float[4];

    public TransactionKafkaListener(UserRepository userRepository,
                                    TransactionRecordRepository transactionRecordRepository,
                                    IncentiveService incentiveService) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.incentiveService = incentiveService;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {

        transactionCount++;
        logger.info("Received transaction #{}: {}", transactionCount, transaction);

        // collect first 4 transaction amounts (for Task 2)
        if (transactionCount <= 4) {
            firstFourAmounts[transactionCount - 1] = transaction.getAmount();
        }

        // validate sender and recipient
        Optional<UserRecord> senderOpt =
                Optional.ofNullable(userRepository.findById(transaction.getSenderId()));
        Optional<UserRecord> recipientOpt =
                Optional.ofNullable(userRepository.findById(transaction.getRecipientId()));

        if (senderOpt.isEmpty() || recipientOpt.isEmpty()) {
            logger.warn("Invalid users — transaction discarded");
            return;
        }

        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        float amount = transaction.getAmount();

        // validate balance
        if (sender.getBalance() < amount) {
            logger.warn("Insufficient balance — transaction discarded");
            return;
        }

        // call Incentive API
        float incentiveAmount = incentiveService.getIncentive(transaction);
        logger.info("Incentive for this transaction: {}", incentiveAmount);

        // update balances
        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

        userRepository.save(sender);
        userRepository.save(recipient);

        // store transaction record including incentive
        TransactionRecord record =
                new TransactionRecord(amount, sender, recipient, incentiveAmount);

        transactionRecordRepository.save(record);
    }
}