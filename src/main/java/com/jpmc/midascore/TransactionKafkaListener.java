package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
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

    private int transactionCount = 0;
    private final float[] firstFourAmounts = new float[4];

    public TransactionKafkaListener(UserRepository userRepository,
                                    TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {

        transactionCount++;
        logger.info("Received transaction #{}: {}", transactionCount, transaction);

        if (transactionCount <= 4) {
            firstFourAmounts[transactionCount - 1] = transaction.getAmount();
        }

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


        if (sender.getBalance() < amount) {
            logger.warn("Insufficient balance — transaction discarded");
            return;
        }


        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount);

        userRepository.save(sender);
        userRepository.save(recipient);


        TransactionRecord record =
                new TransactionRecord(amount, sender, recipient);

        transactionRecordRepository.save(record);
    }
}