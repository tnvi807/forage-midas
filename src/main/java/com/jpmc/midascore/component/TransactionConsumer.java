package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    public TransactionConsumer(DatabaseConduit databaseConduit, RestTemplate restTemplate) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(
        id = "TransactionConsumer",
        topics = "${general.kafka-topic}"
    )
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);
        UserRecord sender = databaseConduit.queryUser(transaction.getSenderId());
        UserRecord recipient = databaseConduit.queryUser(transaction.getRecipientId());

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            Incentive incentive = restTemplate.postForObject("http://localhost:8080/incentive", transaction, Incentive.class);
            float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0.0f;

            logger.info("Processed: {} (sender) -> {} (recipient) | Amount: {} | Incentive: {}", 
                sender.getName(), recipient.getName(), transaction.getAmount(), incentiveAmount);

            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

            databaseConduit.save(sender);
            databaseConduit.save(recipient);

            TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
            databaseConduit.save(transactionRecord);
        } else {
            logger.info("Invalid transaction discarded: {}", transaction);
        }
    }
}
