package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.entity.TransactionRecord;
import org.springframework.stereotype.Component;

@Component
public class DatabaseConduit {
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public DatabaseConduit(UserRepository userRepository,TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }
    public void save(TransactionRecord TransactionRecord) {
        transactionRepository.save(TransactionRecord);
    }
    public UserRecord queryUser(long id) {
        return userRepository.findById(id);
    }

    public UserRecord queryUser(String name) {
        return userRepository.findByName(name);
    }
   


}

