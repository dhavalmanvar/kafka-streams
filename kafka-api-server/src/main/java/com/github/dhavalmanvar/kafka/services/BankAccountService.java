package com.github.dhavalmanvar.kafka.services;

import com.github.dhavalmanvar.kafka.dto.BankAccountDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class BankAccountService {

    private static final String topic = "bank-transactions";

    private ConfigManager configManager;

    private Map<String, BankAccountTransactionProducer> accounts = new HashMap<>();

    @Autowired
    public BankAccountService(ConfigManager configManager) {
        this.configManager = configManager;
    }

    public void updateBankAccount(BankAccountDTO bankAccountDTO) {
        BankAccountTransactionProducer producer = accounts.get(bankAccountDTO.getUserName());
        if(producer == null) {
            producer = new BankAccountTransactionProducer(bankAccountDTO,
                    configManager.getBootstrapServers(),topic);
            accounts.put(bankAccountDTO.getUserName(), producer);
            producer.start();
        } else {
            producer.updateAccount(bankAccountDTO);
        }
    }

    public void deleteAccount(String userName) {
        BankAccountTransactionProducer producer = accounts.remove(userName);
        if(producer != null) {
            producer.terminate();
        }
    }

    public List<BankAccountDTO> getAccounts() {
        List<BankAccountDTO> result = new ArrayList<>();
        accounts.forEach((key, value) -> result.add(value.getBankAccountDTO()));
        return result;
    }

}
