package com.github.dhavalmanvar.kafka.services;

import com.github.dhavalmanvar.kafka.DataType;
import com.github.dhavalmanvar.kafka.dto.BankAccountDTO;
import com.github.dhavalmanvar.kafka.dto.BankTransactionDTO;
import com.github.dhavalmanvar.kafka.serializer.KafkaCustomSerializer;
import com.github.dhavalmanvar.kafka.utils.KafkaProducerUtil;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

public class BankAccountTransactionProducer extends Thread {

    private static final String CLIENT_ID_PREFIX = "bankaccount";

    private static final Logger logger = LoggerFactory.getLogger(BankAccountTransactionProducer.class.getName());

    private final Gson gson = new Gson();

    private BankAccountDTO bankAccountDTO;

    private String bootstrapServers;

    private String topic;

    private SecureRandom random;

    private final SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private final long startTime = System.currentTimeMillis();

    private long transactionNumber = 1;

    private final AtomicBoolean updating = new AtomicBoolean(false);

    private final AtomicBoolean terminated = new AtomicBoolean(false);

    public BankAccountTransactionProducer(BankAccountDTO bankAccountDTO,
                                          String bootstrapServers,
                                          String topic) {
        this.bankAccountDTO = bankAccountDTO;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        random = new SecureRandom();
        random.setSeed(startTime);
    }

    @Override
    public void run() {
        KafkaProducer producer = KafkaProducerUtil.buildKafkaProducer(new StringSerializer(),
                new KafkaCustomSerializer(), bootstrapServers, getClientId(bankAccountDTO.getUserName()));

        try {
            while (!terminated.get()) {
                if(!updating.get()) {
                    try {
                        BankTransactionDTO transactionDTO = getNextTransaction();
                        ProducerRecord record = new ProducerRecord(topic, transactionDTO.getUserName(),
                                transactionDTO);

                        producer.send(record, (metadata, exception) -> {
                            if(exception != null) {
                                logger.error("BankAccountTransactionProducer.run ", exception);
                            }
                        });
                        if(bankAccountDTO.getTransactionFrequency() > 499) {
                            producer.flush();
                        }
                        transactionNumber++;
                        Thread.sleep(bankAccountDTO.getTransactionFrequency());
                    } catch (Exception ex) {
                        logger.error("BankAccountTransactionProducer.run ", ex);
                    }
                }
            }

        } finally {
            producer.flush();
            producer.close();
        }
    }

    public BankAccountDTO getBankAccountDTO() {
        return bankAccountDTO;
    }

    private BankTransactionDTO getNextTransaction() {
        BankTransactionDTO transaction = new BankTransactionDTO();
        transaction.setTime(dtf.format(new Date(startTime + transactionNumber * 1000)));
        transaction.setUserName(bankAccountDTO.getUserName());
        transaction.setAmount(random.nextInt(bankAccountDTO.getMaxTransactionAmount()) + 1);
        return  transaction;
    }

    private String getClientId(String userId) {
        return CLIENT_ID_PREFIX + "-" + userId.toLowerCase().replace(" ", "-");
    }

    public void updateAccount(BankAccountDTO bankAccountDTO) {
        this.updating.set(true);
        this.bankAccountDTO = bankAccountDTO;
        this.updating.set(false);
        this.interrupt();
    }

    public void terminate() {
        this.terminated.set(true);
        this.interrupt();
    }

}
