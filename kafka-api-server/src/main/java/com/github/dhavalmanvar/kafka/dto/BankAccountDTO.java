package com.github.dhavalmanvar.kafka.dto;

import java.io.Serializable;

public class BankAccountDTO implements Serializable {

    private String userName;

    private Integer maxTransactionAmount;

    private Long transactionFrequency;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getMaxTransactionAmount() {
        return maxTransactionAmount;
    }

    public void setMaxTransactionAmount(Integer maxTransactionAmount) {
        this.maxTransactionAmount = maxTransactionAmount;
    }

    public Long getTransactionFrequency() {
        return transactionFrequency;
    }

    public void setTransactionFrequency(Long transactionFrequency) {
        this.transactionFrequency = transactionFrequency;
    }

    @Override
    public String toString() {
        return "BankAccountDTO{" +
                "userName='" + userName + '\'' +
                ", maxTransactionAmount=" + maxTransactionAmount +
                ", transactionFrequency=" + transactionFrequency +
                '}';
    }
}
