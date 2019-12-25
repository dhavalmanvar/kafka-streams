package com.github.dhavalmanvar.kafka.dto;

import java.io.Serializable;

public class BankTransactionDTO implements Serializable {

    private String userName;

    private Integer amount;

    private String time;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "BankTransactionDTO{" +
                "userName='" + userName + '\'' +
                ", amount=" + amount +
                ", time='" + time + '\'' +
                '}';
    }
}
