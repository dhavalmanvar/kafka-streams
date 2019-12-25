package com.github.dhavalmanvar.kafka.controllers;

import com.github.dhavalmanvar.kafka.dto.BankAccountDTO;
import com.github.dhavalmanvar.kafka.services.BankAccountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/bank")
public class BankAccountController {

    private BankAccountService bankAccountService;

    @Autowired
    public BankAccountController(BankAccountService bankAccountService) {
        this.bankAccountService = bankAccountService;
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<BankAccountDTO> getAccounts() {
        return bankAccountService.getAccounts();
    }

    @PutMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public void updateAccount(@RequestBody BankAccountDTO bankAccountDTO) {
        bankAccountService.updateBankAccount(bankAccountDTO);
    }

    @DeleteMapping(value="/{userName}")
    public void deleteAccount(@PathVariable String userName) {
        bankAccountService.deleteAccount(userName);
    }

}
