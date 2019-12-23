package com.github.dhavalmanvar.kafka.controllers;

import com.github.dhavalmanvar.kafka.dto.MessageDTO;
import com.github.dhavalmanvar.kafka.dto.ProducerDTO;
import com.github.dhavalmanvar.kafka.services.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/producer")
public class ProducerController {

    private ProducerService producerService;

    @Autowired
    public ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<ProducerDTO> getProducers() {
        return producerService.getProducers();
    }

    @PutMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public void updateProducer(@RequestBody ProducerDTO producerDTO) {
        this.producerService.updateProducer(producerDTO);
    }

    @DeleteMapping(value = "/{id}")
    public void deleteProducer(@PathVariable String id) {
        this.producerService.deleteProducer(id);
    }

    @PostMapping(value="/message", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void updateProducer(@RequestBody MessageDTO messageDTO) {
        this.producerService.produceMessage(messageDTO);
    }

}
