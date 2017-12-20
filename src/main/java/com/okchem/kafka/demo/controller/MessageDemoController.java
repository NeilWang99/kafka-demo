package com.okchem.kafka.demo.controller;

import com.okchem.kafka.demo.service.MessageDemoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Neil Wang 2017-12-18
 */
@RestController
@RequestMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE)
public class MessageDemoController {

    public static Logger logger = LoggerFactory.getLogger(MessageDemoController.class);

    @Autowired
    private MessageDemoService messageDemoService;

    @RequestMapping(value = "/send/async", method = RequestMethod.GET)
    public ResponseEntity<String> asyncSend() {
        messageDemoService.aSyncSend();
        return ResponseEntity.ok("success");
    }

    @RequestMapping(value = "/send/sync", method = RequestMethod.GET)
    public ResponseEntity<String> syncSend() {
        messageDemoService.syncSend();
        return ResponseEntity.ok("success");
    }

    @RequestMapping(value = "/send/transaction/success", method = RequestMethod.GET)
    public ResponseEntity<String> transactionSendSuccess() {
        messageDemoService.transactionSendSuccess();
        return ResponseEntity.ok("success");
    }

    @RequestMapping(value = "/send/transaction/fail", method = RequestMethod.GET)
    public ResponseEntity<String> transactionSendFail(){
        try {
            messageDemoService.transactionSendFail();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return ResponseEntity.ok("success");
    }

    @RequestMapping(value = "/send/transaction/fail-1", method = RequestMethod.GET)
    public ResponseEntity<String> transactionSendFail1(){
        try {
            messageDemoService.transactionSendFail1();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return ResponseEntity.ok("success");
    }

}
