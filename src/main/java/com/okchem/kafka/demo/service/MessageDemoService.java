package com.okchem.kafka.demo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Neil Wang 2017-12-14
 */
@Service
public class MessageDemoService {

    public static Logger logger = LoggerFactory.getLogger(MessageDemoService.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Testing send by Async model
     */
    @Transactional
    public void aSyncSend() {
        logger.info("ASync send...");
        final String topic = "myTopic";
        final String key = "1";
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, "payload value[1]" + System.currentTimeMillis());

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                logger.info("ProducerRecord:" + result.getProducerRecord().toString());
                logger.info("RecordMetadata:" + result.getRecordMetadata());
            }

            @Override
            public void onFailure(Throwable ex) {
                logger.error(ex.getMessage(), ex);
            }

        });
        logger.info("ASync sent");
    }

    /**
     * Testing send by sync model
     */
    @Transactional
    public void syncSend() {
        logger.info("Sync send...");
        final String topic = "kafka-demo-topic";
        final String key = "2";
        try {
            kafkaTemplate.send(topic, key, "payload value[2]" + System.currentTimeMillis()).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        } catch (ExecutionException e) {
            logger.error(e.getMessage(), e);
        } catch (TimeoutException e) {
            logger.error(e.getMessage(), e);
        }
        logger.info("Sync sent");
    }

    @Transactional
    public void transactionSendSuccess() {
        logger.info("Transaction send...");
        final String topic = "kafka-demo-topic";
        final String key = "1";
        kafkaTemplate.send(topic, 0, "1", "payload value transaction test1[1]" + System.currentTimeMillis());
        kafkaTemplate.send(topic, 1, "2", "payload value transaction test2[1]" + System.currentTimeMillis());
        logger.info("Transaction sent");
    }

    public void transactionSendFail() throws Exception {
        logger.info("Transaction send...");
        final String topic = "kafka-demo-topic";
        final String key = "1";
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        kafkaTemplate.executeInTransaction(t -> {
            t.send(topic, key, "payload value transaction fail test1[1]" + sdf.format(new Date(System.currentTimeMillis())));
            if ("1".equals(key)) {
                throw new RuntimeException("transaction expception");
            }
            t.send(topic, key, "payload value transaction fail test2[]" + sdf.format(new Date(System.currentTimeMillis())));
            return null;
        });
        logger.info("Transaction sent");
    }

    @Transactional
    public void transactionSendFail1() {
        logger.info("Transaction send...");
        final String topic = "kafka-demo-topic";
        final String key = "2";
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        kafkaTemplate.send(topic, key, "payload value transaction fail[1] test1[1]" + sdf.format(new Date(System.currentTimeMillis())));
        if ("2".equals(key)) {
            throw new RuntimeException("transaction expception");
        }
        kafkaTemplate.send(topic, key, "payload value transaction fail[1] test2[]" + sdf.format(new Date(System.currentTimeMillis())));
        logger.info("Transaction sent");
    }

}
