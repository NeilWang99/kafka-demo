package com.okchem.kafka.demo.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * @author Neil Wang 2017-12-20
 */
public class ConsumerListener implements MessageListener<String, String> {

    public static Logger logger = LoggerFactory.getLogger(ConsumerListener.class);

    @Override
    public void onMessage(ConsumerRecord<String, String> data) {
        logger.info("##########ConsumerListener onMessage:" + data.toString());
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
        logger.info("##########ConsumerListener onMessage Acknowledgment:" + data.toString());
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> data, Consumer<?, ?> consumer) {
        logger.info("##########ConsumerListener onMessage Consumer:" + data.toString());
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        logger.info("##########ConsumerListener onMessage Acknowledgment Consumer:" + data.toString());
    }

}
