package com.okchem.kafka.demo;

import com.okchem.kafka.demo.service.ConsumerListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionInitialOffset;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Neil Wang 2017-12-07
 */
@SpringBootApplication
public class Application {

    public static Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }


    @KafkaListener(topics = "myTopic")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        logger.info("##########Listener Message: " + cr.toString());
    }

    @KafkaListener(id = "kafkaDemoConsume0", topicPartitions =
            {@TopicPartition(topic = "kafka-demo-topic",
                    partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "1"))
            })
    public void listenTestTransaction0(ConsumerRecord<?, ?> cr) throws Exception {
        logger.info("##########Listener Test Transaction Message[P0]: " + cr.toString());
    }

    @KafkaListener(id = "kafkaDemoConsume1", topicPartitions =
            {@TopicPartition(topic = "kafka-demo-topic", partitions = {"1"})
            })
    public void listenTestTransaction1(ConsumerRecord<?, ?> cr) throws Exception {
        logger.info("##########Listener Test Transaction Message[P1]: " + cr.toString());
    }

    //#################################
    private ConcurrentMessageListenerContainer concurrentMessageListenerContainer ;
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    public Map<String, Object> consumerConfigs() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.14:9092");
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "okchem-group1");
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return propsMap;
    }

    @PostConstruct
    public void init() {
        logger.info("Init message listener container...");
        TopicPartitionInitialOffset topicPartitionInitialOffset = new TopicPartitionInitialOffset("myTopic", 0, 5L);
        ContainerProperties containerProps = new ContainerProperties(topicPartitionInitialOffset);
        containerProps.setMessageListener(new ConsumerListener());
        concurrentMessageListenerContainer = new ConcurrentMessageListenerContainer(consumerFactory(), containerProps);
        concurrentMessageListenerContainer.start();
        logger.info("Init message listener container done");
    }

    @PreDestroy
    public void close() {
        logger.info("Stop message listener container...");
        if(concurrentMessageListenerContainer != null) {
            concurrentMessageListenerContainer.stop();
        }
        logger.info("Stop message listener container...");
    }

}
