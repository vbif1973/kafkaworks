package com.demo.engine;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

public class ConsumerThread extends Thread {
    private static final String TOPIC = "testpartitions";
    private String topicName;
    private String groupId;
    private long startingOffset;
    private KafkaConsumer<String, String> kafkaConsumer;

    public ConsumerThread(String topicName, String groupId, long startingOffset) {
        this.topicName = topicName;
        this.groupId = groupId;
        this.startingOffset = startingOffset;
    }

//    public static void main(String[] args) {
//        ConsumerThread consumerThread = new ConsumerThread(TOPIC, "cliettwopartitions", 0l);
//        consumerThread.start();
//    }

    public void run() {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "offset123");
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        //Figure out where to start processing messages from
        kafkaConsumer = new KafkaConsumer<String, String>(configProperties);

        kafkaConsumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.printf("%s topic-partitions are revoked from this consumer\n", Arrays.toString(partitions.toArray()));
            }

            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.printf("%s topic-partitions are assigned to this consumer\n", Arrays.toString(partitions.toArray()));
                Iterator<TopicPartition> topicPartitionIterator = partitions.iterator();
                while (topicPartitionIterator.hasNext()) {
                    TopicPartition topicPartition = topicPartitionIterator.next();
                    System.out.println("Current offset is " + kafkaConsumer.position(topicPartition) + " committed offset is ->" +
                            kafkaConsumer.committed(topicPartition));
                    if (startingOffset == 0) {
                        System.out.println("Setting offset to beginning");
                        kafkaConsumer.seekToBeginning(partitions);
                        System.out.println("Customer current position is " + kafkaConsumer.position(topicPartition));
                        System.out.println("Customer current partition is: " + topicPartition);
                    } else if (startingOffset == -1) {
                        System.out.println("Setting it to the end ");
                        kafkaConsumer.seekToEnd(partitions);
                        System.out.println("Customer current position is " + kafkaConsumer.position(topicPartition));
                        System.out.println("Customer current partition is: " + topicPartition);
                    } else {
                        System.out.println("Resetting offset to " + startingOffset);
                        kafkaConsumer.seek(topicPartition, startingOffset);
                        System.out.println("Customer current position is " + kafkaConsumer.position(topicPartition));
                        System.out.println("Customer current partition is: " + topicPartition);
                    }
                }
            }
        });
        //Start processing messages
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }

            }
        }catch(WakeupException ex){
            System.out.println("Exception caught " + ex.getMessage());
        }finally{
            kafkaConsumer.close();
            System.out.println("After closing KafkaConsumer");
        }
    }

    public KafkaConsumer<String, String> getKafkaConsumer() {
        return this.kafkaConsumer;
    }
}
