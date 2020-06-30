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

public class StringConsumer extends Thread {

    private static final String TOPIC = "testpartitions";
    private static final String GROUP_ID = "cliettwopartitions";
    private String topicName;
    private String groupId;
    private long endingOffset;
    private KafkaConsumer<String, String> kafkaConsumer;

    public StringConsumer(String topicName, String groupId, long endingOffset) {
        this.topicName = topicName;
        this.groupId = groupId;
        this.endingOffset = endingOffset;
    }

    public void run() {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "offset123");
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

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
                    System.out.println("Resetting offset to 0");
                    kafkaConsumer.seekToBeginning(partitions);
                    System.out.println("Customer current position is " + kafkaConsumer.position(topicPartition));
                    System.out.println("Customer current partition is: " + topicPartition);
                }
            }
        });

        //Start processing messages
        try {
            boolean keepOnReading = true;
            while (keepOnReading) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Key: "+record.key() + ", Value: " + record.value());
                    System.out.println("Partition: " + record.partition() + ", Offset: " + record.offset());

                    if(record.offset() == endingOffset) {
                        keepOnReading = false;
                        break;
                    }
                }
            }
        } catch(WakeupException ex){
            System.out.println("Exception caught " + ex.getMessage());
        } finally{
            kafkaConsumer.close();
            System.out.println("After closing KafkaConsumer");
        }
    }

    public static void main(String[] args) {
        StringConsumer stringConsumer = new StringConsumer(TOPIC, GROUP_ID, 4l);
        stringConsumer.start();
    }

}
