package life.jugnu.learnkafka.ch04;

import life.jugnu.learnkafka.ch03.customerSerializer.Customer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;

public class AvroConsumer {
    private static KafkaConsumer<String, Customer> c;
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    private static Thread mainThread = Thread.currentThread();

    private static class HandleRebalanced implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            //before rebalanced and after consumer stop reading messages
            System.out.println("Lost partitions in rebalanced. Committing current offset:" + currentOffsets);
            c.commitSync(currentOffsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            //after rebalanced and before consumer begin reading messages
        }
    }

    public static void main(String[] args) {
        Properties p = new Properties();

        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer_Avro");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put("schema.registry.url", "http://localhost:8081");

        c = new KafkaConsumer<>(p);

        //Cancel group and Rebalanced by customising partitions to consume
//        List<TopicPartition> partitions = new LinkedList<>();
//        List<PartitionInfo> partitionInfos = c.partitionsFor("Avro");
//        if (partitionInfos != null) {
//            for (PartitionInfo partition : partitionInfos) {
//                partitions.add(new TopicPartition(partition.topic(), partition.partition()));
//            }
//        }
//        c.assign(partitions);
//        c.seek(new TopicPartition("Avro", 0), 0);

        c.subscribe(Collections.singletonList("Avro"), new HandleRebalanced());
//        c.subscribe(Collections.singletonList("Avro"));

        //Elegant closure for consumer
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            public void run() {
//                System.out.println("Starting exit...");
//                c.wakeup();
//                try {
//                    mainThread.join();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });

        try {
            while (true) {
                ConsumerRecords<String, Customer> rec = c.poll(Duration.ofSeconds(5));
                System.out.println("We got record count " + rec.count());
                for (ConsumerRecord<String, Customer> r : rec) {
                    System.out.printf("topic = %s, partition = %s, offset = %d," +
                                    "consumer = %s, country = %s\n",
                            r.topic(), r.partition(), r.offset(), r.key(), r.value());
                    currentOffsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1, "no metadata"));
                }
                c.commitAsync(currentOffsets, null);
            }
        } catch (WakeupException e) {

        } catch (Exception e) {
            e.printStackTrace(System.err);
        } finally {
            try {
                c.commitSync(currentOffsets);
            } finally {
                c.close();
                System.out.println("Closed consumer and we are done");
            }
        }
    }
}
