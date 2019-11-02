package life.jugnu.learnkafka.ch04;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class FirstConsumer {
    public static void main(String[] args) {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("group.id", "FirstConsumer");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> c = new KafkaConsumer<String, String>(p);
        c.subscribe(Collections.singletonList("firsttopic"));
        try {
            while (true) {
                ConsumerRecords<String, String> rec = c.poll(10000);
                System.out.println("We got record count " + rec.count());
                for (ConsumerRecord<String, String> r : rec) {
                    System.out.printf("topic = %s, partition = %s, offset = %s, key = %s, value = %s\n",
                            r.topic(), r.partition(), r.offset(), r.key(), r.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            c.close();
        }
    }
}
