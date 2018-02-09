import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();

        //Kafka Bootstrap Server
        properties.setProperty("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", KafkaProperties.GROUP_ID);
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.intervals.ms", KafkaProperties.CONNECTION_TIMEOUT);
        properties.setProperty("auto.offset.reset", "earliest");

        //Instantiate Kafka Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(KafkaProperties.TOPIC));

        //Retrieve messages from topic
        while(true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println("Partition: " + consumerRecord.partition() + " Offset: " + consumerRecord.offset()
                + " Key: " + consumerRecord.partition() + " Value: " + consumerRecord.value());
            }
        }




    }
}
