/*
Created 2/8/2018
Class that reads from a file and sends data to Kafka as a proucer.
Made to learn Kafka
 */

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class FileToKafka {
    public static void main(String[] args) {

        //Set up Kafka Producer
        Properties properties = new Properties();
        setProperties(properties);
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

        //Name of file to open
        String fileName = "src/main/Other Files/TestFile.txt";

        //Instantiate BufferedReader to read from file and send each line as a message to Kafka
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            String line;
            while((line = bufferedReader.readLine()) != null) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(KafkaProperties.TOPIC,
                        line);

                producer.send(producerRecord);
            }
        } catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");
        } catch(IOException ex) {
            System.out.println("Error reading from file '" + fileName + "'");
        }

        //Close producer
        producer.close();

    }

    private static void setProperties(Properties properties) {
        //Kafka Bootstrap Server
        properties.setProperty("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Producer acks
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        properties.setProperty("linger.ms", "1");
    }
}
