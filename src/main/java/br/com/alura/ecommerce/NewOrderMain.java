package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        produzirMsg();

    }

    private static void produzirMsg() throws InterruptedException, ExecutionException {
        var producer = new KafkaProducer<String, String>(properties());
        var value = "13121,42321,58783";
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
        producer.send(record, (data,ex) -> {
            if(ex != null){
                ex.getStackTrace();
                return;
            }
            System.out.println("Sucesso enviando " + data.topic() + ":::partition:" + data.partition() + "/offset:" + data.offset());
        }).get();
    }

    private static Properties properties(){
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
