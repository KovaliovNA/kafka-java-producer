package producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Entry point for starting daemon. The first one arg must be a kafka topic name and the second one
 * must be a bootstrap.servers eg. sandbox-hdp.hortonworks.com:2181.
 *
 * @author Kovalev_Nikita1@epam.com
 */
@Slf4j
public class ProducerMain {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err
                    .println("The parameters of this application must be like: topic-name server:port!");
            return;
        }

        Producer.builder()
                .topicName(args[0])
                .producer(new AtomicReference<>(new KafkaProducer<>(generateProperties(args[1]))))
                .recordsCount(100000)
                .threadPoolSize(10)
                .build()
                .start();
    }

    private static Properties generateProperties(String server) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }
}