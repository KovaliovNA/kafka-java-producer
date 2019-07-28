package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Properties;

public class ConsumerMain {

    /**
     * Entry point for running {@link Consumer}. This consumer works without spark.
     *
     * @param args must contains topic-name server:port hdfs-host:19000 /hdfs/path/file.ext.
     * @throws IOException in case some error while saving data into hdfs.
     */
    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length != 4) {
            System.err
                    .println("The parameters of this application must be like: " +
                            "topic-name server:port hdfs-host:19000 /hdfs/path/file.ext");
            return;
        }
        org.apache.kafka.clients.consumer.Consumer<String, String> consumer =
                new KafkaConsumer<>(generateProperties(args[1]));
        consumer.subscribe(Collections.singletonList(args[0]));

        Consumer.builder()
                .consumer(consumer)
                .hdfsUri(args[2])
                .hdfsFilePath(args[3])
                .build()
                .runConsumer();
    }

    private static Properties generateProperties(String kafkaServer) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "console-consumer-myapp");

        return props;
    }
}
