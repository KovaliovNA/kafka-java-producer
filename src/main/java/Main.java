import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import pojo.BookingColumnsTypes;
import pojo.BookingColumnsTypesParser;

/**
 * Entry point for starting daemon. First arg for app must be a bootstrap.servers as
 * sandbox-hdp.hortonworks.com:2181.
 * </p>
 * @author Kovalev_Nikita1@epam.com
 */
@Slf4j
public class Main {

  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("The parameters of this application must be like topic-name server:port!");
    }

    BookingDaemon daemon = new BookingDaemon();
    daemon.setTopicName(args[0]);
    daemon.setProducer(new AtomicReference<>(new KafkaProducer<>(generateProperties(args[1]))));
    daemon.setRecordsCount(100000);
    daemon.setThreadPoolSize(10);

    daemon.start();
  }

  private static Properties generateProperties(String server) {
    Properties props = new Properties();
    props.put("bootstrap.servers", server);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return props;
  }
}