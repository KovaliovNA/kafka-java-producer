import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pojo.BookingColumnsTypes;
import pojo.BookingColumnsTypesParser;
import pojo.DataType;
import processbar.ProcessBar;

/**
 * Daemon producing records for topic booking.
 *
 * @author Kovalev_Nikita1@epam.com
 */
@Slf4j
public class BookingDaemon extends Thread {

  @Setter
  private AtomicReference<KafkaProducer<String, String>> producer;
  @Setter
  private int recordsCount;
  @Setter
  private int threadPoolSize;
  @Setter
  private String topicName;

  private BookingColumnsTypes topicColumnsTypes;
  private ExecutorService threadPool;


  /**
   * Creates threads for producing generated records to booking topic.
   */
  @Override
  public void run() {
    threadPool = Executors.newFixedThreadPool(threadPoolSize);
    topicColumnsTypes = BookingColumnsTypesParser.parse();

    if (topicColumnsTypes == null) {
      log.error("Booking columns types must not be null!");
      return;
    }

    Collection<CompletableFuture<Void>> senders = new ArrayList<>();

    ProcessBar processBar = new ProcessBar(recordsCount,
        "The producing of " + recordsCount + " messages was started...");

    for (int i = 0; !Thread.interrupted() && i < recordsCount; i++) {
      senders.add(createSenderThread().thenAccept(Void -> processBar.step()));
    }

    CompletableFuture
        .allOf(senders.toArray(new CompletableFuture[0]))
        .join();

    System.exit(0);
  }

  private CompletableFuture<Void> createSenderThread() {
    return CompletableFuture.runAsync(() -> {
      String record = getRandomRecord(topicColumnsTypes);
      String id = String.valueOf(ThreadLocalRandom.current().nextInt());

      producer.get().send(new ProducerRecord<>(topicName, id, record));
    }, threadPool);
  }

  /**
   * Generates record based on returned map form {@link BookingColumnsTypes#getTypes()}.
   *
   * @param topicColumnsTypes - map that contains field name and their type.
   * @return - string that contains random values.
   */
  private String getRandomRecord(BookingColumnsTypes topicColumnsTypes) {
    StringBuilder builder = new StringBuilder();

    Date previousDate = null;
    for (DataType type : topicColumnsTypes.getTypes().values()) {
      switch (type) {
        case INTEGER:
          builder.append(ThreadLocalRandom.current().nextInt(1, 100000));
          break;
        case DATE:
          if (previousDate != null) {
            previousDate = new Date(Math.abs(ThreadLocalRandom.current()
                .nextLong(previousDate.getTime())));
          } else {
            previousDate = new Date(Math.abs(ThreadLocalRandom.current()
                .nextLong()));
          }

          builder.append(previousDate.toString());
          break;
        case BOOLEAN:
          builder.append(ThreadLocalRandom.current().nextBoolean());
          break;
        case DOUBLE:
          builder.append(ThreadLocalRandom.current().nextDouble(50, 100000));
          break;
      }

      builder.append(",");
    }

    return builder.toString();
  }
}