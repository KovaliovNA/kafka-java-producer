package producer;

import columns.ColumnsTypes;
import columns.ColumnsTypesParser;
import columns.DataType;
import lombok.Builder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import processbar.ProcessBar;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Daemon producing records into kafka topic.
 *
 * @author Kovalev_Nikita1@epam.com
 */
@Slf4j
@Builder
public class Producer extends Thread {

    @Setter
    private AtomicReference<KafkaProducer<String, String>> producer;
    @Setter
    private int recordsCount;
    @Setter
    private int threadPoolSize;
    @Setter
    private String topicName;

    private ExecutorService threadPool;
    public static final String COLUMN_SEPARATOR = ",";
    private final static ColumnsTypes COLUMNS_TYPES = ColumnsTypesParser.parse();


    /**
     * Creates threads for producing generated records to kafka topic.
     */
    @Override
    public void run() {
        threadPool = Executors.newFixedThreadPool(threadPoolSize);

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
            String record = getRandomRecord();
            String id = String.valueOf(ThreadLocalRandom.current().nextInt());

            producer.get().send(new ProducerRecord<>(topicName, id, record));
        }, threadPool);
    }

    /**
     * Generates record based on returned map form {@link ColumnsTypes#getTypes()}.
     *
     * @return - string that contains random values.
     */
    private String getRandomRecord() {
        Objects.requireNonNull(COLUMNS_TYPES, "Booking columns types must not be null!");

        StringBuilder builder = new StringBuilder();

        Date previousDate = null;
        for (DataType type : COLUMNS_TYPES.getTypes().values()) {
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

            builder.append(COLUMN_SEPARATOR);
        }

        return builder.toString();
    }
}