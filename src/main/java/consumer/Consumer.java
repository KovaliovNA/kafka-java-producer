package consumer;

import columns.ColumnsTypes;
import columns.ColumnsTypesParser;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * Consumer that read data from kafka topic and store it into hdfs.
 */
@Slf4j
public class Consumer {

    private final static ColumnsTypes COLUMNS_TYPES = ColumnsTypesParser.parse();

    private final org.apache.kafka.clients.consumer.Consumer<String, String> consumer;
    private final String HDFS_URI;
    private final String HDFS_FILE_PATH;
    private boolean running = true;

    @Builder
    public Consumer(org.apache.kafka.clients.consumer.Consumer<String, String> consumer,
                    String hdfsUri, String hdfsFilePath) {
        this.consumer = consumer;
        HDFS_URI = hdfsUri;
        HDFS_FILE_PATH = hdfsFilePath;
    }


    /**
     * Starts reading data by {@link consumer} to hdfs by uri {@link Consumer#HDFS_URI}
     * and place into hdfs {@link Consumer#HDFS_FILE_PATH}.
     * <p>
     * If file {@link Consumer#HDFS_FILE_PATH} doesn`t exist, than it will be create with header
     * that stores as {@link Consumer#COLUMNS_TYPES} keys.
     *
     * @throws IOException throws in case some errors while saving data into hdfs.
     */
    public void runConsumer() throws IOException {
        while (running) {
            writeDataToHdfs(consumer.poll(Duration.ofSeconds(100)));
        }
    }

    private void writeDataToHdfs(ConsumerRecords<String, String> poll) throws IOException {
        if (poll.isEmpty()) {
            return;
        }

        Configuration conf = new Configuration();
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "ALWAYS");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.best-effort", "true");
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());

        Path uri = new Path(HDFS_URI + HDFS_FILE_PATH);
        Path hdfsFilePath = new Path(HDFS_FILE_PATH);
        FileSystem hdfs = uri.getFileSystem(conf);

        try (FSDataOutputStream outputStream = hdfs.exists(hdfsFilePath) ? hdfs.append(uri) : createFile(hdfs, hdfsFilePath)) {
            poll.forEach(record -> recordWriter(record, outputStream));
        }
    }

    private FSDataOutputStream createFile(FileSystem hdfs, Path hdfsPath) throws IOException {
        FSDataOutputStream outputStream = hdfs.create(hdfsPath);

        Objects.requireNonNull(COLUMNS_TYPES, "Booking columns types must not be null!");

        outputStream.writeBytes(String.join(",", COLUMNS_TYPES.getTypes().keySet()) + "\n");

        return outputStream;
    }

    private void recordWriter(ConsumerRecord<String, String> record, FSDataOutputStream outputStream) {
        try {
            if (record == null) {
                return;
            }

            outputStream.writeBytes(prepareRecord(record) + "\n");
        } catch (IOException e) {
            log.error("Error in writing record from kafka topic: {} to hdfs: {}",
                    consumer.subscription(), HDFS_URI + HDFS_FILE_PATH);
            running = false;
        }
    }

    private String prepareRecord(ConsumerRecord<String, String> record) {
        return escapeSpecialCharacters(record.value());
    }

    private String escapeSpecialCharacters(String data) {
        return data.replace("\"", "");
    }
}
