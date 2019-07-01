import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import pojo.BookingColumnsTypes;
import pojo.BookingColumnsTypesParser;
import pojo.DataType;

/**
 * @author Kovalev_Nikita1@epam.com
 */
@Ignore
@RunWith(PowerMockRunner.class)
public class BookingDaemonTest {

  private static final int RECORDS_COUNT = 2;
  private static final int THREAD_POOL_SIZE = 2;

  @Spy
  @InjectMocks
  private BookingDaemon subj;

  @Mock
  private Producer<String, String> producer;

  @Before
  public void setUp() {
    subj.setThreadPoolSize(THREAD_POOL_SIZE);
    subj.setRecordsCount(RECORDS_COUNT);

    Map<String, DataType> bookingColumnsTypes = new HashMap<>();
    bookingColumnsTypes.put("dateTest", DataType.DATE);
    bookingColumnsTypes.put("intTest", DataType.INTEGER);
    bookingColumnsTypes.put("boolTest", DataType.BOOLEAN);
    bookingColumnsTypes.put("doubleTest", DataType.DOUBLE);

    BookingColumnsTypes pojo = new BookingColumnsTypes();
    pojo.setTypes(bookingColumnsTypes);

    mockStatic(BookingColumnsTypesParser.class);
    when(BookingColumnsTypesParser.parse()).thenReturn(pojo);
  }

  @Test
  @Ignore
  public void run_testSendingOf2MessageToTheKafkaTopic() {
    subj.run();

    verify(producer, times(2)).send(any());
    verify(producer).close();
  }
}