package pojo;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * Types of topic rows for sending to kafka.
 * 
 * @author Kovalev_Nikita1@epam.com
 */
@Getter
@Setter
public class BookingColumnsTypes {

  /**
   * Map a types of topic that will be created for sending to kafka.
   */
  Map<String, DataType> types;
}