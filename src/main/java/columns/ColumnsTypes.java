package columns;

import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Types of topic rows for sending to kafka.
 *
 * @author Kovalev_Nikita1@epam.com
 */
@Getter
@Setter
public class ColumnsTypes {

    /**
     * Map a types of topic that will be created for sending to kafka.
     */
    LinkedHashMap<String, DataType> types;
}