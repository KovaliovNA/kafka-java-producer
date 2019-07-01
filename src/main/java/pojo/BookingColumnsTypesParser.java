package pojo;

import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.yaml.snakeyaml.Yaml;

/**
 * Parsing of yml file that contains types of columns.
 * 
 * @author Kovalev_Nikita1@epam.com
 */
@Slf4j
public class BookingColumnsTypesParser {
  private static final String FILE_PATH = "/booking_columns_types.yml";
  
  public static BookingColumnsTypes parse() {
    Yaml yaml = new Yaml();

    Resource resource = new ClassPathResource(FILE_PATH);

    try (InputStream is =  resource.getInputStream()) {
      return yaml.loadAs(is, BookingColumnsTypes.class);
    } catch (IOException e) {
      log.error("Error while parsing columns types!", e);
    }

    return null;
  }
}
