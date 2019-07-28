package columns;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;

/**
 * Parsing of yml file that contains types of columns.
 *
 * @author Kovalev_Nikita1@epam.com
 */
@Slf4j
public class ColumnsTypesParser {
    private static final String FILE_PATH = "/columns_types.yml";

    public static ColumnsTypes parse() {
        Yaml yaml = new Yaml();

        Resource resource = new ClassPathResource(FILE_PATH);

        try (InputStream is = resource.getInputStream()) {
            return yaml.loadAs(is, ColumnsTypes.class);
        } catch (IOException e) {
            log.error("Error while parsing columns types!", e);
        }

        return null;
    }
}
