import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.Properties;

public class Helper {

    public static Properties getPropValues() {
        Logger logger = LoggerFactory.getLogger(Helper.class.getName());
        Properties prop = new Properties();
        try (FileReader fileReader = new FileReader("app.properties")) {
            prop.load(fileReader);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return prop;
    }
}
