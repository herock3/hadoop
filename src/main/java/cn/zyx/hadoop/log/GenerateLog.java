package cn.zyx.hadoop.log;
import java.util.Date;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
public class GenerateLog {
    public GenerateLog() {
    }

    public static void main(String[] args) throws Exception {
        Logger logger = LogManager.getLogger("testlog");
        int i = 0;

        do {
            logger.info((new Date()).toString() + "-----------------------------");
            ++i;
            Thread.sleep(500L);
        } while(i <= 1000000);

    }
}
