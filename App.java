
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
public class App extends spark_conn {

    static String username = "BIG_DATA_USER";
    static String password = "Geepas123";
    static String filePath = "/opt/nesto_TS/source.csv";

    public static void main(String[] args) {

        File file = new File(filePath);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            spark_conn spark = new spark_conn();
            SparkSession sparkSession = spark.getSpark();

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");

                String hostwithport = values[0];
                String schema = values[1];
                String tableName = values[2];
                String hivedb = values[3];

                String url = "jdbc:sap://" + hostwithport;

                tableName = tableName.trim(); // Trim any leading or trailing whitespace
                if (tableName.startsWith("/")) {
                    tableName = "\"" + tableName + "\""; // Wrap in quotes if it starts with a slash
                }
                String fullTableName = schema + "." + tableName; // Fully qualified table name

                // Read from the table
                Dataset<Row> df = sparkSession.read()
                        .format("jdbc")
                        .option("driver", "com.sap.db.jdbc.Driver")
                        .option("url", url)
                        .option("user", username)
                        .option("password", password)
                        .option("dbtable", fullTableName)
                        .option("fetchsize","200000")
                        .load();

                // Path to save in HDFS
                df.write().format("orc").option("path", "/demo").mode("overwrite").saveAsTable(hivedb +"."+ tableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            new spark_conn().getSpark().stop();
        }
    }
}
