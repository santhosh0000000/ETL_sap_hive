import org.apache.spark.sql.SparkSession;

public class spark_conn {
    private SparkSession spark;

    public spark_conn() {
        this.spark = SparkSession
                .builder()
                .appName("SparkConnector")
                .master("local[*]")
                .config("spark.jars", "/root/ngdbc-2.17.12.jar")
                .config("hive.metastore.uris", "thrift://hdp01-preprod.geepas.local:9083")
                .config("spark.executor.memory", "15g")
                .config("spark.executor.cores", "10")
                .config("spark.executor.instances", "25")
                .config("spark.dynamicAllocation.enabled", "false")
                .config("spark.default.parallelism", "200")
                .config("spark.cores.max", "160")
                .config("spark.sql.orc.compression.codec", "snappy")
                .enableHiveSupport()
                .getOrCreate();
    }

    public SparkSession getSpark() {
        return spark;
    }
}
