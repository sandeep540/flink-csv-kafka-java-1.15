
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MainApp {

    public static void main(String[] args) {

        final Configuration flinkConfig = new Configuration();

        // flinkConfig.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);
        // final int parallelism = 2;
        // final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, flinkConfig);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointStorage("file:///Users/sandeep/csv");
        //env.setParallelism(4);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //Source look up
        String sql = "CREATE TABLE lookup (`id` BIGINT, `blacklist` BOOLEAN) WITH ('connector' = 'filesystem', 'path' = 'file:///Users/sandeep/csv/lookup.csv', 'format' = 'csv')";
        tEnv.executeSql(sql);

        //Sink kafka
        String sinkSql = "CREATE TABLE transactions (`id` BIGINT, `blacklist` BOOLEAN, PRIMARY KEY (id) NOT ENFORCED) " +
                "WITH ('connector' = 'upsert-kafka', 'topic' = 'transactions', 'properties.bootstrap.servers' = 'localhost:9092', 'key.format' = 'json', 'value.format' = 'json')";
        tEnv.executeSql(sinkSql);

        tEnv.executeSql("select orders.order_id, orders.product, UPPER(orders.town) as `town`, lookup.blacklist from orders join lookup on orders.order_id = lookup.id")
                .print();

    }
}
