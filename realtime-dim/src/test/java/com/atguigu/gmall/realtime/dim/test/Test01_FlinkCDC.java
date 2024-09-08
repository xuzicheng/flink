package com.atguigu.gmall.realtime.dim.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        //1.基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置并行度
        env.setParallelism(1);

        // enable checkpoint
        env.enableCheckpointing(3000);

        //3.使用Flink CDC读取MySQL表中的数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall2024_config") // set captured database
                .tableList("gmall2024_config.t_user") // set captured table
                .username("root")
                .password("000000")
                //.startFromLatest(StartOption.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();


        //{"before":null,"after":{"name":"s1","age":11},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"t_user","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1725801996617,"transaction":null}
        //{"before":null,"after":{"name":"3","age":1231},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1725802117000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000019","pos":700,"row":0,"thread":16,"query":null},"op":"c","ts_ms":1725802109932,"transaction":null}
        //{"before":null,"after":{"name":"www","age":121},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1725802151000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000019","pos":1021,"row":0,"thread":16,"query":null},"op":"c","ts_ms":1725802144303,"transaction":null}
        // enable checkpoint

        env.enableCheckpointing(3000);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print();// use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
