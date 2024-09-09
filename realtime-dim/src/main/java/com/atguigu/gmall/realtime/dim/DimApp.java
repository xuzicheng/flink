package com.atguigu.gmall.realtime.dim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

//dim维度层的处理
public class DimApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4); //设置并行度为1
        //开启检查点
        env.enableCheckpointing(50000, CheckpointingMode.EXACTLY_ONCE); //设置检查点间隔为50s
        //设置超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000); //设置超时时间为60s
        //设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置检查点之间时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000); //设置检查点之间最小间隔为2s

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000)); //设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        //设置hadoop的用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");


        //从kafka中读取topic_db数据

        String topic_db = "topic_db";
        String groupId = "dim_app_group";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setGroupId(groupId)
                .setTopics(Constant.TOPIC_DB)
                //确保消费一致性，需要手动维护偏移量，kafkaSource ->kafkaSourceReader ->存储偏移量
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if(message != null ){
                                    return new String(message);
                                }
                               return null;
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_Source");



    }
}
