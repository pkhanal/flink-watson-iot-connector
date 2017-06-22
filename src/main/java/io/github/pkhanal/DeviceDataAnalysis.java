package io.github.pkhanal;


import io.github.pkhanal.sources.MQTTSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class DeviceDataAnalysis {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.load(DeviceDataAnalysis.class.getResourceAsStream("config.properties"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        MQTTSource mqttSource = new MQTTSource(properties);
        DataStreamSource<String> tempratureDataSource = env.addSource(mqttSource);
        DataStream<String> stream = tempratureDataSource.map((MapFunction<String, String>) s -> s);
        stream.print();
    }

}
