package io.github.pkhanal;


import io.github.pkhanal.sources.MQTTSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileInputStream;
import java.util.Properties;

public class DeviceDataAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/resources/application.properties"));

        Properties mqttProperties = new Properties();

        // client id = a:<Organization_ID>:<App_Id>
        mqttProperties.setProperty(MQTTSource.CLIENT_ID,
                String.format("a:%s:%s",
                        properties.getProperty("Org_ID"),
                        properties.getProperty("App_Id")));

        // mqtt server url = tcp://<Org_ID>.messaging.internetofthings.ibmcloud.com:1883
        mqttProperties.setProperty(MQTTSource.URL,
                String.format("tcp://%s.messaging.internetofthings.ibmcloud.com:1883",
                        properties.getProperty("Org_ID")));

        // topic = iot-2/type/<Device_Type>/id/<Device_ID>/evt/<Event_Id>/fmt/json
        mqttProperties.setProperty(MQTTSource.TOPIC,
                String.format("iot-2/type/%s/id/%s/evt/%s/fmt/json",
                        properties.getProperty("Device_Type"),
                        properties.getProperty("Device_ID"),
                        properties.getProperty("EVENT_ID")));

        mqttProperties.setProperty(MQTTSource.USERNAME, properties.getProperty("API_Key"));
        mqttProperties.setProperty(MQTTSource.PASSWORD, properties.getProperty("APP_Authentication_Token"));


        MQTTSource mqttSource = new MQTTSource(mqttProperties);
        DataStreamSource<String> tempratureDataSource = env.addSource(mqttSource);
        DataStream<String> stream = tempratureDataSource.map((MapFunction<String, String>) s -> s);
        stream.print();

        env.execute("Temperature Analysis");
    }

}
