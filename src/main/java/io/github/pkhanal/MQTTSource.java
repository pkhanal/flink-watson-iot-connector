package io.github.pkhanal;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.paho.client.mqttv3.*;

import java.util.Properties;

public class MQTTSource implements SourceFunction<String >, StoppableFunction, MqttCallback {

    // ----- Required property keys
    public static final String URL = "mqtt.broker.url";
    public static final String CLIENT_ID = "mqtt.client.id";

    // ------ Optional property keys
    public static final String USERNAME = "mqtt.username";
    public static final String PASSWORD = "mqtt.password";


    private final Properties properties;

    // ------ Runtime fields
    private transient MqttClient client;
    private transient volatile boolean running;

    public MQTTSource(Properties properties) {
        checkProperty(properties, URL);
        checkProperty(properties, CLIENT_ID);

        this.properties = properties;
    }

    private static void checkProperty(Properties p, String key) {
        if (!p.containsKey(key)) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }


    @Override
    public void stop() {

    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);

        if (properties.containsKey(USERNAME)) {
            connectOptions.setUserName(properties.getProperty(USERNAME));
        }

        if (properties.containsKey(PASSWORD)) {
            connectOptions.setPassword(properties.getProperty(PASSWORD).toCharArray());
        }

        connectOptions.setAutomaticReconnect(true);

        client = new MqttClient(properties.getProperty(URL), properties.getProperty(CLIENT_ID));
    }

    @Override
    public void cancel() {

    }

    @Override
    public void connectionLost(Throwable throwable) {

    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }
}
