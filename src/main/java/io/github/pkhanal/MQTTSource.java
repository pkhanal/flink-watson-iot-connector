package io.github.pkhanal;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.paho.client.mqttv3.*;

import java.util.Properties;

public class MQTTSource implements SourceFunction<String >, StoppableFunction {

    // ----- Required property keys
    public static final String URL = "mqtt.broker.url";
    public static final String CLIENT_ID = "mqtt.client.id";
    public static final String TOPIC = "mqtt.client.topic";

    // ------ Optional property keys
    public static final String USERNAME = "mqtt.username";
    public static final String PASSWORD = "mqtt.password";


    private final Properties properties;

    // ------ Runtime fields
    private transient MqttClient client;
    private transient volatile boolean running;
    private transient Object waitLock = new Object();

    public MQTTSource(Properties properties) {
        checkProperty(properties, URL);
        checkProperty(properties, CLIENT_ID);
        checkProperty(properties, TOPIC);

        this.properties = properties;
    }

    private static void checkProperty(Properties p, String key) {
        if (!p.containsKey(key)) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }

    @Override
    public void stop() {
        close();
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
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
        client.connect(connectOptions);

        client.subscribe(properties.getProperty(TOPIC), 0, (topic, message) -> {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(new String(message.getPayload(), Charsets.UTF_8));
            }
        });

        while (running) {
            synchronized (waitLock) {
                waitLock.wait(100L);
            }
        }
    }

    @Override
    public void cancel() {
        close();
    }

    private void close() {
        this.running = false;
        if (client != null) {
            try {
                client.disconnect();
            } catch (MqttException exception) {

            }
        }
        // leave main method
        synchronized (waitLock) {
            waitLock.notify();
        }
    }
}
