package io.github.pkhanal.sources;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.paho.client.mqttv3.*;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class MQTTSource implements SourceFunction<String>, StoppableFunction {

    // ----- Required property keys
    public static final String URL = "mqtt.server.url";
    public static final String CLIENT_ID = "mqtt.client.id";
    public static final String TOPIC = "mqtt.topic";

    // ------ Optional property keys
    public static final String USERNAME = "mqtt.username";
    public static final String PASSWORD = "mqtt.password";


    private final Properties properties;

    // ------ Runtime fields
    private transient MqttClient client;
    private transient volatile boolean running;
    private transient Object waitLock;

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
    public void run(final SourceContext<String> ctx) throws Exception {
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

        client.subscribe(properties.getProperty(MQTTSource.TOPIC), (topic, message) -> {
            String msg = new String(message.getPayload(), StandardCharsets.UTF_8);
            ctx.collect(msg);
        });

        running = true;
        waitLock = new Object();

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
        try {
            if (client != null) {
                client.disconnect();
            }
        } catch (MqttException exception) {

        } finally {
            this.running = false;
        }

        // leave main method
        synchronized (waitLock) {
            waitLock.notify();
        }
    }
}
