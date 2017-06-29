package io.github.pkhanal;


import com.google.gson.JsonObject;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;

// Publishes randomly generated temperature date every 500 milliseconds
public class DeviceSimulator {

    private static final String USERNAME = "use-token-auth";


    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/resources/device.properties"));

        // d:<orgid>:<device-type>:<device-id>
        String clientId = String.format("d:%s:%s:%s",
                properties.getProperty("Org_ID"),
                properties.getProperty("Device_Type"),
                properties.getProperty("Device_ID"));

        // tcp://<Org_ID>.messaging.internetofthings.ibmcloud.com:1883
        String serverUrl = String.format("tcp://%s.messaging.internetofthings.ibmcloud.com:1883", properties.getProperty("Org_ID"));

        // iot-2/evt/<event-id>/fmt/json
        String topic = String.format("iot-2/evt/%s/fmt/json", properties.getProperty("EVENT_ID"));

        String password = properties.getProperty("Authentication_Token");

        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setUserName(USERNAME);
        connectOptions.setPassword(password.toCharArray());
        MqttClient client = new MqttClient(serverUrl, clientId);
        client.connect(connectOptions);

        // Generate random temperature values between 40 - 60
        Random rand = new Random();

        while (true) {
            int temprature = rand.nextInt((60 - 40) + 1) + 40;
            JsonObject event = new JsonObject();
            event.addProperty("temperature", temprature);
            event.addProperty("timestamp", Instant.now().toEpochMilli());
            System.out.println("Published Message: " + event.toString());
            client.publish(topic, new MqttMessage(event.toString().getBytes(StandardCharsets.UTF_8)));
            try {
                Thread.sleep(500);
            } catch (Exception ex) {

            }
        }
    }
}
