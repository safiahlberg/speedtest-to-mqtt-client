package com.wixia.speedtest.test;

import com.amazonaws.services.iot.client.AWSIotException;
import com.amazonaws.services.iot.client.AWSIotMqttClient;

public class BlockingPublisher implements Runnable {
    private final AWSIotMqttClient awsIotClient;
    private final String topicName;

    public BlockingPublisher(AWSIotMqttClient awsIotClient, String topicName) {
        this.awsIotClient = awsIotClient;
        this.topicName = topicName;
    }

    @Override
    public void run() {
        long counter = 1;

        while (true) {
            String payload = "hello from blocking publisher - " + (counter++);
            try {
                awsIotClient.publish(topicName, payload);
            } catch (AWSIotException e) {
                System.out.println(System.currentTimeMillis() + ": publish failed for " + payload);
            }
            System.out.println(System.currentTimeMillis() + ": >>> " + payload);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println(System.currentTimeMillis() + ": BlockingPublisher was interrupted");
                return;
            }
        }
    }

}
