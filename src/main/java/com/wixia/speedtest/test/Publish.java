package com.wixia.speedtest.test;

import com.amazonaws.services.iot.client.*;
import com.amazonaws.services.iot.client.sample.sampleUtil.CommandArguments;
import com.amazonaws.services.iot.client.sample.sampleUtil.SampleUtil;

public class Publish {
    private static final AWSIotQos TestTopicQos = AWSIotQos.QOS0;

    private static AWSIotMqttClient awsIotClient;

    public static void setClient(AWSIotMqttClient client) {
        awsIotClient = client;
    }

    private static void initClient(CommandArguments arguments) {
        String clientEndpoint = arguments.getNotNull("client.endpoint", SampleUtil.getConfig("client.endpoint"));
        String clientId = arguments.getNotNull("client.id", SampleUtil.getConfig("client.id"));

        String certificateFile = arguments.get("client.certificate.file", SampleUtil.getConfig("client.certificate.file"));
        String privateKeyFile = arguments.get("client.privatekey.file", SampleUtil.getConfig("client.privatekey.file"));
        if (awsIotClient == null && certificateFile != null && privateKeyFile != null) {
            String algorithm = arguments.get("keyAlgorithm", SampleUtil.getConfig("keyAlgorithm"));

            SampleUtil.KeyStorePasswordPair pair = SampleUtil.getKeyStorePasswordPair(certificateFile, privateKeyFile, algorithm);

            awsIotClient = new AWSIotMqttClient(clientEndpoint, clientId, pair.keyStore, pair.keyPassword);
        }

        if (awsIotClient == null) {
            String awsAccessKeyId = arguments.get("awsAccessKeyId", SampleUtil.getConfig("awsAccessKeyId"));
            String awsSecretAccessKey = arguments.get("awsSecretAccessKey", SampleUtil.getConfig("awsSecretAccessKey"));
            String sessionToken = arguments.get("sessionToken", SampleUtil.getConfig("sessionToken"));

            if (awsAccessKeyId != null && awsSecretAccessKey != null) {
                awsIotClient = new AWSIotMqttClient(clientEndpoint, clientId, awsAccessKeyId, awsSecretAccessKey,
                        sessionToken);
            }
        }

        if (awsIotClient == null) {
            throw new IllegalArgumentException("Failed to construct client due to missing certificate or credentials.");
        }
    }



    public static void main(String args[]) throws InterruptedException, AWSIotException, AWSIotTimeoutException {
        CommandArguments arguments = CommandArguments.parse(args);
        initClient(arguments);

        awsIotClient.connect();

        String topicName = arguments.getNotNull("client.topic.name", SampleUtil.getConfig("client.topic.name"));

        Thread blockingPublishThread = new Thread(new BlockingPublisher(awsIotClient, topicName));

        blockingPublishThread.start();

        blockingPublishThread.join();
    }

}
