package com.wixia.speedtest;

import com.amazonaws.services.iot.client.*;
import com.amazonaws.services.iot.client.sample.sampleUtil.SampleUtil;
import org.apache.commons.cli.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;

public class MqttToDynamoDb {
    static final Option help = new Option("help", "print this message");
    static final Option configFile = Option.builder("configfile")
            .required(false)
            .hasArg()
            .numberOfArgs(1)
            .desc("use given file as configuration")
            .build();

    static void connectAndListen(Configuration configuration) throws AWSIotException {
        String clientEndpoint = configuration.getString("client.endpoint");
        String clientId = configuration.getString("client.id");
        String certificateFile = configuration.getString("client.certificate.file", "certificate.pem.crt");
        String privateKeyFile = configuration.getString("client.privatekey.file", "private.pem.key");
        String topicName = configuration.getString("client.topic.name", "speedtest");

        // SampleUtil.java and its dependency PrivateKeyReader.java can be copied from the sample source code.
        // Alternatively, you could load key store directly from a file - see the example included in this README.
        SampleUtil.KeyStorePasswordPair pair = SampleUtil.getKeyStorePasswordPair(certificateFile, privateKeyFile);
        AWSIotMqttClient client = new AWSIotMqttClient(clientEndpoint, clientId, pair.keyStore, pair.keyPassword);

        // optional parameters can be set before connect()
        client.connect();

        AWSIotQos qos = AWSIotQos.QOS0;

        SpeedTestTopic topic = new SpeedTestTopic(topicName, qos);
        client.subscribe(topic);

        // client.publish(topicName, "{ \"message\": \"Testing\" }");
    }

    static CommandLine parseArgs(String[] args, CommandLineParser parser) throws ParseException {
        final Options options = new Options();

        options.addOption(help);
        options.addOption(configFile);

        CommandLine line = parser.parse(options, args);

        return line;
    }

    static Configuration configFromFile(CommandLine line, Configurations configurations) throws ConfigurationException {
        final String configFileName = line.hasOption(configFile.getArgName()) ? line.getOptionValue(configFile.getArgName()) : "config.properties";

        return configurations.properties(new File(configFileName));
    }

    public static void main(String[] args) throws ParseException, ConfigurationException, AWSIotException {
        final CommandLine line = parseArgs(args, new DefaultParser());

        final Configuration configuration = configFromFile(line, new Configurations());

        connectAndListen(configuration);
    }

    public static class SpeedTestTopic extends AWSIotTopic {
        public SpeedTestTopic(String topic, AWSIotQos qos) {
            super(topic, qos);
        }

        @Override
        public void onSuccess() {
            System.out.println(String.format("connected to %s topic", getTopic()));
            super.onSuccess();
        }

        @Override
        public void onFailure() {
            System.out.println("Error in topic");
            super.onFailure();
        }

        @Override
        public void onMessage(AWSIotMessage message) {
            System.out.println(String.format("%d : <<< %s", System.currentTimeMillis(), message.getStringPayload()));
        }

    }

}