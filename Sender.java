package com.hireright.testtask;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class Sender implements AutoCloseable {
    private final Logger log = Logger.getLogger(Sender.class.getName());
    private final Random RAND = new Random();
    private final int MAX_LENGTH = 50;

    private String host, queue, exchange;
    private Connection connection;
    private Channel channel;

    public Sender() {
        try (InputStream propSource = Sender.class.getClassLoader().getResourceAsStream("rabbit.properties")) {
            Properties prop = new Properties();
            prop.load(propSource);
            host = prop.getProperty("host");
            queue = prop.getProperty("queue.name");
            exchange = prop.getProperty("exchange.name");
        } catch (IOException e) {
            log.warning("Could not load file \"rabbit.properties\": " + e.getMessage());
            System.exit(1);
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(exchange, "direct", true);
            log.info("Created direct exchange \"" + exchange + "\"");
            channel.queueDeclare(queue, true, false, false, null);
            log.info("Created durable queue \"" + queue + "\"");
            channel.queueBind(queue, exchange, "");
        } catch (TimeoutException | IOException e) {
            log.warning("Something went wrong: " + e.getMessage());
        }
    }

    private String generate() {
        int length = RAND.nextInt(MAX_LENGTH);
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int nextChar = (int)'a' + (int)(RAND.nextFloat() * ('z' - 'a' + 1));
            builder.append((char)nextChar);
        }
        return builder.toString();
    }

    public void sendRandomMessage() {
        String message = generate();
        try {
            channel.basicPublish("", queue, MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes(StandardCharsets.UTF_8));
            log.info("Sent message: \"" + message + "\"");
        } catch (IOException e) {
            log.warning("Failed to send message \"" + message + "\": " + e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        channel.close();
        connection.close();
    }
}
