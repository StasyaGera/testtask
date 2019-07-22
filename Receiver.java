package com.hireright.testtask;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class Receiver implements AutoCloseable {
    private final Logger log = Logger.getLogger(Receiver.class.getName());

    private String host, queue;
    private Connection connection;
    private Channel channel;

    public Receiver() {
        try (InputStream input = Receiver.class.getClassLoader().getResourceAsStream("rabbit.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            host = prop.getProperty("host");
            queue = prop.getProperty("queue.name");
        } catch (IOException e) {
            log.warning("Could not load file \"rabbit.properties\": " + e.getMessage());
            System.exit(1);
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(queue, true, false, false, null);
            log.info("Created durable queue \"" + queue + "\"");
        } catch (TimeoutException e) {
            log.warning("Could not create new connection: " + e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            log.warning("Something went wrong: " + e.getMessage());
            System.exit(1);
        }
    }

    public void run() {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            log.info("Received message \"" + message + "\"");
            System.out.println(message);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
        try {
            channel.basicConsume(queue, false, deliverCallback, consumerTag -> { });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        channel.close();
        connection.close();
    }
}
