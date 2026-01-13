package ru.teamscore.sensors.consumer;

import jakarta.persistence.EntityManagerFactory;
import ru.teamscore.sensors.common.config.EntityManagerFactoryProvider;

import java.util.logging.Logger;

/**
 * Приложение Consumer для обработки сырых сообщений от датчиков.
 */
public class ConsumerApp {
    private static final Logger LOGGER = Logger.getLogger(ConsumerApp.class.getName());

    public static void main(String[] args) {
        LOGGER.info("Starting Consumer Application...");

        EntityManagerFactory emf = EntityManagerFactoryProvider.getEntityManagerFactory();
        ConsumerService consumerService = new ConsumerService(emf);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown signal received");
            consumerService.stop();
        }));

        try {
            consumerService.start();
        } finally {
            emf.close();
            LOGGER.info("Consumer Application stopped");
        }
    }
}