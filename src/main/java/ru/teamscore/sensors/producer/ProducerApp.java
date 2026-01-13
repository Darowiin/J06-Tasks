package ru.teamscore.sensors.producer;

import ru.teamscore.sensors.common.config.EntityManagerFactoryProvider;

import java.util.Scanner;

/**
 * Точка входа для приложения Producer.
 * Генерирует и записывает в БД сообщения от датчиков в непрерывном цикле.
 * <p>
 * Для остановки нажмите Enter.
 */
public class ProducerApp {
    public static void main(String[] args) {
        System.out.println("=== Sensor Data Producer ===");
        System.out.println("Generating random sensor data...");
        System.out.println("Press Enter to stop.");
        System.out.println();

        int minDelay = 10;
        int maxDelay = 100;

        if (args.length >= 1) {
            try {
                minDelay = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid minDelay, using default: " + minDelay);
            }
        }
        if (args.length >= 2) {
            try {
                maxDelay = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid maxDelay, using default: " + maxDelay);
            }
        }

        System.out.println("Delay range: " + minDelay + "-" + maxDelay + " ms");
        System.out.println();

        ProducerService producer = new ProducerService(
                EntityManagerFactoryProvider.getEntityManagerFactory(),
                new SensorDataGenerator(),
                minDelay,
                maxDelay
        );

        Thread producerThread = new Thread(producer::start, "ProducerThread");
        producerThread.start();

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        System.out.println("Stopping producer...");
        producer.stop();

        System.out.println("Total messages produced: " + producer.getMessageCount());

        try {
            producerThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("Done.");

        EntityManagerFactoryProvider.getEntityManagerFactory().close();
    }
}