package ru.teamscore.sensors.producer;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import ru.teamscore.sensors.common.entity.RawSensorMessage;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Сервис производителя сенсорных данных.
 * Работает в непрерывном цикле, генерируя и записывая в БД сообщения от датчиков.
 */
public class ProducerService {
    private static final Logger LOGGER = Logger.getLogger(ProducerService.class.getName());

    private final EntityManagerFactory emf;
    private final SensorDataGenerator generator;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong messageCount = new AtomicLong(0);

    private final int minDelayMs;
    private final int maxDelayMs;

    public ProducerService(EntityManagerFactory emf) {
        this(emf, new SensorDataGenerator(), 10, 50);
    }

    public ProducerService(EntityManagerFactory emf, SensorDataGenerator generator) {
        this(emf, generator, 10, 50);
    }

    public ProducerService(EntityManagerFactory emf, SensorDataGenerator generator, int minDelayMs, int maxDelayMs) {
        this.emf = emf;
        this.generator = generator;
        this.minDelayMs = Math.max(0, minDelayMs);
        this.maxDelayMs = Math.max(this.minDelayMs, maxDelayMs);
    }

    /**
     * Запускает бесконечный цикл генерации сообщений.
     * Может быть остановлен вызовом метода stop().
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            LOGGER.info("Producer service started");
            runProducerLoop();
        } else {
            LOGGER.warning("Producer service is already running");
        }
    }

    /**
     * Останавливает цикл генерации.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            LOGGER.info("Producer service stopping...");
        }
    }

    /**
     * Проверяет, работает ли сервис.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Возвращает количество успешно записанных сообщений.
     */
    public long getMessageCount() {
        return messageCount.get();
    }

    /**
     * Основной цикл генерации и записи сообщений.
     */
    private void runProducerLoop() {
        while (running.get()) {
            try {
                RawSensorMessage message = generator.generateRandomMessage();
                saveMessage(message);
                messageCount.incrementAndGet();

                if (messageCount.get() % 1000 == 0) {
                    LOGGER.info("Produced " + messageCount.get() + " messages");
                }

                int delay = minDelayMs + (int) (Math.random() * (maxDelayMs - minDelayMs));
                Thread.sleep(delay);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.info("Producer thread interrupted");
                break;
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error producing message: " + e.getMessage(), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        LOGGER.info("Producer service stopped. Total messages: " + messageCount.get());
    }

    /**
     * Сохраняет сообщение в базу данных.
     */
    private void saveMessage(RawSensorMessage message) {
        EntityTransaction tx = null;
        try (EntityManager em = emf.createEntityManager()) {
            tx = em.getTransaction();
            tx.begin();
            em.persist(message);
            tx.commit();
            LOGGER.fine("Saved message: " + message);
        } catch (Exception e) {
            if (tx != null && tx.isActive()) {
                tx.rollback();
            }
            throw new RuntimeException("Failed to save message: " + message, e);
        }
    }

    /**
     * Генерирует и сохраняет одно сообщение (для тестирования).
     */
    public RawSensorMessage produceOne() {
        RawSensorMessage message = generator.generateRandomMessage();
        saveMessage(message);
        messageCount.incrementAndGet();
        return message;
    }
}