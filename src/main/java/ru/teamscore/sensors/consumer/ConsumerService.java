package ru.teamscore.sensors.consumer;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import ru.teamscore.sensors.common.entity.ProcessingState;
import ru.teamscore.sensors.common.entity.RawSensorMessage;
import ru.teamscore.sensors.common.entity.SensorDevice;
import ru.teamscore.sensors.common.entity.metric.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Сервис потребителя сенсорных данных.
 * Работает в непрерывном цикле, обрабатывая новые сообщения от датчиков.
 */
public class ConsumerService {
    private static final Logger LOGGER = Logger.getLogger(ConsumerService.class.getName());
    private static final String COMPONENT_NAME = "consumer";

    private final EntityManagerFactory emf;
    private final MessageParser messageParser;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong processedCount = new AtomicLong(0);

    private final int pollingIntervalMs;
    private final int batchSize;

    public ConsumerService(EntityManagerFactory emf) {
        this(emf, new MessageParser(), 100, 1000);
    }

    public ConsumerService(EntityManagerFactory emf, MessageParser messageParser) {
        this(emf, messageParser, 100, 1000);
    }

    public ConsumerService(EntityManagerFactory emf, MessageParser messageParser, int pollingIntervalMs, int batchSize) {
        this.emf = emf;
        this.messageParser = messageParser;
        this.pollingIntervalMs = Math.max(100, pollingIntervalMs);
        this.batchSize = Math.max(1, batchSize);
    }

    /**
     * Запускает непрерывный цикл обработки сообщений.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            LOGGER.info("Consumer service started");
            runConsumerLoop();
        } else {
            LOGGER.warning("Consumer service is already running");
        }
    }

    /**
     * Останавливает цикл обработки.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            LOGGER.info("Consumer service stopping...");
        }
    }

    /**
     * Проверяет, работает ли сервис.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Возвращает количество обработанных сообщений.
     */
    public long getProcessedCount() {
        return processedCount.get();
    }

    /**
     * Основной цикл обработки сообщений.
     */
    private void runConsumerLoop() {
        while (running.get()) {
            try {
                int processed = processBatch();
                if (processed > 0) {
                    LOGGER.info("Processed " + processed + " messages, total: " + processedCount.get());
                }

                if (processed == 0) {
                    Thread.sleep(pollingIntervalMs);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.info("Consumer thread interrupted");
                break;
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error processing messages: " + e.getMessage(), e);
                try {
                    Thread.sleep(pollingIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        LOGGER.info("Consumer loop finished");
    }

    /**
     * Обрабатывает пакет сообщений в одной транзакции.
     * @return количество обработанных сообщений
     */
    public int processBatch() {
        EntityTransaction tx = null;
        try (EntityManager em = emf.createEntityManager()) {
            tx = em.getTransaction();
            tx.begin();

            LocalDateTime lastProcessedTime = getLastProcessedTime(em);

            List<RawSensorMessage> messages = getNewMessages(em, lastProcessedTime);

            if (messages.isEmpty()) {
                tx.commit();
                return 0;
            }

            LocalDateTime maxSavedAt = lastProcessedTime;

            for (RawSensorMessage message : messages) {
                processMessage(em, message);

                if (message.getSavedAt().isAfter(maxSavedAt)) {
                    maxSavedAt = message.getSavedAt();
                }
            }

            updateLastProcessedTime(em, maxSavedAt);

            tx.commit();
            processedCount.addAndGet(messages.size());
            return messages.size();

        } catch (Exception e) {
            if (tx != null && tx.isActive()) {
                tx.rollback();
            }
            throw e;
        }
    }

    /**
     * Получает время последней обработки из ProcessingState.
     */
    private LocalDateTime getLastProcessedTime(EntityManager em) {
        ProcessingState state = em.find(ProcessingState.class, COMPONENT_NAME);
        if (state == null) {
            return LocalDateTime.of(1970, 1, 1, 0, 0, 0);
        }
        return state.getLastProcessedTime();
    }

    /**
     * Получает новые сообщения, которые были сохранены после lastProcessedTime.
     */
    private List<RawSensorMessage> getNewMessages(EntityManager em, LocalDateTime lastProcessedTime) {
        return em.createQuery(
                "SELECT m FROM RawSensorMessage m WHERE m.savedAt > :lastTime ORDER BY m.savedAt ASC",
                RawSensorMessage.class)
                .setParameter("lastTime", lastProcessedTime)
                .setMaxResults(batchSize)
                .getResultList();
    }

    /**
     * Обрабатывает одно сообщение:
     * <p>
     * 1. Обновляет/создаёт устройство
     * <p>
     * 2. Создаёт соответствующую метрику
     */
    private void processMessage(EntityManager em, RawSensorMessage message) {
        updateOrCreateDevice(em, message);

        SensorMetric metric = messageParser.parseMessage(message);
        em.persist(metric);
    }

    /**
     * Обновляет существующее устройство или создаёт новое.
     */
    private void updateOrCreateDevice(EntityManager em, RawSensorMessage message) {
        SensorDevice device = em.find(SensorDevice.class, message.getSensorId());

        if (device == null) {
            device = new SensorDevice(
                    message.getSensorId(),
                    message.getDeviceName(),
                    message.getSensorType(),
                    message.getMeasuredAt()
            );
            em.persist(device);
        } else {
            device.setDeviceName(message.getDeviceName());
            device.setSensorType(message.getSensorType());
            if (message.getMeasuredAt().isAfter(device.getLastSeen())) {
                device.setLastSeen(message.getMeasuredAt());
            }
            em.merge(device);
        }
    }

    /**
     * Обновляет время последней обработки в ProcessingState.
     */
    private void updateLastProcessedTime(EntityManager em, LocalDateTime time) {
        ProcessingState state = em.find(ProcessingState.class, COMPONENT_NAME);
        if (state == null) {
            state = new ProcessingState(COMPONENT_NAME, time);
            em.persist(state);
        } else {
            state.setLastProcessedTime(time);
            em.merge(state);
        }
    }
}