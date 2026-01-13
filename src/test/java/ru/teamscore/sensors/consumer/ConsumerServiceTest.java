package ru.teamscore.sensors.consumer;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.*;
import ru.teamscore.sensors.common.SensorType;
import ru.teamscore.sensors.common.entity.ProcessingState;
import ru.teamscore.sensors.common.entity.RawSensorMessage;
import ru.teamscore.sensors.common.entity.SensorDevice;
import ru.teamscore.sensors.common.entity.metric.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerServiceTest {

    private static EntityManagerFactory emf;
    private ConsumerService consumerService;

    @BeforeAll
    static void setUpClass() {
        emf = new Configuration()
                .addAnnotatedClass(RawSensorMessage.class)
                .addAnnotatedClass(SensorDevice.class)
                .addAnnotatedClass(ProcessingState.class)
                .addAnnotatedClass(LightMetric.class)
                .addAnnotatedClass(BarometerMetric.class)
                .addAnnotatedClass(LocationMetric.class)
                .addAnnotatedClass(AccelerometerMetric.class)
                .buildSessionFactory();
    }

    @AfterAll
    static void tearDownClass() {
        if (emf != null) {
            emf.close();
        }
    }

    @BeforeEach
    void setUp() {
        consumerService = new ConsumerService(emf);
        clearDatabase();
    }

    private void clearDatabase() {
        try (EntityManager em = emf.createEntityManager()) {
            em.getTransaction().begin();
            em.createQuery("DELETE FROM LightMetric").executeUpdate();
            em.createQuery("DELETE FROM BarometerMetric").executeUpdate();
            em.createQuery("DELETE FROM LocationMetric").executeUpdate();
            em.createQuery("DELETE FROM AccelerometerMetric").executeUpdate();
            em.createQuery("DELETE FROM SensorDevice").executeUpdate();
            em.createQuery("DELETE FROM ProcessingState").executeUpdate();
            em.createQuery("DELETE FROM RawSensorMessage").executeUpdate();
            em.getTransaction().commit();
        }
    }

    @Test
    void processBatch_EmptyDatabase_ReturnsZero() {
        int result = consumerService.processBatch();
        assertEquals(0, result);
    }

    @Test
    void processBatch_SingleLightMessage_ProcessedCorrectly() {
        UUID sensorId = UUID.randomUUID();
        LocalDateTime measuredAt = LocalDateTime.now().minusMinutes(5);
        LocalDateTime savedAt = LocalDateTime.now().minusMinutes(4);

        saveRawMessage(sensorId, SensorType.LIGHT, "TestDevice", measuredAt, savedAt, "{\"light\": 512}");

        int processed = consumerService.processBatch();

        assertEquals(1, processed);
        assertEquals(1, consumerService.getProcessedCount());

        try (EntityManager em = emf.createEntityManager()) {
            List<LightMetric> metrics = em.createQuery("SELECT m FROM LightMetric m", LightMetric.class)
                    .getResultList();
            assertEquals(1, metrics.size());
            assertEquals(sensorId, metrics.get(0).getSensorId());
            assertEquals(512, metrics.get(0).getLightValue());

            SensorDevice device = em.find(SensorDevice.class, sensorId);
            assertNotNull(device);
            assertEquals("TestDevice", device.getDeviceName());
            assertEquals(SensorType.LIGHT, device.getSensorType());
        }
    }

    @Test
    void processBatch_SingleBarometerMessage_ProcessedCorrectly() {
        UUID sensorId = UUID.randomUUID();
        LocalDateTime measuredAt = LocalDateTime.now().minusMinutes(5);
        LocalDateTime savedAt = LocalDateTime.now().minusMinutes(4);

        saveRawMessage(sensorId, SensorType.BAROMETER, "BaroDevice", measuredAt, savedAt,
                "{\"air_pressure\": 101325.5}");

        int processed = consumerService.processBatch();

        assertEquals(1, processed);

        try (EntityManager em = emf.createEntityManager()) {
            List<BarometerMetric> metrics = em.createQuery("SELECT m FROM BarometerMetric m", BarometerMetric.class)
                    .getResultList();
            assertEquals(1, metrics.size());
            assertEquals(101325.5, metrics.get(0).getAirPressure(), 0.001);
        }
    }

    @Test
    void processBatch_SingleLocationMessage_ProcessedCorrectly() {
        UUID sensorId = UUID.randomUUID();
        LocalDateTime measuredAt = LocalDateTime.now().minusMinutes(5);
        LocalDateTime savedAt = LocalDateTime.now().minusMinutes(4);

        saveRawMessage(sensorId, SensorType.LOCATION, "LocationDevice", measuredAt, savedAt,
                "{\"latitude\": 55.7558, \"longitude\": 37.6173}");

        int processed = consumerService.processBatch();

        assertEquals(1, processed);

        try (EntityManager em = emf.createEntityManager()) {
            List<LocationMetric> metrics = em.createQuery("SELECT m FROM LocationMetric m", LocationMetric.class)
                    .getResultList();
            assertEquals(1, metrics.size());
            assertEquals(55.7558, metrics.get(0).getLatitude(), 0.0001);
            assertEquals(37.6173, metrics.get(0).getLongitude(), 0.0001);
        }
    }

    @Test
    void processBatch_SingleAccelerometerMessage_ProcessedCorrectly() {
        UUID sensorId = UUID.randomUUID();
        LocalDateTime measuredAt = LocalDateTime.now().minusMinutes(5);
        LocalDateTime savedAt = LocalDateTime.now().minusMinutes(4);

        saveRawMessage(sensorId, SensorType.ACCELEROMETER, "AccelDevice", measuredAt, savedAt,
                "{\"x\": 1.5, \"y\": -2.3, \"z\": 9.8}");

        int processed = consumerService.processBatch();

        assertEquals(1, processed);

        try (EntityManager em = emf.createEntityManager()) {
            List<AccelerometerMetric> metrics = em.createQuery("SELECT m FROM AccelerometerMetric m", AccelerometerMetric.class)
                    .getResultList();
            assertEquals(1, metrics.size());
            assertEquals(1.5, metrics.get(0).getX(), 0.001);
            assertEquals(-2.3, metrics.get(0).getY(), 0.001);
            assertEquals(9.8, metrics.get(0).getZ(), 0.001);
        }
    }

    @Test
    void processBatch_MultipleMessages_AllProcessed() {
        LocalDateTime now = LocalDateTime.now();

        saveRawMessage(UUID.randomUUID(), SensorType.LIGHT, "Device1", now.minusMinutes(5),
                now.minusMinutes(4), "{\"light\": 100}");
        saveRawMessage(UUID.randomUUID(), SensorType.BAROMETER, "Device2", now.minusMinutes(4),
                now.minusMinutes(3), "{\"air_pressure\": 101000.0}");
        saveRawMessage(UUID.randomUUID(), SensorType.LOCATION, "Device3", now.minusMinutes(3),
                now.minusMinutes(2), "{\"latitude\": 50.0, \"longitude\": 30.0}");

        int processed = consumerService.processBatch();

        assertEquals(3, processed);
        assertEquals(3, consumerService.getProcessedCount());
    }

    @Test
    void processBatch_ProcessingStateUpdated() {
        LocalDateTime savedAt = LocalDateTime.now().minusMinutes(1);
        saveRawMessage(UUID.randomUUID(), SensorType.LIGHT, "Device", LocalDateTime.now().minusMinutes(5),
                savedAt, "{\"light\": 500}");

        consumerService.processBatch();

        try (EntityManager em = emf.createEntityManager()) {
            ProcessingState state = em.find(ProcessingState.class, "consumer");
            assertNotNull(state);
            assertEquals(savedAt.withNano(0), state.getLastProcessedTime().withNano(0));
        }
    }

    @Test
    void processBatch_OnlyNewMessages_Processed() {
        LocalDateTime now = LocalDateTime.now();

        setProcessingState(now.minusMinutes(3));

        saveRawMessage(UUID.randomUUID(), SensorType.LIGHT, "OldDevice", now.minusMinutes(10),
                now.minusMinutes(5), "{\"light\": 100}");
        saveRawMessage(UUID.randomUUID(), SensorType.LIGHT, "NewDevice", now.minusMinutes(2),
                now.minusMinutes(1), "{\"light\": 200}");

        int processed = consumerService.processBatch();

        assertEquals(1, processed);

        try (EntityManager em = emf.createEntityManager()) {
            List<LightMetric> metrics = em.createQuery("SELECT m FROM LightMetric m", LightMetric.class)
                    .getResultList();
            assertEquals(1, metrics.size());
            assertEquals(200, metrics.get(0).getLightValue());
        }
    }

    @Test
    void processBatch_InvalidJson_TransactionRolledBack() {
        LocalDateTime now = LocalDateTime.now();

        saveRawMessage(UUID.randomUUID(), SensorType.LIGHT, "GoodDevice", now.minusMinutes(5),
                now.minusMinutes(4), "{\"light\": 100}");
        saveRawMessage(UUID.randomUUID(), SensorType.LIGHT, "BadDevice", now.minusMinutes(3),
                now.minusMinutes(2), "{\"invalid\": \"json\"}");

        assertThrows(Exception.class, () -> consumerService.processBatch());

        try (EntityManager em = emf.createEntityManager()) {
            List<LightMetric> metrics = em.createQuery("SELECT m FROM LightMetric m", LightMetric.class)
                    .getResultList();
            assertEquals(1, metrics.size());

            List<SensorDevice> devices = em.createQuery("SELECT d FROM SensorDevice d", SensorDevice.class)
                    .getResultList();
            assertEquals(1, devices.size());
        }
    }

    private void saveRawMessage(UUID sensorId, SensorType sensorType, String deviceName,
                                 LocalDateTime measuredAt, LocalDateTime savedAt, String jsonValue) {
        try (EntityManager em = emf.createEntityManager()) {
            em.getTransaction().begin();
            RawSensorMessage message = new RawSensorMessage(sensorId, sensorType, deviceName, measuredAt, jsonValue);
            message.setSavedAt(savedAt);
            em.persist(message);
            em.getTransaction().commit();
        }
    }

    private void setProcessingState(LocalDateTime lastProcessedTime) {
        try (EntityManager em = emf.createEntityManager()) {
            em.getTransaction().begin();
            ProcessingState state = new ProcessingState("consumer", lastProcessedTime);
            em.persist(state);
            em.getTransaction().commit();
        }
    }
}