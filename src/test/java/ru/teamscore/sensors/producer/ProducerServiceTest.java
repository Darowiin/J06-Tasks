package ru.teamscore.sensors.producer;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.TypedQuery;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.*;
import ru.teamscore.sensors.common.SensorType;
import ru.teamscore.sensors.common.entity.ProcessingState;
import ru.teamscore.sensors.common.entity.RawSensorMessage;
import ru.teamscore.sensors.common.entity.SensorDevice;
import ru.teamscore.sensors.common.entity.metric.AccelerometerMetric;
import ru.teamscore.sensors.common.entity.metric.BarometerMetric;
import ru.teamscore.sensors.common.entity.metric.LightMetric;
import ru.teamscore.sensors.common.entity.metric.LocationMetric;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ProducerServiceTest {

    private static EntityManagerFactory emf;
    private ProducerService producerService;
    private SensorDataGenerator generator;

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
        if (emf != null && emf.isOpen()) {
            emf.close();
        }
    }

    @BeforeEach
    void setUp() {
        generator = new SensorDataGenerator(List.of(
                new SensorDataGenerator.SensorConfig(UUID.randomUUID(), SensorType.LIGHT, "TestDevice")
        ));
        producerService = new ProducerService(emf, generator, 10, 50);

        clearDatabase();
    }

    @AfterEach
    void tearDown() {
        if (producerService.isRunning()) {
            producerService.stop();
        }
    }

    private void clearDatabase() {
        try (EntityManager em = emf.createEntityManager()) {
            em.getTransaction().begin();
            em.createQuery("DELETE FROM RawSensorMessage").executeUpdate();
            em.getTransaction().commit();
        }
    }

    @Test
    void testProduceOne_SavesMessageToDatabase() {
        RawSensorMessage message = producerService.produceOne();

        assertNotNull(message);
        assertEquals(1, producerService.getMessageCount());

        try (EntityManager em = emf.createEntityManager()) {
            TypedQuery<RawSensorMessage> query = em.createQuery(
                    "SELECT m FROM RawSensorMessage m WHERE m.sensorId = :sensorId",
                    RawSensorMessage.class);
            query.setParameter("sensorId", message.getSensorId());
            List<RawSensorMessage> results = query.getResultList();

            assertEquals(1, results.size());
            assertEquals(message.getSensorType(), results.get(0).getSensorType());
            assertEquals(message.getDeviceName(), results.get(0).getDeviceName());
        }
    }

    @Test
    void testProduceMultipleMessages() {
        for (int i = 0; i < 5; i++) {
            producerService.produceOne();
        }

        assertEquals(5, producerService.getMessageCount());

        try (EntityManager em = emf.createEntityManager()) {
            Long count = em.createQuery("SELECT COUNT(m) FROM RawSensorMessage m", Long.class)
                    .getSingleResult();
            assertEquals(5L, count);
        }
    }

    @Test
    void testIsRunning_InitiallyFalse() {
        assertFalse(producerService.isRunning());
    }

    @Test
    void testStartAndStop() throws InterruptedException {
        Thread producerThread = new Thread(producerService::start);
        producerThread.start();

        Thread.sleep(100);
        assertTrue(producerService.isRunning());

        producerService.stop();
        producerThread.join(2000);

        assertFalse(producerService.isRunning());
        assertTrue(producerService.getMessageCount() > 0, "Should have produced at least one message");
    }

    @Test
    void testMessageCountIncrementsOnProduce() {
        assertEquals(0, producerService.getMessageCount());

        producerService.produceOne();
        assertEquals(1, producerService.getMessageCount());

        producerService.produceOne();
        assertEquals(2, producerService.getMessageCount());

        producerService.produceOne();
        assertEquals(3, producerService.getMessageCount());
    }

    @Test
    void testSavedAtIsSet() {
        RawSensorMessage message = producerService.produceOne();

        try (EntityManager em = emf.createEntityManager()) {
            RawSensorMessage saved = em.find(RawSensorMessage.class, message.getId());
            assertNotNull(saved.getSavedAt());
        }
    }

    @Test
    void testJsonValueIsStored() {
        RawSensorMessage message = producerService.produceOne();

        try (EntityManager em = emf.createEntityManager()) {
            RawSensorMessage saved = em.find(RawSensorMessage.class, message.getId());
            assertNotNull(saved.getJsonValue());
            assertTrue(saved.getJsonValue().contains("light"));
        }
    }
}