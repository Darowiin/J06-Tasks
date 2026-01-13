package ru.teamscore.sensors.aggregator;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.*;
import ru.teamscore.sensors.common.SensorType;
import ru.teamscore.sensors.common.entity.ProcessingState;
import ru.teamscore.sensors.common.entity.RawSensorMessage;
import ru.teamscore.sensors.common.entity.SensorDevice;
import ru.teamscore.sensors.common.entity.metric.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class AggregatorServiceTest {

    private static EntityManagerFactory emf;
    private AggregatorService aggregatorService;
    private ByteArrayOutputStream outputStream;
    private PrintStream printStream;

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
        outputStream = new ByteArrayOutputStream();
        printStream = new PrintStream(outputStream);
        ByteArrayInputStream inputStream = new ByteArrayInputStream("\n\n\n\n\n".getBytes());
        aggregatorService = new AggregatorService(emf, printStream, inputStream);
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
    void fetchAggregatedData_EmptyDatabase_ReturnsEmptyList() {
        LocalDateTime start = LocalDateTime.of(2025, 12, 1, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2025, 12, 31, 23, 59, 59);

        List<AggregatedResult> results = aggregatorService.fetchAggregatedData(
                SensorType.LIGHT, start, end, TimeInterval.HOUR, null);

        assertTrue(results.isEmpty());
    }

    @Test
    void fetchAggregatedData_LightMetrics_ReturnsAggregatedData() {
        UUID sensorId = UUID.randomUUID();
        LocalDateTime hour1 = LocalDateTime.of(2025, 12, 1, 10, 0, 0);
        LocalDateTime hour1_30 = LocalDateTime.of(2025, 12, 1, 10, 30, 0);
        LocalDateTime hour2 = LocalDateTime.of(2025, 12, 1, 11, 0, 0);

        createDevice(sensorId, "TestDevice", SensorType.LIGHT);
        createLightMetric(sensorId, hour1, 100);
        createLightMetric(sensorId, hour1_30, 200);
        createLightMetric(sensorId, hour2, 300);

        LocalDateTime start = LocalDateTime.of(2025, 12, 1, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2025, 12, 1, 23, 59, 59);

        List<AggregatedResult> results = aggregatorService.fetchAggregatedData(
                SensorType.LIGHT, start, end, TimeInterval.HOUR, null);

        assertEquals(2, results.size());
        assertEquals("TestDevice", results.get(0).getDeviceName());
        assertEquals(LocalDateTime.of(2025, 12, 1, 11, 0, 0), results.get(0).getIntervalStart());
        assertEquals(300.0, results.get(0).getValue(), 0.001);

        assertEquals("TestDevice", results.get(1).getDeviceName());
        assertEquals(LocalDateTime.of(2025, 12, 1, 10, 0, 0), results.get(1).getIntervalStart());
        assertEquals(150.0, results.get(1).getValue(), 0.001); // (100 + 200) / 2
    }

    @Test
    void fetchAggregatedData_BarometerMetrics_ReturnsAggregatedData() {
        UUID sensorId = UUID.randomUUID();
        LocalDateTime time1 = LocalDateTime.of(2025, 12, 1, 10, 15, 0);
        LocalDateTime time2 = LocalDateTime.of(2025, 12, 1, 10, 45, 0);

        createDevice(sensorId, "BaroDevice", SensorType.BAROMETER);
        createBarometerMetric(sensorId, time1, 101325.0);
        createBarometerMetric(sensorId, time2, 101350.0);

        LocalDateTime start = LocalDateTime.of(2025, 12, 1, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2025, 12, 1, 23, 59, 59);

        List<AggregatedResult> results = aggregatorService.fetchAggregatedData(
                SensorType.BAROMETER, start, end, TimeInterval.HOUR, null);

        assertEquals(1, results.size());
        assertEquals("BaroDevice", results.get(0).getDeviceName());
        assertEquals(101337.5, results.get(0).getValue(), 0.001);
    }

    @Test
    void fetchAggregatedData_LocationMetrics_ReturnsTwoValues() {
        UUID sensorId = UUID.randomUUID();
        LocalDateTime time1 = LocalDateTime.of(2025, 12, 1, 10, 10, 0);
        LocalDateTime time2 = LocalDateTime.of(2025, 12, 1, 10, 20, 0);

        createDevice(sensorId, "LocationDevice", SensorType.LOCATION);
        createLocationMetric(sensorId, time1, 55.7558, 37.6173);
        createLocationMetric(sensorId, time2, 55.7600, 37.6200);

        LocalDateTime start = LocalDateTime.of(2025, 12, 1, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2025, 12, 1, 23, 59, 59);

        List<AggregatedResult> results = aggregatorService.fetchAggregatedData(
                SensorType.LOCATION, start, end, TimeInterval.HOUR, null);

        assertEquals(1, results.size());
        Double[] values = results.get(0).getValues();
        assertEquals(2, values.length);
        assertEquals(55.7579, values[0], 0.001);
        assertEquals(37.61865, values[1], 0.001);
    }

    @Test
    void fetchAggregatedData_AccelerometerMetrics_ReturnsThreeValues() {
        UUID sensorId = UUID.randomUUID();
        LocalDateTime time1 = LocalDateTime.of(2025, 12, 1, 10, 5, 0);
        LocalDateTime time2 = LocalDateTime.of(2025, 12, 1, 10, 10, 0);

        createDevice(sensorId, "AccelDevice", SensorType.ACCELEROMETER);
        createAccelerometerMetric(sensorId, time1, 1.0, 2.0, 3.0);
        createAccelerometerMetric(sensorId, time2, 3.0, 4.0, 5.0);

        LocalDateTime start = LocalDateTime.of(2025, 12, 1, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2025, 12, 1, 23, 59, 59);

        List<AggregatedResult> results = aggregatorService.fetchAggregatedData(
                SensorType.ACCELEROMETER, start, end, TimeInterval.HOUR, null);

        assertEquals(1, results.size());
        Double[] values = results.get(0).getValues();
        assertEquals(3, values.length);
        assertEquals(2.0, values[0], 0.001);
        assertEquals(3.0, values[1], 0.001);
        assertEquals(4.0, values[2], 0.001);
    }

    @Test
    void fetchAggregatedData_MultipleDevices_SortedByDeviceName() {
        UUID sensor1 = UUID.randomUUID();
        UUID sensor2 = UUID.randomUUID();
        LocalDateTime time = LocalDateTime.of(2025, 12, 1, 10, 0, 0);

        createDevice(sensor1, "Zebra Device", SensorType.LIGHT);
        createDevice(sensor2, "Alpha Device", SensorType.LIGHT);
        createLightMetric(sensor1, time, 500);
        createLightMetric(sensor2, time, 700);

        LocalDateTime start = LocalDateTime.of(2025, 12, 1, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2025, 12, 1, 23, 59, 59);

        List<AggregatedResult> results = aggregatorService.fetchAggregatedData(
                SensorType.LIGHT, start, end, TimeInterval.HOUR, null);

        assertEquals(2, results.size());
        assertEquals("Alpha Device", results.get(0).getDeviceName());
        assertEquals("Zebra Device", results.get(1).getDeviceName());
    }

    @Test
    void fetchAggregatedData_FilterByDeviceName_ReturnsOnlyMatchingDevice() {
        UUID sensor1 = UUID.randomUUID();
        UUID sensor2 = UUID.randomUUID();
        LocalDateTime time = LocalDateTime.of(2025, 12, 1, 10, 0, 0);

        createDevice(sensor1, "MyDevice", SensorType.LIGHT);
        createDevice(sensor2, "OtherDevice", SensorType.LIGHT);
        createLightMetric(sensor1, time, 500);
        createLightMetric(sensor2, time, 700);

        LocalDateTime start = LocalDateTime.of(2025, 12, 1, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2025, 12, 1, 23, 59, 59);

        List<AggregatedResult> results = aggregatorService.fetchAggregatedData(
                SensorType.LIGHT, start, end, TimeInterval.HOUR, "MyDevice");

        assertEquals(1, results.size());
        assertEquals("MyDevice", results.get(0).getDeviceName());
        assertEquals(500.0, results.get(0).getValue(), 0.001);
    }

    @Test
    void fetchAggregatedData_FilterByTimeRange_ReturnsOnlyInRange() {
        UUID sensorId = UUID.randomUUID();
        createDevice(sensorId, "TestDevice", SensorType.LIGHT);

        createLightMetric(sensorId, LocalDateTime.of(2025, 11, 30, 23, 59, 0), 100);
        createLightMetric(sensorId, LocalDateTime.of(2025, 12, 1, 10, 0, 0), 200);
        createLightMetric(sensorId, LocalDateTime.of(2025, 12, 1, 11, 0, 0), 300);
        createLightMetric(sensorId, LocalDateTime.of(2025, 12, 2, 0, 1, 0), 400);

        LocalDateTime start = LocalDateTime.of(2025, 12, 1, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2025, 12, 1, 23, 59, 59);

        List<AggregatedResult> results = aggregatorService.fetchAggregatedData(
                SensorType.LIGHT, start, end, TimeInterval.HOUR, null);

        assertEquals(2, results.size());
    }

    @Test
    void aggregate_PrintsTableWithHeaders() {
        UUID sensorId = UUID.randomUUID();
        createDevice(sensorId, "MyHome ZZZ", SensorType.LIGHT);
        createLightMetric(sensorId, LocalDateTime.of(2025, 12, 1, 10, 30, 0), 1023);

        LocalDateTime start = LocalDateTime.of(2025, 12, 1, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2025, 12, 1, 23, 59, 59);

        aggregatorService.aggregate(SensorType.LIGHT, start, end, TimeInterval.HOUR, null);

        String output = outputStream.toString();
        assertTrue(output.contains("DEVICE"), "Output should contain DEVICE header");
        assertTrue(output.contains("DATE"), "Output should contain DATE header");
        assertTrue(output.contains("LIGHT"), "Output should contain LIGHT header");
        assertTrue(output.contains("MyHome ZZZ"), "Output should contain device name");
        assertTrue(output.contains("1023.00"), "Output should contain light value: " + output);
    }

    @Test
    void aggregate_EmptyData_PrintsNoDataMessage() {
        LocalDateTime start = LocalDateTime.of(2025, 12, 1, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2025, 12, 1, 23, 59, 59);

        aggregatorService.aggregate(SensorType.LIGHT, start, end, TimeInterval.HOUR, null);

        String output = outputStream.toString();
        assertTrue(output.contains("Нет данных"));
    }

    @Test
    void fetchAggregatedData_MinuteInterval_GroupsByMinute() {
        UUID sensorId = UUID.randomUUID();
        createDevice(sensorId, "TestDevice", SensorType.LIGHT);

        createLightMetric(sensorId, LocalDateTime.of(2025, 12, 1, 10, 5, 10), 100);
        createLightMetric(sensorId, LocalDateTime.of(2025, 12, 1, 10, 5, 50), 200);
        createLightMetric(sensorId, LocalDateTime.of(2025, 12, 1, 10, 6, 30), 300);

        LocalDateTime start = LocalDateTime.of(2025, 12, 1, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2025, 12, 1, 23, 59, 59);

        List<AggregatedResult> results = aggregatorService.fetchAggregatedData(
                SensorType.LIGHT, start, end, TimeInterval.MINUTE, null);

        assertEquals(2, results.size());
        assertEquals(LocalDateTime.of(2025, 12, 1, 10, 6, 0), results.get(0).getIntervalStart());
        assertEquals(300.0, results.get(0).getValue(), 0.001);
        assertEquals(LocalDateTime.of(2025, 12, 1, 10, 5, 0), results.get(1).getIntervalStart());
        assertEquals(150.0, results.get(1).getValue(), 0.001);
    }

    @Test
    void fetchAggregatedData_DayInterval_GroupsByDay() {
        UUID sensorId = UUID.randomUUID();
        createDevice(sensorId, "TestDevice", SensorType.LIGHT);

        createLightMetric(sensorId, LocalDateTime.of(2025, 12, 1, 0, 0, 0), 100);
        createLightMetric(sensorId, LocalDateTime.of(2025, 12, 1, 23, 59, 59), 200);
        createLightMetric(sensorId, LocalDateTime.of(2025, 12, 2, 12, 0, 0), 300);

        LocalDateTime start = LocalDateTime.of(2025, 12, 1, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2025, 12, 31, 23, 59, 59);

        List<AggregatedResult> results = aggregatorService.fetchAggregatedData(
                SensorType.LIGHT, start, end, TimeInterval.DAY, null);

        assertEquals(2, results.size());
    }

    private void createDevice(UUID sensorId, String deviceName, SensorType sensorType) {
        try (EntityManager em = emf.createEntityManager()) {
            em.getTransaction().begin();
            SensorDevice device = new SensorDevice(sensorId, deviceName, sensorType, LocalDateTime.now());
            em.persist(device);
            em.getTransaction().commit();
        }
    }

    private void createLightMetric(UUID sensorId, LocalDateTime measuredAt, int lightValue) {
        try (EntityManager em = emf.createEntityManager()) {
            em.getTransaction().begin();
            LightMetric metric = new LightMetric(sensorId, measuredAt, lightValue);
            em.persist(metric);
            em.getTransaction().commit();
        }
    }

    private void createBarometerMetric(UUID sensorId, LocalDateTime measuredAt, double airPressure) {
        try (EntityManager em = emf.createEntityManager()) {
            em.getTransaction().begin();
            BarometerMetric metric = new BarometerMetric(sensorId, measuredAt, airPressure);
            em.persist(metric);
            em.getTransaction().commit();
        }
    }

    private void createLocationMetric(UUID sensorId, LocalDateTime measuredAt, double latitude, double longitude) {
        try (EntityManager em = emf.createEntityManager()) {
            em.getTransaction().begin();
            LocationMetric metric = new LocationMetric(sensorId, measuredAt, latitude, longitude);
            em.persist(metric);
            em.getTransaction().commit();
        }
    }

    private void createAccelerometerMetric(UUID sensorId, LocalDateTime measuredAt, double x, double y, double z) {
        try (EntityManager em = emf.createEntityManager()) {
            em.getTransaction().begin();
            AccelerometerMetric metric = new AccelerometerMetric(sensorId, measuredAt, x, y, z);
            em.persist(metric);
            em.getTransaction().commit();
        }
    }
}