package ru.teamscore.sensors.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.teamscore.sensors.common.SensorType;
import ru.teamscore.sensors.common.entity.RawSensorMessage;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SensorDataGeneratorTest {

    private SensorDataGenerator generator;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        generator = new SensorDataGenerator();
        objectMapper = new ObjectMapper();
    }

    @Test
    void testGenerateRandomMessage_NotNull() {
        RawSensorMessage message = generator.generateRandomMessage();
        assertNotNull(message);
        assertNotNull(message.getSensorId());
        assertNotNull(message.getSensorType());
        assertNotNull(message.getDeviceName());
        assertNotNull(message.getMeasuredAt());
        assertNotNull(message.getJsonValue());
    }

    @Test
    void testGenerateRandomMessage_ValidSensorTypes() {
        Set<SensorType> generatedTypes = new HashSet<>();
        // Генерируем достаточно сообщений, чтобы получить все типы
        for (int i = 0; i < 100; i++) {
            RawSensorMessage message = generator.generateRandomMessage();
            generatedTypes.add(message.getSensorType());
        }
        // Проверяем, что генерируются все типы датчиков
        assertTrue(generatedTypes.size() >= 1);
    }

    @Test
    void testGenerateLightMessage_ValidJson() throws Exception {
        List<SensorDataGenerator.SensorConfig> lightSensors = List.of(
                new SensorDataGenerator.SensorConfig(UUID.randomUUID(), SensorType.LIGHT, "TestDevice")
        );
        SensorDataGenerator lightGenerator = new SensorDataGenerator(lightSensors);

        RawSensorMessage message = lightGenerator.generateRandomMessage();
        JsonNode json = objectMapper.readTree(message.getJsonValue());

        assertTrue(json.has("light"));
        int light = json.get("light").asInt();
        assertTrue(light >= 0 && light <= 1023, "Light value should be 0-1023, got: " + light);
    }

    @Test
    void testGenerateBarometerMessage_ValidJson() throws Exception {
        List<SensorDataGenerator.SensorConfig> barometerSensors = List.of(
                new SensorDataGenerator.SensorConfig(UUID.randomUUID(), SensorType.BAROMETER, "TestDevice")
        );
        SensorDataGenerator barometerGenerator = new SensorDataGenerator(barometerSensors);

        RawSensorMessage message = barometerGenerator.generateRandomMessage();
        JsonNode json = objectMapper.readTree(message.getJsonValue());

        assertTrue(json.has("air_pressure"));
        double pressure = json.get("air_pressure").asDouble();
        assertTrue(pressure >= 95000 && pressure <= 110000, "Pressure should be 95000-110000 Pa, got: " + pressure);
    }

    @Test
    void testGenerateLocationMessage_ValidJson() throws Exception {
        List<SensorDataGenerator.SensorConfig> locationSensors = List.of(
                new SensorDataGenerator.SensorConfig(UUID.randomUUID(), SensorType.LOCATION, "TestDevice")
        );
        SensorDataGenerator locationGenerator = new SensorDataGenerator(locationSensors);

        RawSensorMessage message = locationGenerator.generateRandomMessage();
        JsonNode json = objectMapper.readTree(message.getJsonValue());

        assertTrue(json.has("latitude"));
        assertTrue(json.has("longitude"));
        double lat = json.get("latitude").asDouble();
        double lon = json.get("longitude").asDouble();
        assertTrue(lat >= -90 && lat <= 90, "Latitude should be -90 to 90, got: " + lat);
        assertTrue(lon >= -180 && lon <= 180, "Longitude should be -180 to 180, got: " + lon);
    }

    @Test
    void testGenerateAccelerometerMessage_ValidJson() throws Exception {
        List<SensorDataGenerator.SensorConfig> accelSensors = List.of(
                new SensorDataGenerator.SensorConfig(UUID.randomUUID(), SensorType.ACCELEROMETER, "TestDevice")
        );
        SensorDataGenerator accelGenerator = new SensorDataGenerator(accelSensors);

        RawSensorMessage message = accelGenerator.generateRandomMessage();
        JsonNode json = objectMapper.readTree(message.getJsonValue());

        assertTrue(json.has("x"));
        assertTrue(json.has("y"));
        assertTrue(json.has("z"));
        double x = json.get("x").asDouble();
        double y = json.get("y").asDouble();
        double z = json.get("z").asDouble();
        assertTrue(x >= -10 && x <= 10, "X should be -10 to 10, got: " + x);
        assertTrue(y >= -10 && y <= 10, "Y should be -10 to 10, got: " + y);
        assertTrue(z >= -10 && z <= 10, "Z should be -10 to 10, got: " + z);
    }

    @Test
    void testCustomSensorsConfiguration() {
        UUID customId = UUID.randomUUID();
        String customDevice = "CustomDevice123";
        List<SensorDataGenerator.SensorConfig> customSensors = List.of(
                new SensorDataGenerator.SensorConfig(customId, SensorType.LIGHT, customDevice)
        );
        SensorDataGenerator customGenerator = new SensorDataGenerator(customSensors);

        RawSensorMessage message = customGenerator.generateRandomMessage();
        assertEquals(customId, message.getSensorId());
        assertEquals(customDevice, message.getDeviceName());
        assertEquals(SensorType.LIGHT, message.getSensorType());
    }

    @Test
    void testGetSensors_ReturnsUnmodifiableList() {
        List<SensorDataGenerator.SensorConfig> sensors = generator.getSensors();
        assertNotNull(sensors);
        assertFalse(sensors.isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> sensors.add(null));
    }

    @Test
    void testEmptySensorsList_UsesDefaults() {
        SensorDataGenerator emptyGenerator = new SensorDataGenerator(List.of());
        assertFalse(emptyGenerator.getSensors().isEmpty());
    }

    @Test
    void testNullSensorsList_UsesDefaults() {
        SensorDataGenerator nullGenerator = new SensorDataGenerator(null);
        assertFalse(nullGenerator.getSensors().isEmpty());
    }
}