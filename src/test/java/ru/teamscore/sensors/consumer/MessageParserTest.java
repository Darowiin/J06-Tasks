package ru.teamscore.sensors.consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.teamscore.sensors.common.SensorType;
import ru.teamscore.sensors.common.entity.RawSensorMessage;
import ru.teamscore.sensors.common.entity.metric.*;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MessageParserTest {

    private MessageParser parser;

    @BeforeEach
    void setUp() {
        parser = new MessageParser();
    }

    @Test
    void parseMessage_NullMessage_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> parser.parseMessage(null));
    }

    @Test
    void parseMessage_NullSensorType_ThrowsException() {
        RawSensorMessage message = new RawSensorMessage(
                UUID.randomUUID(), null, "Device", LocalDateTime.now(), "{\"light\": 500}");
        assertThrows(IllegalArgumentException.class, () -> parser.parseMessage(message));
    }

    @Test
    void parseMessage_NullJson_ThrowsException() {
        RawSensorMessage message = new RawSensorMessage(
                UUID.randomUUID(), SensorType.LIGHT, "Device", LocalDateTime.now(), null);
        assertThrows(IllegalArgumentException.class, () -> parser.parseMessage(message));
    }

    @Test
    void parseMessage_EmptyJson_ThrowsException() {
        RawSensorMessage message = new RawSensorMessage(
                UUID.randomUUID(), SensorType.LIGHT, "Device", LocalDateTime.now(), "");
        assertThrows(IllegalArgumentException.class, () -> parser.parseMessage(message));
    }

    @Test
    void parseMessage_InvalidJson_ThrowsException() {
        RawSensorMessage message = new RawSensorMessage(
                UUID.randomUUID(), SensorType.LIGHT, "Device", LocalDateTime.now(), "not a json");
        assertThrows(IllegalArgumentException.class, () -> parser.parseMessage(message));
    }

    @Test
    void parseMessage_LightMetric_Success() {
        UUID sensorId = UUID.randomUUID();
        LocalDateTime measuredAt = LocalDateTime.of(2025, 12, 17, 12, 33, 56, 123000000);
        RawSensorMessage message = new RawSensorMessage(
                sensorId, SensorType.LIGHT, "Device1", measuredAt, "{\"light\": 512}");

        SensorMetric result = parser.parseMessage(message);

        assertInstanceOf(LightMetric.class, result);
        LightMetric lightMetric = (LightMetric) result;
        assertEquals(sensorId, lightMetric.getSensorId());
        assertEquals(measuredAt, lightMetric.getMeasuredAt());
        assertEquals(512, lightMetric.getLightValue());
    }

    @Test
    void parseMessage_LightMetric_MissingField_ThrowsException() {
        RawSensorMessage message = new RawSensorMessage(
                UUID.randomUUID(), SensorType.LIGHT, "Device", LocalDateTime.now(), "{\"other\": 500}");
        assertThrows(IllegalArgumentException.class, () -> parser.parseMessage(message));
    }

    @Test
    void parseMessage_BarometerMetric_Success() {
        UUID sensorId = UUID.randomUUID();
        LocalDateTime measuredAt = LocalDateTime.of(2025, 12, 17, 12, 33, 56, 123000000);
        RawSensorMessage message = new RawSensorMessage(
                sensorId, SensorType.BAROMETER, "Device2", measuredAt, "{\"air_pressure\": 101325.5}");

        SensorMetric result = parser.parseMessage(message);

        assertInstanceOf(BarometerMetric.class, result);
        BarometerMetric barometerMetric = (BarometerMetric) result;
        assertEquals(sensorId, barometerMetric.getSensorId());
        assertEquals(measuredAt, barometerMetric.getMeasuredAt());
        assertEquals(101325.5, barometerMetric.getAirPressure(), 0.001);
    }

    @Test
    void parseMessage_BarometerMetric_MissingField_ThrowsException() {
        RawSensorMessage message = new RawSensorMessage(
                UUID.randomUUID(), SensorType.BAROMETER, "Device", LocalDateTime.now(), "{\"pressure\": 101325.0}");
        assertThrows(IllegalArgumentException.class, () -> parser.parseMessage(message));
    }

    @Test
    void parseMessage_LocationMetric_Success() {
        UUID sensorId = UUID.randomUUID();
        LocalDateTime measuredAt = LocalDateTime.of(2025, 12, 17, 12, 33, 56, 123000000);
        RawSensorMessage message = new RawSensorMessage(
                sensorId, SensorType.LOCATION, "Device3", measuredAt,
                "{\"latitude\": 55.7558, \"longitude\": 37.6173}");

        SensorMetric result = parser.parseMessage(message);

        assertInstanceOf(LocationMetric.class, result);
        LocationMetric locationMetric = (LocationMetric) result;
        assertEquals(sensorId, locationMetric.getSensorId());
        assertEquals(measuredAt, locationMetric.getMeasuredAt());
        assertEquals(55.7558, locationMetric.getLatitude(), 0.0001);
        assertEquals(37.6173, locationMetric.getLongitude(), 0.0001);
    }

    @Test
    void parseMessage_LocationMetric_MissingLatitude_ThrowsException() {
        RawSensorMessage message = new RawSensorMessage(
                UUID.randomUUID(), SensorType.LOCATION, "Device", LocalDateTime.now(),
                "{\"longitude\": 37.6173}");
        assertThrows(IllegalArgumentException.class, () -> parser.parseMessage(message));
    }

    @Test
    void parseMessage_LocationMetric_MissingLongitude_ThrowsException() {
        RawSensorMessage message = new RawSensorMessage(
                UUID.randomUUID(), SensorType.LOCATION, "Device", LocalDateTime.now(),
                "{\"latitude\": 55.7558}");
        assertThrows(IllegalArgumentException.class, () -> parser.parseMessage(message));
    }

    @Test
    void parseMessage_AccelerometerMetric_Success() {
        UUID sensorId = UUID.randomUUID();
        LocalDateTime measuredAt = LocalDateTime.of(2025, 12, 17, 12, 33, 56, 123000000);
        RawSensorMessage message = new RawSensorMessage(
                sensorId, SensorType.ACCELEROMETER, "Device4", measuredAt,
                "{\"x\": 125.1, \"y\": -0.15, \"z\": 92.0002}");

        SensorMetric result = parser.parseMessage(message);

        assertInstanceOf(AccelerometerMetric.class, result);
        AccelerometerMetric accelMetric = (AccelerometerMetric) result;
        assertEquals(sensorId, accelMetric.getSensorId());
        assertEquals(measuredAt, accelMetric.getMeasuredAt());
        assertEquals(125.1, accelMetric.getX(), 0.0001);
        assertEquals(-0.15, accelMetric.getY(), 0.0001);
        assertEquals(92.0002, accelMetric.getZ(), 0.0001);
    }

    @Test
    void parseMessage_AccelerometerMetric_MissingX_ThrowsException() {
        RawSensorMessage message = new RawSensorMessage(
                UUID.randomUUID(), SensorType.ACCELEROMETER, "Device", LocalDateTime.now(),
                "{\"y\": -0.15, \"z\": 92.0}");
        assertThrows(IllegalArgumentException.class, () -> parser.parseMessage(message));
    }

    @Test
    void parseMessage_AccelerometerMetric_MissingY_ThrowsException() {
        RawSensorMessage message = new RawSensorMessage(
                UUID.randomUUID(), SensorType.ACCELEROMETER, "Device", LocalDateTime.now(),
                "{\"x\": 125.1, \"z\": 92.0}");
        assertThrows(IllegalArgumentException.class, () -> parser.parseMessage(message));
    }

    @Test
    void parseMessage_AccelerometerMetric_MissingZ_ThrowsException() {
        RawSensorMessage message = new RawSensorMessage(
                UUID.randomUUID(), SensorType.ACCELEROMETER, "Device", LocalDateTime.now(),
                "{\"x\": 125.1, \"y\": -0.15}");
        assertThrows(IllegalArgumentException.class, () -> parser.parseMessage(message));
    }

    @Test
    void parseMessage_LightMetric_BoundaryValues() {
        RawSensorMessage minMessage = new RawSensorMessage(
                UUID.randomUUID(), SensorType.LIGHT, "Device", LocalDateTime.now(), "{\"light\": 0}");
        LightMetric minMetric = (LightMetric) parser.parseMessage(minMessage);
        assertEquals(0, minMetric.getLightValue());

        RawSensorMessage maxMessage = new RawSensorMessage(
                UUID.randomUUID(), SensorType.LIGHT, "Device", LocalDateTime.now(), "{\"light\": 1023}");
        LightMetric maxMetric = (LightMetric) parser.parseMessage(maxMessage);
        assertEquals(1023, maxMetric.getLightValue());
    }
}