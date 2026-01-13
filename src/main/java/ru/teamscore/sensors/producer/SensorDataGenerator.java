package ru.teamscore.sensors.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.teamscore.sensors.common.SensorType;
import ru.teamscore.sensors.common.entity.RawSensorMessage;

import java.time.LocalDateTime;
import java.util.*;

/**
 * Генератор случайных данных датчиков.
 * Создает RawSensorMessage с рандомными показаниями для различных типов сенсоров.
 */
public class SensorDataGenerator {
    private final Random random = new Random();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<SensorConfig> sensors;

    /**
     * Конфигурация виртуального датчика.
     */
    public record SensorConfig(UUID sensorId, SensorType type, String deviceName) {}

    public SensorDataGenerator() {
        this.sensors = initializeDefaultSensors();
    }

    public SensorDataGenerator(List<SensorConfig> sensors) {
        this.sensors = sensors != null && !sensors.isEmpty() ? sensors : initializeDefaultSensors();
    }

    /**
     * Инициализация датчиков по умолчанию.
     */
    private List<SensorConfig> initializeDefaultSensors() {
        return List.of(
                new SensorConfig(UUID.randomUUID(), SensorType.LIGHT, "SmallRice Pro99"),
                new SensorConfig(UUID.randomUUID(), SensorType.LIGHT, "MyHome ZZZ"),
                new SensorConfig(UUID.randomUUID(), SensorType.BAROMETER, "SmallRice Pro99"),
                new SensorConfig(UUID.randomUUID(), SensorType.BAROMETER, "WeatherStation"),
                new SensorConfig(UUID.randomUUID(), SensorType.LOCATION, "SmallRice Pro99"),
                new SensorConfig(UUID.randomUUID(), SensorType.LOCATION, "GPSTracker"),
                new SensorConfig(UUID.randomUUID(), SensorType.ACCELEROMETER, "SmallRice Pro99"),
                new SensorConfig(UUID.randomUUID(), SensorType.ACCELEROMETER, "FitnessBand")
        );
    }

    /**
     * Генерирует случайное сообщение от случайного датчика.
     */
    public RawSensorMessage generateRandomMessage() {
        SensorConfig sensor = sensors.get(random.nextInt(sensors.size()));
        return generateMessage(sensor);
    }

    /**
     * Генерирует сообщение от конкретного датчика.
     */
    public RawSensorMessage generateMessage(SensorConfig sensor) {
        LocalDateTime measuredAt = LocalDateTime.now();
        String jsonValue = generateJsonValue(sensor.type());

        return new RawSensorMessage(
                sensor.sensorId(),
                sensor.type(),
                sensor.deviceName(),
                measuredAt,
                jsonValue
        );
    }

    /**
     * Генерирует JSON с показаниями в зависимости от типа датчика.
     */
    private String generateJsonValue(SensorType type) {
        try {
            Map<String, Object> values = new HashMap<>();
            switch (type) {
                case LIGHT -> values.put("light", random.nextInt(1024)); // 0-1023
                case BAROMETER -> values.put("air_pressure", 95000 + random.nextDouble() * 15000); // 95000-110000 Па
                case LOCATION -> {
                    values.put("latitude", -90 + random.nextDouble() * 180); // -90 to 90
                    values.put("longitude", -180 + random.nextDouble() * 360); // -180 to 180
                }
                case ACCELEROMETER -> {
                    values.put("x", -10 + random.nextDouble() * 20); // -10 to 10 м/с
                    values.put("y", -10 + random.nextDouble() * 20);
                    values.put("z", -10 + random.nextDouble() * 20);
                }
            }
            return objectMapper.writeValueAsString(values);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to generate JSON for sensor type: " + type, e);
        }
    }

    /**
     * Возвращает список настроенных датчиков.
     */
    public List<SensorConfig> getSensors() {
        return Collections.unmodifiableList(sensors);
    }
}