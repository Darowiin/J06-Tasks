package ru.teamscore.sensors.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.teamscore.sensors.common.entity.RawSensorMessage;
import ru.teamscore.sensors.common.entity.metric.*;

/**
 * Парсер сообщений от датчиков.
 * Извлекает данные из JSON и создаёт соответствующие метрики.
 */
public class MessageParser {
    private final ObjectMapper objectMapper;

    public MessageParser() {
        this.objectMapper = new ObjectMapper();
    }

    public MessageParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Парсит сырое сообщение и создаёт соответствующую метрику.
     * @param message сырое сообщение
     * @return метрика соответствующего типа
     * @throws IllegalArgumentException если тип датчика неизвестен или JSON некорректен
     */
    public SensorMetric parseMessage(RawSensorMessage message) {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        if (message.getSensorType() == null) {
            throw new IllegalArgumentException("Sensor type cannot be null");
        }
        if (message.getJsonValue() == null || message.getJsonValue().isEmpty()) {
            throw new IllegalArgumentException("JSON value cannot be null or empty");
        }

        try {
            JsonNode json = objectMapper.readTree(message.getJsonValue());

            return switch (message.getSensorType()) {
                case LIGHT -> parseLightMetric(message, json);
                case BAROMETER -> parseBarometerMetric(message, json);
                case LOCATION -> parseLocationMetric(message, json);
                case ACCELEROMETER -> parseAccelerometerMetric(message, json);
            };
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid JSON: " + e.getMessage(), e);
        }
    }

    /**
     * Парсит метрику освещённости.
     * Ожидаемый формат JSON: { "light": 512 }
     */
    private LightMetric parseLightMetric(RawSensorMessage message, JsonNode json) {
        if (!json.has("light")) {
            throw new IllegalArgumentException("Light metric JSON must contain 'light' field");
        }

        int lightValue = json.get("light").asInt();
        return new LightMetric(message.getSensorId(), message.getMeasuredAt(), lightValue);
    }

    /**
     * Парсит метрику барометра.
     * Ожидаемый формат JSON: { "air_pressure": 101325.0 }
     */
    private BarometerMetric parseBarometerMetric(RawSensorMessage message, JsonNode json) {
        if (!json.has("air_pressure")) {
            throw new IllegalArgumentException("Barometer metric JSON must contain 'air_pressure' field");
        }

        double airPressure = json.get("air_pressure").asDouble();
        return new BarometerMetric(message.getSensorId(), message.getMeasuredAt(), airPressure);
    }

    /**
     * Парсит метрику местоположения.
     * Ожидаемый формат JSON: { "latitude": 55.7558, "longitude": 37.6173 }
     */
    private LocationMetric parseLocationMetric(RawSensorMessage message, JsonNode json) {
        if (!json.has("latitude") || !json.has("longitude")) {
            throw new IllegalArgumentException("Location metric JSON must contain 'latitude' and 'longitude' fields");
        }

        double latitude = json.get("latitude").asDouble();
        double longitude = json.get("longitude").asDouble();
        return new LocationMetric(message.getSensorId(), message.getMeasuredAt(), latitude, longitude);
    }

    /**
     * Парсит метрику акселерометра.
     * Ожидаемый формат JSON: { "x": 0.0, "y": 9.8, "z": 0.0 }
     */
    private AccelerometerMetric parseAccelerometerMetric(RawSensorMessage message, JsonNode json) {
        if (!json.has("x") || !json.has("y") || !json.has("z")) {
            throw new IllegalArgumentException("Accelerometer metric JSON must contain 'x', 'y', and 'z' fields");
        }

        double x = json.get("x").asDouble();
        double y = json.get("y").asDouble();
        double z = json.get("z").asDouble();
        return new AccelerometerMetric(message.getSensorId(), message.getMeasuredAt(), x, y, z);
    }
}