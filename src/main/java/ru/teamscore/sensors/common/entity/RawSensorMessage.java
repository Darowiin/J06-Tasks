package ru.teamscore.sensors.common.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.teamscore.sensors.common.SensorType;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@NoArgsConstructor
@Entity
@Table(name = "raw_sensor_messages")
public class RawSensorMessage {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Setter
    @Column(name = "sensor_id", nullable = false)
    private UUID sensorId;

    @Setter
    @Enumerated(EnumType.STRING)
    @Column(name = "sensor_type", nullable = false, length = 32)
    private SensorType sensorType;

    @Setter
    @Column(name = "device_name", length = 32, nullable = false)
    private String deviceName;

    @Setter
    @Column(name = "measured_at", nullable = false)
    private LocalDateTime measuredAt;

    @Setter
    @Column(name = "saved_at", nullable = false)
    private LocalDateTime savedAt = LocalDateTime.now();

    @Setter
    @Column(name = "json_value", columnDefinition = "TEXT")
    private String jsonValue;

    public RawSensorMessage(UUID sensorId, SensorType sensorType, String deviceName, LocalDateTime measuredAt, String jsonValue) {
        this.sensorId = sensorId;
        this.sensorType = sensorType;
        this.deviceName = deviceName;
        this.measuredAt = measuredAt;
        this.jsonValue = jsonValue;
        this.savedAt = LocalDateTime.now();
    }

    @Override
    public String toString() {
        return "RawSensorMessage{id=" + id + ", sensor_type=" + sensorType + ", sensor_id='" + sensorId + "'}";
    }
}