package ru.teamscore.sensors.common.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.teamscore.sensors.common.SensorType;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "sensor_devices")
public class SensorDevice {
    @Id
    @Column(name = "sensor_id")
    private UUID sensorId;

    @Column(name = "device_name")
    private String deviceName;

    @Enumerated(EnumType.STRING)
    @Column(name = "sensor_type", length = 32)
    private SensorType sensorType;

    @Column(name = "last_seen")
    private LocalDateTime lastSeen;

    public SensorDevice(UUID sensorId, String deviceName, SensorType sensorType, LocalDateTime lastSeen) {
        this.sensorId = sensorId;
        this.deviceName = deviceName;
        this.sensorType = sensorType;
        this.lastSeen = lastSeen;
    }
}