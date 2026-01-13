package ru.teamscore.sensors.common.entity.metric;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@NoArgsConstructor
@MappedSuperclass
public abstract class SensorMetric {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Setter
    @Column(name = "sensor_id", nullable = false)
    private UUID sensorId;

    @Setter
    @Column(name = "measured_at", nullable = false)
    private LocalDateTime measuredAt;

    public SensorMetric(UUID sensorId, LocalDateTime measuredAt) {
        this.sensorId = sensorId;
        this.measuredAt = measuredAt;
    }
}