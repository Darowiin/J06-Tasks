package ru.teamscore.sensors.common.entity.metric;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "metric_accelerometer")
public class AccelerometerMetric extends SensorMetric {

    @Column(name = "val_x")
    private Double x;

    @Column(name = "val_y")
    private Double y;

    @Column(name = "val_z")
    private Double z;

    public AccelerometerMetric(UUID sensorId, LocalDateTime measuredAt, Double x, Double y, Double z) {
        super(sensorId, measuredAt);
        this.x = x;
        this.y = y;
        this.z = z;
    }
}