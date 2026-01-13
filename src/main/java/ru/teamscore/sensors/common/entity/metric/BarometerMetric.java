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
@Table(name = "metric_barometer")
public class BarometerMetric extends SensorMetric {

    @Column(name = "air_pressure")
    private Double airPressure;

    public BarometerMetric(UUID sensorId, LocalDateTime measuredAt, Double airPressure) {
        super(sensorId, measuredAt);
        this.airPressure = airPressure;
    }
}