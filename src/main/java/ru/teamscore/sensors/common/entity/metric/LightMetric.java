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
@Table(name = "metric_light")
public class LightMetric extends SensorMetric {

    @Column(name = "light_value")
    private Integer lightValue;

    public LightMetric(UUID sensorId, LocalDateTime measuredAt, Integer lightValue) {
        super(sensorId, measuredAt);
        this.lightValue = lightValue;
    }
}