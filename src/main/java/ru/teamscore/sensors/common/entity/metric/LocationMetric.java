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
@Table(name = "metric_location")
public class LocationMetric extends SensorMetric {

    @Column(name = "latitude")
    private Double latitude;

    @Column(name = "longitude")
    private Double longitude;

    public LocationMetric(UUID sensorId, LocalDateTime measuredAt, Double latitude, Double longitude) {
        super(sensorId, measuredAt);
        this.latitude = latitude;
        this.longitude = longitude;
    }
}