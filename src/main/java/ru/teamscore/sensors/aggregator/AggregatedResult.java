package ru.teamscore.sensors.aggregator;

import lombok.Getter;

import java.time.LocalDateTime;

/**
 * Результат агрегации данных датчика.
 */
@Getter
public class AggregatedResult {
    private final String deviceName;
    private final LocalDateTime intervalStart;
    private final Double[] values;

    public AggregatedResult(String deviceName, LocalDateTime intervalStart, Double... values) {
        this.deviceName = deviceName;
        this.intervalStart = intervalStart;
        this.values = values;
    }

    /**
     * Получить первое значение (для датчиков с одним показателем).
     */
    public Double getValue() {
        return values.length > 0 ? values[0] : null;
    }
}