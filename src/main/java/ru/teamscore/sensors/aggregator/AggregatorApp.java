package ru.teamscore.sensors.aggregator;

import jakarta.persistence.EntityManagerFactory;
import ru.teamscore.sensors.common.SensorType;
import ru.teamscore.sensors.common.config.EntityManagerFactoryProvider;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Приложение Aggregator для агрегации данных датчиков.
 * Запускается с параметрами командной строки.
 * <p>
 * Использование:
 * java AggregatorApp <тип_датчика> <дата_начала> <дата_окончания> <интервал> [название_устройства]
 * <p>
 * Параметры:
 *   тип_датчика: LIGHT, BAROMETER, LOCATION, ACCELEROMETER
 *   дата_начала: формат yyyy-MM-dd HH:mm:ss
 *   дата_окончания: формат yyyy-MM-dd HH:mm:ss
 *   интервал: MINUTE, HOUR, DAY, WEEK
 *   название_устройства: опционально
 */
public class AggregatorApp {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        if (args.length < 4) {
            printUsage();
            System.exit(1);
        }

        try {
            SensorType sensorType = parseSensorType(args[0]);
            LocalDateTime startTime = parseDateTime(args[1], "дата начала");
            LocalDateTime endTime = parseDateTime(args[2], "дата окончания");
            TimeInterval interval = parseInterval(args[3]);
            String deviceName = args.length > 4 ? args[4] : null;

            if (endTime.isBefore(startTime)) {
                System.err.println("Ошибка: дата окончания должна быть позже даты начала");
                System.exit(1);
            }

            System.out.println("=".repeat(60));
            System.out.println("Агрегация данных датчиков");
            System.out.println("=".repeat(60));
            System.out.println("Тип датчика: " + sensorType);
            System.out.println("Период: " + startTime.format(DATE_TIME_FORMATTER) + " - " + endTime.format(DATE_TIME_FORMATTER));
            System.out.println("Интервал: " + interval);
            if (deviceName != null) {
                System.out.println("Устройство: " + deviceName);
            }
            System.out.println("=".repeat(60));
            System.out.println();

            try (EntityManagerFactory emf = EntityManagerFactoryProvider.getEntityManagerFactory()) {
                AggregatorService aggregatorService = new AggregatorService(emf);
                aggregatorService.aggregate(sensorType, startTime, endTime, interval, deviceName);
            }

        } catch (IllegalArgumentException e) {
            System.err.println("Ошибка: " + e.getMessage());
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println();
        System.out.println("Использование:");
        System.out.println("  java AggregatorApp <тип_датчика> <дата_начала> <дата_окончания> <интервал> [название_устройства]");
        System.out.println();
        System.out.println("Параметры:");
        System.out.println("  тип_датчика: LIGHT, BAROMETER, LOCATION, ACCELEROMETER");
        System.out.println("  дата_начала: формат \"yyyy-MM-dd HH:mm:ss\"");
        System.out.println("  дата_окончания: формат \"yyyy-MM-dd HH:mm:ss\"");
        System.out.println("  интервал: MINUTE, HOUR, DAY, WEEK");
        System.out.println("  название_устройства: опционально, фильтр по устройству");
        System.out.println();
        System.out.println("Пример:");
        System.out.println("  java AggregatorApp LIGHT \"2025-12-01 00:00:00\" \"2025-12-31 23:59:59\" HOUR");
        System.out.println("  java AggregatorApp LIGHT \"2025-12-01 00:00:00\" \"2025-12-31 23:59:59\" HOUR \"MyHome ZZZ\"");
    }

    private static SensorType parseSensorType(String value) {
        try {
            return SensorType.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Неверный тип датчика: " + value +
                    ". Допустимые значения: LIGHT, BAROMETER, LOCATION, ACCELEROMETER");
        }
    }

    private static LocalDateTime parseDateTime(String value, String paramName) {
        try {
            return LocalDateTime.parse(value, DATE_TIME_FORMATTER);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Неверный формат даты для параметра '" + paramName +
                    "': " + value + ". Ожидаемый формат: yyyy-MM-dd HH:mm:ss");
        }
    }

    private static TimeInterval parseInterval(String value) {
        try {
            return TimeInterval.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Неверный интервал: " + value +
                    ". Допустимые значения: MINUTE, HOUR, DAY, WEEK");
        }
    }
}