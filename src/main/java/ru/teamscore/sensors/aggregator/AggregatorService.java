package ru.teamscore.sensors.aggregator;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Query;
import org.hibernate.Session;
import ru.teamscore.sensors.common.SensorType;

import java.io.InputStream;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;

/**
 * Сервис агрегации данных датчиков.
 */
public class AggregatorService {
    private static final int PAGE_SIZE = 16;
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final EntityManagerFactory emf;
    private final PrintStream out;
    private final InputStream in;

    public AggregatorService(EntityManagerFactory emf) {
        this(emf, System.out, System.in);
    }

    public AggregatorService(EntityManagerFactory emf, PrintStream out, InputStream in) {
        this.emf = emf;
        this.out = out;
        this.in = in;
    }

    /**
     * Выполняет агрегацию и выводит результаты.
     */
    public void aggregate(SensorType sensorType, LocalDateTime startTime, LocalDateTime endTime,
                          TimeInterval interval, String deviceName) {
        List<AggregatedResult> results = fetchAggregatedData(sensorType, startTime, endTime, interval, deviceName);
        printResults(sensorType, results);
    }

    /**
     * Получает агрегированные данные из БД.
     */
    public List<AggregatedResult> fetchAggregatedData(SensorType sensorType, LocalDateTime startTime,
                                                       LocalDateTime endTime, TimeInterval interval, String deviceName) {
        try (EntityManager em = emf.createEntityManager()) {
            String tableName = getTableName(sensorType);
            boolean isH2 = isH2Database(em);
            String truncateFunction = getTruncateFunction(interval, isH2);

            StringBuilder sql = new StringBuilder();
            sql.append("SELECT d.device_name, ").append(truncateFunction).append(" as interval_start, ");
            sql.append(getAvgColumns(sensorType));
            sql.append(" FROM ").append(tableName).append(" m ");
            sql.append("JOIN sensor_devices d ON m.sensor_id = d.sensor_id ");
            sql.append("WHERE m.measured_at >= :startTime AND m.measured_at <= :endTime ");

            if (deviceName != null && !deviceName.isEmpty()) {
                sql.append("AND d.device_name = :deviceName ");
            }

            sql.append("GROUP BY d.device_name, ").append(truncateFunction).append(" ");
            sql.append("ORDER BY d.device_name ASC, interval_start DESC");

            Query query = em.createNativeQuery(sql.toString());
            query.setParameter("startTime", startTime);
            query.setParameter("endTime", endTime);

            if (deviceName != null && !deviceName.isEmpty()) {
                query.setParameter("deviceName", deviceName);
            }

            List<Object[]> rows = query.getResultList();

            List<AggregatedResult> results = new ArrayList<>();
            for (Object[] row : rows) {
                String device = (String) row[0];
                LocalDateTime intervalStart = convertToLocalDateTime(row[1]);
                Double[] values = extractValues(sensorType, row);
                results.add(new AggregatedResult(device, intervalStart, values));
            }

            return results;
        }
    }

    /**
     * Выводит результаты с пагинацией.
     */
    private void printResults(SensorType sensorType, List<AggregatedResult> results) {
        if (results.isEmpty()) {
            out.println("Нет данных за указанный период.");
            return;
        }

        String[] headers = getHeaders(sensorType);
        int[] columnWidths = calculateColumnWidths(sensorType, results, headers);

        Scanner scanner = new Scanner(in);
        int totalRows = results.size();
        int displayedRows = 0;

        while (displayedRows < totalRows) {
            if (displayedRows % PAGE_SIZE == 0) {
                printHeader(headers, columnWidths);
            }

            int pageEnd = Math.min(displayedRows + PAGE_SIZE, totalRows);
            for (int i = displayedRows; i < pageEnd; i++) {
                printRow(sensorType, results.get(i), columnWidths);
            }
            displayedRows = pageEnd;

            printSeparator(columnWidths);
            if (displayedRows >= totalRows) {
                out.println("Выведено " + displayedRows + " строк из " + totalRows + ". Конец таблицы============");
            } else {
                out.println("Выведено " + displayedRows + " строк из " + totalRows + ". Нажмите Enter для продолжения...");
                scanner.nextLine();
            }
        }
    }

    private void printHeader(String[] headers, int[] columnWidths) {
        printSeparator(columnWidths);
        StringBuilder headerLine = new StringBuilder("|");
        for (int i = 0; i < headers.length; i++) {
            headerLine.append(" ").append(padRight(headers[i], columnWidths[i])).append(" |");
        }
        out.println(headerLine);
        printSeparator(columnWidths);
    }

    private void printSeparator(int[] columnWidths) {
        StringBuilder separator = new StringBuilder("+");
        for (int width : columnWidths) {
            separator.append("-".repeat(width + 2)).append("+");
        }
        out.println(separator);
    }

    private void printRow(SensorType sensorType, AggregatedResult result, int[] columnWidths) {
        StringBuilder line = new StringBuilder("|");
        line.append(" ").append(padRight(result.getDeviceName(), columnWidths[0])).append(" |");
        line.append(" ").append(padRight(result.getIntervalStart().format(DATE_FORMAT), columnWidths[1])).append(" |");

        Double[] values = result.getValues();
        for (int i = 0; i < values.length; i++) {
            String valueStr = values[i] != null ? String.format(Locale.US, "%.2f", values[i]) : "N/A";
            line.append(" ").append(padLeft(valueStr, columnWidths[2 + i])).append(" |");
        }
        out.println(line);
    }

    private String[] getHeaders(SensorType sensorType) {
        return switch (sensorType) {
            case LIGHT -> new String[]{"DEVICE", "DATE", "LIGHT"};
            case BAROMETER -> new String[]{"DEVICE", "DATE", "AIR_PRESSURE"};
            case LOCATION -> new String[]{"DEVICE", "DATE", "LATITUDE", "LONGITUDE"};
            case ACCELEROMETER -> new String[]{"DEVICE", "DATE", "X", "Y", "Z"};
        };
    }

    private int[] calculateColumnWidths(SensorType sensorType, List<AggregatedResult> results, String[] headers) {
        int[] widths = new int[headers.length];

        for (int i = 0; i < headers.length; i++) {
            widths[i] = headers[i].length();
        }

        for (AggregatedResult result : results) {
            widths[0] = Math.max(widths[0], result.getDeviceName().length());
            widths[1] = Math.max(widths[1], 19);

            Double[] values = result.getValues();
            for (int i = 0; i < values.length; i++) {
                String valueStr = values[i] != null ? String.format(Locale.US, "%.2f", values[i]) : "N/A";
                widths[2 + i] = Math.max(widths[2 + i], valueStr.length());
            }
        }

        return widths;
    }

    private String getTableName(SensorType sensorType) {
        return switch (sensorType) {
            case LIGHT -> "metric_light";
            case BAROMETER -> "metric_barometer";
            case LOCATION -> "metric_location";
            case ACCELEROMETER -> "metric_accelerometer";
        };
    }

    private String getAvgColumns(SensorType sensorType) {
        return switch (sensorType) {
            case LIGHT -> "AVG(m.light_value)";
            case BAROMETER -> "AVG(m.air_pressure)";
            case LOCATION -> "AVG(m.latitude), AVG(m.longitude)";
            case ACCELEROMETER -> "AVG(m.val_x), AVG(m.val_y), AVG(m.val_z)";
        };
    }

    private String getTruncateFunction(TimeInterval interval, boolean isH2) {
        if (isH2) {
            return switch (interval) {
                case MINUTE -> "PARSEDATETIME(FORMATDATETIME(m.measured_at, 'yyyy-MM-dd HH:mm:00'), 'yyyy-MM-dd HH:mm:ss')";
                case HOUR -> "PARSEDATETIME(FORMATDATETIME(m.measured_at, 'yyyy-MM-dd HH:00:00'), 'yyyy-MM-dd HH:mm:ss')";
                case DAY -> "PARSEDATETIME(FORMATDATETIME(m.measured_at, 'yyyy-MM-dd 00:00:00'), 'yyyy-MM-dd HH:mm:ss')";
                case WEEK -> "DATEADD('DAY', -(DAY_OF_WEEK(m.measured_at) - 1), PARSEDATETIME(FORMATDATETIME(m.measured_at, 'yyyy-MM-dd 00:00:00'), 'yyyy-MM-dd HH:mm:ss'))";
            };
        } else {
            return switch (interval) {
                case MINUTE -> "DATE_TRUNC('minute', m.measured_at)";
                case HOUR -> "DATE_TRUNC('hour', m.measured_at)";
                case DAY -> "DATE_TRUNC('day', m.measured_at)";
                case WEEK -> "DATE_TRUNC('week', m.measured_at)";
            };
        }
    }

    private boolean isH2Database(EntityManager em) {
        try {
            String productName = em.unwrap(Session.class)
                    .doReturningWork(connection -> connection.getMetaData().getDatabaseProductName());
            return "H2".equalsIgnoreCase(productName);
        } catch (Exception e) {
            return false;
        }
    }

    private LocalDateTime convertToLocalDateTime(Object value) {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        } else if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toLocalDateTime();
        } else if (value instanceof java.time.OffsetDateTime) {
            return ((java.time.OffsetDateTime) value).toLocalDateTime();
        }
        throw new IllegalArgumentException("Cannot convert to LocalDateTime: " + value.getClass());
    }

    private Double[] extractValues(SensorType sensorType, Object[] row) {
        return switch (sensorType) {
            case LIGHT, BAROMETER -> new Double[]{toDouble(row[2])};
            case LOCATION -> new Double[]{toDouble(row[2]), toDouble(row[3])};
            case ACCELEROMETER -> new Double[]{toDouble(row[2]), toDouble(row[3]), toDouble(row[4])};
        };
    }

    private Double toDouble(Object value) {
        if (value == null) return null;
        if (value instanceof Double) return (Double) value;
        if (value instanceof Number) return ((Number) value).doubleValue();
        return Double.parseDouble(value.toString());
    }

    private String padRight(String s, int length) {
        if (s.length() >= length) return s;
        return s + " ".repeat(length - s.length());
    }

    private String padLeft(String s, int length) {
        if (s.length() >= length) return s;
        return " ".repeat(length - s.length()) + s;
    }
}