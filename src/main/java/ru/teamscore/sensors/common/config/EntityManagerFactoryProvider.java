package ru.teamscore.sensors.common.config;

import jakarta.persistence.EntityManagerFactory;
import org.hibernate.cfg.Configuration;
import ru.teamscore.sensors.common.entity.ProcessingState;
import ru.teamscore.sensors.common.entity.RawSensorMessage;
import ru.teamscore.sensors.common.entity.SensorDevice;
import ru.teamscore.sensors.common.entity.metric.AccelerometerMetric;
import ru.teamscore.sensors.common.entity.metric.BarometerMetric;
import ru.teamscore.sensors.common.entity.metric.LightMetric;
import ru.teamscore.sensors.common.entity.metric.LocationMetric;

public class EntityManagerFactoryProvider {
    private static final EntityManagerFactory ENTITY_MANAGER_FACTORY = new Configuration()
            .addAnnotatedClass(RawSensorMessage.class)
            .addAnnotatedClass(SensorDevice.class)
            .addAnnotatedClass(ProcessingState.class)
            .addAnnotatedClass(LightMetric.class)
            .addAnnotatedClass(BarometerMetric.class)
            .addAnnotatedClass(LocationMetric.class)
            .addAnnotatedClass(AccelerometerMetric.class)
            .buildSessionFactory();

    private EntityManagerFactoryProvider() {}

    public static EntityManagerFactory getEntityManagerFactory() {
        return ENTITY_MANAGER_FACTORY;
    }
}