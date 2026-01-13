package ru.teamscore.sensors.common.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "processing_state")
public class ProcessingState {
    @Id
    @Column(name = "component_name")
    private String componentName;

    @Column(name = "last_processed_time")
    private LocalDateTime lastProcessedTime;

    public ProcessingState(String componentName, LocalDateTime lastProcessedTime) {
        this.componentName = componentName;
        this.lastProcessedTime = lastProcessedTime;
    }
}