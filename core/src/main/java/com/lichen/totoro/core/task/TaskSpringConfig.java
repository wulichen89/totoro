package com.lichen.totoro.core.task;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TaskSpringConfig {
    private static final int DEFAULT_POOL_TASK_THREADS = Runtime.getRuntime().availableProcessors() * 10 - 1;
    private static final int DEFAULT_TASK_QUEUE_SIZE = 1000 * 1000;
    private static final long DEFAULT_TASK_WARN_TIME = 30 * 1000;
    private static final long DEFAULT_SCAN_TIME = 30 * 1000;

    @Bean
    public TaskManager getTaskManager(@Value("${pool_task_threads}") Integer numOfThreadsInput, @Value("${pool_max_waiting_tasks}") Integer numOfMaxWaitingTaskInput, @Value("${pool_warn_time}") Long warnTimeInput, @Value("${pool_scan_time}") Long scanTimeInput) {
        if (numOfThreadsInput == null) {
            numOfThreadsInput = DEFAULT_POOL_TASK_THREADS;
        }

        if (numOfMaxWaitingTaskInput == null) {
            numOfMaxWaitingTaskInput = DEFAULT_TASK_QUEUE_SIZE;
        }

        if (warnTimeInput == null) {
            warnTimeInput = DEFAULT_TASK_WARN_TIME;
        }

        if (scanTimeInput == null) {
            scanTimeInput = DEFAULT_SCAN_TIME;
        }

        return new TaskManager(numOfThreadsInput, numOfMaxWaitingTaskInput, warnTimeInput, scanTimeInput);
    }
}