package com.lichen.totoro.core.task;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class TaskManagerTest {

    @Test
    public void submitTaskTest () throws InterruptedException {
        TaskManager taskManager = new TaskSpringConfig().getTaskManager(null, null, 1L, null);
        taskManager.submitTask(() -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "Test Task");

        Thread.sleep(1);
        List<TaskManager.ThreadStatus> statuses = taskManager.getThreadStatus();

        TaskManager.ThreadStatus status = statuses.stream().filter(s -> s.getTaskName().equals("Test Task")).findAny().get();
        assertTrue(status != null);
        assertTrue(status.isRunning());
        assertTrue(status.getThreadName() != null);
        assertTrue(status.getProcessTime() > 0);
        assertEquals("Test Task", status.getTaskName());

        Thread.sleep(10);
    }

    @Test
    public void submitTaskExceptionTest () throws InterruptedException {
        TaskManager taskManager = new TaskSpringConfig().getTaskManager(1, 10, null, 10000000000L);

        Thread.sleep(10);
        try {
            taskManager.submitTask(() -> {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }, "Test Task");

            IntStream.rangeClosed(1,10).forEach(i -> {
                taskManager.submitTask(() -> {}, "Over Task");
            });

            fail();
        } catch (Exception e) {
            assertTrue(e instanceof RejectedExecutionException);
            assertEquals(10, taskManager.getQueueSize());
        }
    }

    @Test
    public void submitDelayTaskTest () throws InterruptedException {
        TaskManager taskManager = new TaskSpringConfig().getTaskManager(null, null, 1L, 1L);

        taskManager.submitDelayTask(() -> {}, "delay task", 1);

        taskManager.submitCanBeCancelledDelayTask(() -> {}, "delay task", 1000);
        taskManager.cancelDelayTaskByTaskName("delay task");

        taskManager.submitCanBeCancelledDelayTask(() -> {}, "delay task2", 1);

        Thread.sleep(10);
    }

    @Test
    public void submitDelayTaskExceptionTest () {
        TaskManager taskManager = new TaskSpringConfig().getTaskManager(1, 10, null, 10L);

        taskManager.submitCanBeCancelledDelayTask(() -> {}, "Delay Task", 100L);

        try {
            taskManager.submitCanBeCancelledDelayTask(() -> {}, "Delay Task", 100L);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof RejectedExecutionException);
        }

        try {
            IntStream.rangeClosed(1,10).forEach(i -> {
                taskManager.submitDelayTask(() -> {}, "Over Task", 10);
            });

            fail();
        } catch (Exception e) {
            assertTrue(e instanceof RejectedExecutionException);
        }
    }

    @Test
    public void submitScheduledTask() throws InterruptedException {
        TaskManager taskManager = new TaskSpringConfig().getTaskManager(1, 10, 1L, null);

        AtomicInteger atomicInteger = new AtomicInteger(0);

        taskManager.submitScheduledTaskWithFixedDelay(() -> {atomicInteger.incrementAndGet();}, "Scheduled Task", 1);

        Thread.sleep(10);
        assertTrue(atomicInteger.get() > 2);

        try {
            IntStream.rangeClosed(1,10).forEach(i -> {
                taskManager.submitScheduledTaskWithFixedDelay(() -> {}, "Over Task", 10);
            });

            fail();
        } catch (Exception e) {
            assertTrue(e instanceof RejectedExecutionException);
        }
    }
}