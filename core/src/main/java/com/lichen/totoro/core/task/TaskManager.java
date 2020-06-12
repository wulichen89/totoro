package com.lichen.totoro.core.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class TaskManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskManager.class);

    private class ToToRoTask implements Runnable {
        private final String taskName;
        private final Runnable runnable;
        private final long warnTime;
        private long startTime = -1;

        public ToToRoTask(String taskName, Runnable runnable, long warnTime) {
            this.taskName = taskName;
            this.runnable = runnable;
            this.warnTime = warnTime;
        }

        public String getTaskName () {
            return this.taskName;
        }

        public long getProcessTime () {
            return startTime == -1 ? -1 : System.currentTimeMillis() - startTime;
        }

        public void run() {
            startTime = System.currentTimeMillis();

            threadMap.put(Thread.currentThread(), this);
            runnable.run();

            long processTime = System.currentTimeMillis() - startTime;
            if (processTime >= this.warnTime) {
                LOGGER.warn(String.format("%s takes %s ms to run", this.taskName, processTime));
            }
        }
    }

    class ThreadStatus {
        private String threadName;
        private String taskName;
        private long processTime;
        private boolean isRunning;

        public ThreadStatus (Thread thread, ToToRoTask toToRoTask) {
            this.threadName = thread.getName();
            this.taskName = toToRoTask.getTaskName();
            this.processTime = toToRoTask.getProcessTime();
            this.isRunning = this.processTime >= 0;
        }

        public String getThreadName() {
            return threadName;
        }

        public long getProcessTime() {
            return processTime;
        }

        public String getTaskName() {
            return this.taskName;
        }

        public boolean isRunning() {
            return isRunning;
        }
    }

    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
    private final int maxWaitingTasksSize;
    private final long defaultWarnTime;
    private final ConcurrentHashMap<Thread, ToToRoTask> threadMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, ScheduledFuture> canBeCancelledDelayTaskMap = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    public TaskManager(int poolSize, int maxWaitingTasksSize, long defaultWarnTime, long scheduledTimeScanMap) {
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(poolSize);
        this.maxWaitingTasksSize = maxWaitingTasksSize;
        this.defaultWarnTime = defaultWarnTime;

        this.submitScheduledTaskWithFixedDelay(() -> {
            for (Map.Entry<String, ScheduledFuture> scheduledFutureEntry : canBeCancelledDelayTaskMap.entrySet()) {
                if (scheduledFutureEntry.getValue() == null || scheduledFutureEntry.getValue().isCancelled() || scheduledFutureEntry.getValue().isDone()) {
                    canBeCancelledDelayTaskMap.remove(scheduledFutureEntry.getKey());
                }
            }
        }, "Scanning cancelled tasks", this.defaultWarnTime, scheduledTimeScanMap);
    }

    public void submitTask(Runnable runnable, String taskName) {
        submitTask(runnable, taskName, this.defaultWarnTime);
    }

    public void submitTask(Runnable runnable, String taskName, long warnTime) {
        lock.lock();
        try {
            if (scheduledThreadPoolExecutor.getQueue().size() >= maxWaitingTasksSize) {
                throw new RejectedExecutionException("TaskPool has reached the maximum size");
            }

            scheduledThreadPoolExecutor.schedule(new ToToRoTask(taskName, runnable, warnTime), 0, TimeUnit.SECONDS);

        } finally {
            lock.unlock();
        }
    }

    public void submitDelayTask(Runnable runnable, String taskName, long delayTime) {
        this.submitDelayTask(runnable, taskName, this.defaultWarnTime, delayTime, false);
    }

    public void submitCanBeCancelledDelayTask(Runnable runnable, String taskName, long delayTime) {
        this.submitDelayTask(runnable, taskName, this.defaultWarnTime, delayTime, true);
    }

    public void submitDelayTask(Runnable runnable, String taskName, long warnTime, long delayTime, boolean canBeCancelled) {
        lock.lock();
        try {
            if (scheduledThreadPoolExecutor.getQueue().size() >= maxWaitingTasksSize) {
                throw new RejectedExecutionException("TaskPool has reached the maximum size");
            }

            if (canBeCancelled) {
                if (canBeCancelledDelayTaskMap.containsKey(taskName)) {
                    throw new RejectedExecutionException(taskName + "is duplicated in canceled task pool, please cancel it first");
                }

                ScheduledFuture scheduledFuture = scheduledThreadPoolExecutor.schedule(new ToToRoTask(taskName, runnable, warnTime), delayTime, TimeUnit.MILLISECONDS);
                canBeCancelledDelayTaskMap.put(taskName, scheduledFuture);
            } else {
                scheduledThreadPoolExecutor.schedule(new ToToRoTask(taskName, runnable, warnTime), delayTime, TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    public void submitScheduledTaskWithFixedDelay(Runnable runnable, String taskName, long delayTime) {
        this.submitScheduledTaskWithFixedDelay(runnable, taskName, this.defaultWarnTime, delayTime);
    }

    public void submitScheduledTaskWithFixedDelay(Runnable runnable, String taskName, long warnTime, long delayTime) {
        lock.lock();
        try {
            if (scheduledThreadPoolExecutor.getQueue().size() >= maxWaitingTasksSize) {
                throw new RejectedExecutionException("TaskPool has reached the maximum size");
            }

            scheduledThreadPoolExecutor.scheduleWithFixedDelay(new ToToRoTask(taskName, runnable, warnTime), 0, delayTime, TimeUnit.MILLISECONDS);

        } finally {
            lock.unlock();
        }
    }

    public void cancelDelayTaskByTaskName(String taskName) {
        lock.lock();
        try {
            ScheduledFuture scheduledFuture = canBeCancelledDelayTaskMap.remove(taskName);

            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
            }

            this.scheduledThreadPoolExecutor.getQueue().remove(scheduledFuture);
        } finally {
            lock.unlock();
        }
    }

    public List<ThreadStatus> getThreadStatus () {
        List<ThreadStatus> threadStatuses = new LinkedList<>();

        for (Map.Entry<Thread, ToToRoTask> entry : threadMap.entrySet()) {
            threadStatuses.add(new ThreadStatus(entry.getKey(), entry.getValue()));
        }

        return threadStatuses;
    }

    public int getQueueSize () {
        return this.scheduledThreadPoolExecutor.getQueue().size();
    }
}