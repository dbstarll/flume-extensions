package io.github.dbstarll.flume.sink;

import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockedNullSink extends AbstractSink implements Configurable, BatchSizeSupported {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockedNullSink.class);

    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_LOG_EVERY_N_EVENTS = 10000;
    private static final int DEFAULT_SLEEP_MILLIS = 1000;

    private SinkCounter sinkCounter;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private int logEveryNEvents = DEFAULT_LOG_EVERY_N_EVENTS;
    private int sleepMillis = DEFAULT_SLEEP_MILLIS;

    @Override
    public void configure(final Context context) {
        batchSize = context.getInteger("batchSize", DEFAULT_BATCH_SIZE);
        LOGGER.debug("sink {} batch size set to {}", getName(), batchSize);
        Preconditions.checkArgument(batchSize > 0, "Batch size must be > 0");

        logEveryNEvents = context.getInteger("logEveryNEvents", DEFAULT_LOG_EVERY_N_EVENTS);
        LOGGER.debug("sink {} log event N events set to {}", getName(), logEveryNEvents);
        Preconditions.checkArgument(logEveryNEvents > 0, "logEveryNEvents must be > 0");

        sleepMillis = context.getInteger("sleepMillis", DEFAULT_SLEEP_MILLIS);
        LOGGER.debug("sink {} sleep millis set to {}", getName(), sleepMillis);
        Preconditions.checkArgument(sleepMillis > 0, "sleepMillis must be > 0");

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        final Channel channel = getChannel();
        final Transaction transaction = channel.getTransaction();
        try {
            transaction.begin();
            final int size = takeBatch(channel);
            if (size == 0) {
                sinkCounter.incrementBatchEmptyCount();
            } else {
                if (size < batchSize) {
                    sinkCounter.incrementBatchUnderflowCount();
                } else {
                    sinkCounter.incrementBatchCompleteCount();
                }
                sinkCounter.addToEventDrainAttemptCount(size);
                Thread.sleep(sleepMillis);
            }
            transaction.commit();
            sinkCounter.addToEventDrainSuccessCount(size);
            return size == 0 ? Status.BACKOFF : Status.READY;
        } catch (Throwable t) {
            transaction.rollback();
            if (t instanceof Error) {
                throw (Error) t;
            } else if (t instanceof ChannelException) {
                LOGGER.error("sink " + getName() + ": Unable to get event from channel "
                        + channel.getName() + ". Exception follows.", t);
                sinkCounter.incrementChannelReadFail();
                return Status.BACKOFF;
            } else {
                sinkCounter.incrementEventWriteFail();
                throw new EventDeliveryException("Failed to send events", t);
            }
        } finally {
            transaction.close();
        }
    }

    private int takeBatch(final Channel channel) throws ChannelException {
        long eventCounter = sinkCounter.getEventDrainSuccessCount();
        for (int i = 0; i < batchSize; i++) {
            if (channel.take() == null) {
                return i;
            } else if (++eventCounter % logEveryNEvents == 0) {
                LOGGER.info("sink {} successful processed {} events.", getName(), eventCounter);
            }
        }
        return batchSize;
    }

    @Override
    public synchronized void start() {
        LOGGER.info("Starting {}...", this);
        sinkCounter.start();
        super.start();
        LOGGER.info("sink {} started.", getName());
    }

    @Override
    public synchronized void stop() {
        LOGGER.info("sink {} stopping...", getName());
        super.stop();
        sinkCounter.stop();
        LOGGER.info("sink {} stopped. Event metrics: {}", getName(), sinkCounter);
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }
}
