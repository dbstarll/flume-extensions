package io.github.dbstarll.flume.instrumentation;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LoggerServer implements MonitorService, Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerServer.class);

    private static final int DEFAULT_POLL_FREQUENCY = 60;
    private static final long AWAIT_TERMINATION_TIMEOUT = 500;

    private int pollFrequency = DEFAULT_POLL_FREQUENCY;
    private ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    private final List<Map.Entry<String, String>> metrics = new LinkedList<>();

    @Override
    public void configure(final Context context) {
        pollFrequency = context.getInteger("pollFrequency", DEFAULT_POLL_FREQUENCY);
        LOGGER.debug("monitor poll frequency set to {}", pollFrequency);
        Preconditions.checkArgument(pollFrequency > 0, "poll frequency must be > 0");

        metrics.clear();
        for (final String item : StringUtils.split(context.getString("metrics", ""), ',')) {
            final int idx = item.indexOf(':');
            if (idx > 0) {
                metrics.add(new MetricsEntry(item.substring(0, idx), item.substring(idx + 1)));
            }
        }
        LOGGER.debug("monitor metrics set to {}", metrics);
    }

    @Override
    public void run() {
        final Map<String, Map<String, String>> metricsMap = JMXPollUtil.getAllMBeans();
        if (metrics.isEmpty()) {
            LOGGER.info("metrics: {}", metricsMap);
        } else {
            for (Map.Entry<String, String> entry : metrics) {
                final Map<String, String> attrs = metricsMap.get(entry.getKey());
                if (attrs != null) {
                    final String attr = attrs.get(entry.getValue());
                    if (attr != null) {
                        LOGGER.info("metrics: {}.{} = {}", entry.getKey(), entry.getValue(), attr);
                    }
                }
            }
        }
    }

    @Override
    public void start() {
        LOGGER.info("Starting {}...", this);
        if (service.isShutdown() || service.isTerminated()) {
            service = Executors.newSingleThreadScheduledExecutor();
        }
        service.scheduleWithFixedDelay(this, 0, pollFrequency, TimeUnit.SECONDS);
        LOGGER.info("monitor started.");
    }

    @Override
    public void stop() {
        LOGGER.info("monitor stopping...");
        service.shutdown();
        while (!service.isTerminated()) {
            try {
                LOGGER.warn("Waiting for logger monitor to stop");
                service.awaitTermination(AWAIT_TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                LOGGER.warn("Interrupted while waiting for logger monitor to shutdown", ex);
                service.shutdownNow();
            }
        }
        LOGGER.info("monitor stopped.");
    }

    public static final class MetricsEntry implements Map.Entry<String, String> {
        private final String key;
        private final String value;

        private MetricsEntry(final String key, final String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String setValue(final String newValue) {
            throw new UnsupportedOperationException();
        }
    }
}
