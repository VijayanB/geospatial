/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import static java.util.Collections.unmodifiableSet;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.metrics.CounterMetric;

/**
 * Contains the total upload stats
 */
public final class UploadStats implements Writeable {

    public enum FIELDS {

        METRICS,
        TOTAL,
        UPLOAD;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.getDefault());
        }
    }

    private static final UploadStats instance = new UploadStats();

    private final Set<UploadMetric> metrics;
    private final CounterMetric totalAPICount;

    /**
     * @return Singleton instance of UploadStats
     */
    public static UploadStats getInstance() {
        return instance;
    }

    UploadStats() {
        metrics = new HashSet<>();
        totalAPICount = new CounterMetric();
    }

    UploadStats(StreamInput input) throws IOException {
        totalAPICount = new CounterMetric();
        totalAPICount.inc(input.readVLong());
        metrics = unmodifiableSet(input.readSet(UploadMetric.UploadMetricBuilder::fromStreamInput));
    }

    /**
     * Add new metric to {@link UploadStats}
     * @param newMetric {@link UploadMetric} to be added to Stats
     */
    public void addMetric(UploadMetric newMetric) {
        Objects.requireNonNull(newMetric, "metric cannot be null");
        if (metrics.contains(newMetric)) {
            throw new IllegalArgumentException(newMetric.getMetricID() + " already exists");
        }
        if (newMetric.getUploadCount() < 1) {
            throw new IllegalArgumentException("metric should have at least 1 upload");
        }
        metrics.add(newMetric);
    }

    /**
     * Increment API Count
     */
    public void incrementAPICount() {
        totalAPICount.inc();
    }

    /**
     * Get total number of times Upload API is called
     * @return value of totalAPICount
     */
    public long getTotalAPICount() {
        return totalAPICount.count();
    }

    /**
     * Get list of added metrics so far to stats
     * @return List of {@link UploadMetric}
     */
    public List<UploadMetric> getMetrics() {
        return List.copyOf(metrics);
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeVLong(getTotalAPICount());
        output.writeCollection(getMetrics());
    }

}
