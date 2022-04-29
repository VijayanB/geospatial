/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class StatsResponse extends BaseNodesResponse<StatsNodeResponse> implements Writeable, ToXContentObject {

    protected StatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public StatsResponse(ClusterName clusterName, List<StatsNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<StatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(StatsNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<StatsNodeResponse> nodeResponses) throws IOException {
        out.writeList(nodeResponses);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        final Long reduce = getNodesMap().entrySet()
            .stream()
            .map(entry -> entry.getValue().getStats().getTotalAPICount())
            .reduce(0L, Long::sum);
        final List<UploadMetric> uploadMetrics = getNodesMap().entrySet()
            .stream()
            .map(entry -> entry.getValue().getStats().getMetrics())
            .flatMap(List::stream).collect(Collectors.toList());
        builder.startObject("total");

        builder.field("count", reduce);
        builder.field("upload", summingMetricField(uploadMetrics, UploadMetric::getUploadCount));
        builder.field("success", summingMetricField(uploadMetrics, UploadMetric::getSuccessCount));
        builder.field("failed", summingMetricField(uploadMetrics, UploadMetric::getFailedCount));
        builder.field("duration", summingMetricField(uploadMetrics, UploadMetric::getDuration));

        builder.endObject();
        builder.startObject("nodes");
        for (Map.Entry<String, StatsNodeResponse> e : getNodesMap().entrySet()) {
            builder.startObject(e.getKey());
            e.getValue().toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    private Long summingMetricField(List<UploadMetric> metrics, Function<UploadMetric, Long> mapper) {
        return metrics.stream().collect(Collectors.summingLong(mapper::apply));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsResponse otherResponse = (StatsResponse) o;
        return Objects.equals(getNodes(), otherResponse.getNodes()) && Objects.equals(failures(), otherResponse.failures());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodes(), failures());
    }

}
