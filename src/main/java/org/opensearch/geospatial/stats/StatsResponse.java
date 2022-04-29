/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.geospatial.stats;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        builder.startObject("nodes");
        for (Map.Entry<String, StatsNodeResponse> e : getNodesMap().entrySet()) {
            builder.startObject(e.getKey());
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
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
