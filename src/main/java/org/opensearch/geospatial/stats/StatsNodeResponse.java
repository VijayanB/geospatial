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

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

public class StatsNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    private final UploadStats stats;

    public StatsNodeResponse(DiscoveryNode node, UploadStats stats) {
        super(node);
        this.stats = stats;
    }

    public StatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.stats = in.readBoolean() ? new UploadStats(in) : UploadStats.getInstance();
    }

    public UploadStats getStats() {
        return stats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (stats == null) {
            out.writeBoolean(Boolean.FALSE);
            return;
        }
        out.writeBoolean(Boolean.TRUE);
        stats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        this.stats.toXContent(builder, params);
        return builder;
    }
}
