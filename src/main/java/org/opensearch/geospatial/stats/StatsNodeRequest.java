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

import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class StatsNodeRequest extends BaseNodeRequest {

    private StatsRequest request;

    public StatsNodeRequest() {
        super();
    }

    public StatsNodeRequest(StreamInput in) throws IOException {
        super(in);
        request = new StatsRequest(in);
    }

    public StatsNodeRequest(StatsRequest request) {
        this.request = request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }

}
