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

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.io.stream.StreamInput;

public class StatsRequest extends BaseNodesRequest<StatsRequest> {

    /**
     * Empty constructor needed for StatsTransportAction
     */
    public StatsRequest() {
        super((String[]) null);
    }

    public StatsRequest(StreamInput in) throws IOException {
        super(in);
    }
}
