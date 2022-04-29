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

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class StatsTransportAction extends TransportNodesAction<StatsRequest, StatsResponse, StatsNodeRequest, StatsNodeResponse> {

    private final TransportService transportService;

    @Inject
    public StatsTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            StatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            StatsRequest::new,
            StatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            StatsNodeResponse.class
        );
        this.transportService = transportService;
    }

    @Override
    protected StatsResponse newResponse(
        StatsRequest nodesRequest,
        List<StatsNodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new StatsResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected StatsNodeRequest newNodeRequest(StatsRequest nodesRequest) {
        return null;
    }

    @Override
    protected StatsNodeResponse newNodeResponse(StreamInput streamInput) throws IOException {
        return null;
    }

    @Override
    protected StatsNodeResponse nodeOperation(StatsNodeRequest nodeRequest) {
        return null;
    }
}
