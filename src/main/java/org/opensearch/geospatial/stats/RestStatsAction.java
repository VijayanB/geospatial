/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats;

import static org.opensearch.geospatial.plugin.GeospatialPlugin.URL_DELIMITER;
import static org.opensearch.rest.RestRequest.Method.POST;

import java.util.List;

import org.opensearch.client.node.NodeClient;
import org.opensearch.geospatial.plugin.GeospatialPlugin;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

public final class RestStatsAction extends BaseRestHandler {

    private static final String NAME = "geospatial_stats";
    public static final String ACTION_OBJECT = "stats";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        String path = String.join(URL_DELIMITER, GeospatialPlugin.getPluginURLPrefix(), ACTION_OBJECT);
        return List.of(new Route(POST, path));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) {
        return channel -> nodeClient.execute(StatsAction.INSTANCE, new StatsRequest(), new RestToXContentListener<>(channel));
    }
}
