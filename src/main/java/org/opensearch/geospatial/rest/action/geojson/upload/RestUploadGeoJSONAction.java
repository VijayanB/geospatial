/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.rest.action.geojson.upload;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

import java.util.List;
import java.util.Map;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.ParseField;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.action.geojson.upload.UploadGeoJSONAction;
import org.opensearch.geospatial.action.geojson.upload.UploadGeoJSONRequest;
import org.opensearch.geospatial.plugin.GeospatialPlugin;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

/**
 * Rest Action handler to accepts requests and route for geojson upload action
 */
public class RestUploadGeoJSONAction extends BaseRestHandler {

    public static final String ACTION_OBJECT = "geojson";
    public static final String ACTION_UPLOAD = "_upload";
    public static final String NAME = "upload_geojson_action";
    public static final String URL_DELIMITER = "/";

    public static ParseField FIELD_INDEX = new ParseField("index", null);

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Supported Routes are
     * POST /_plugins/geospatial/gejson/_upload
     * {
     *     "index" : "index_name_that_does_not_exists"
     *     //TODO https://github.com/opensearch-project/geospatial/issues/29 (implement rest of request body)
     * }
     *
     */
    @Override
    public List<Route> routes() {
        String path = String.join(URL_DELIMITER, GeospatialPlugin.getPluginURLPrefix(), ACTION_OBJECT, ACTION_UPLOAD);
        return singletonList(new Route(POST, path));
    }

    private String getValueAsString(Map<String, Object> input, ParseField key) {
        Object value = input.get(key.getPreferredName());
        if (value == null) {
            throw new IllegalArgumentException(input.toString());
        }
        if (!(value instanceof String)) {
            throw new IllegalArgumentException(key.getPreferredName() + " is not an instance of String");
        }
        return value.toString();
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        Map<String, Object> body = XContentHelper.convertToMap(restRequest.content(), false, XContentType.JSON).v2();
        UploadGeoJSONRequest request = new UploadGeoJSONRequest(getValueAsString(body, FIELD_INDEX));
        return channel -> client.execute(UploadGeoJSONAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
