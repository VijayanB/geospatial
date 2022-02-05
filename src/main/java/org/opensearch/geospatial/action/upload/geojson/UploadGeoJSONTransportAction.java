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

package org.opensearch.geospatial.action.upload.geojson;

import java.util.Map;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.geospatial.GeospatialParser;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * TransportAction to handle import operation
 */
public class UploadGeoJSONTransportAction extends HandledTransportAction<UploadGeoJSONRequest, AcknowledgedResponse> {

    private final Client client;

    @Inject
    public UploadGeoJSONTransportAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(UploadGeoJSONAction.NAME, transportService, actionFilters, UploadGeoJSONRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, UploadGeoJSONRequest request, ActionListener<AcknowledgedResponse> actionListener) {
        Map<String, Object> contentAsMap = GeospatialParser.convertToMap(request.getContent());
        UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(contentAsMap);
        Uploader uploader = new Uploader(client, content, true, actionListener);
        try {
            uploader.upload();
        } catch (Exception e) {
            actionListener.onFailure(e);
        }
    }
}
