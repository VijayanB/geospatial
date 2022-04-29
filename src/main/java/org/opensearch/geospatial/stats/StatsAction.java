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

import org.opensearch.action.ActionType;

public class StatsAction extends ActionType<StatsResponse> {

    public static final StatsAction INSTANCE = new StatsAction();
    public static final String NAME = "cluster:monitor/ingest/geoip/stats";

    public StatsAction() {
        super(NAME, StatsResponse::new);
    }

}
