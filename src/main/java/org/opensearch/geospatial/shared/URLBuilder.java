/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.shared;

public class URLBuilder {

    public static final String NAME = "geospatial";
    public static final String PLUGIN_PREFIX = "_plugins";
    public static final String URL_DELIMITER = "/";

    public static String getPluginURLPrefix() {
        return String.join(URL_DELIMITER, PLUGIN_PREFIX, NAME);
    }
}
