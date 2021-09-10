/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.dac.explore.bi;

/**
 * Class capturing key/value attributes of fields in the Tableau SDK Connector.
 * Values compatible with Dremio Tableau SDK Connector 2021.2 or earlier.
 *
 * Please refer to https://tableau.github.io/connector-plugin-sdk/docs/mcd for latest SDK updates.
 */
public final class TableauSDKConstants {
    public static final String CONN_ATTR = "connection";
    public static final String CLASS = "class";
    public static final String DBNAME = "dbname";
    public static final String SERVER = "server";
    public static final String PORT = "port";
    public static final String USERNAME = "username";
    public static final String SCHEMA = "schema";
    public static final String PRODUCT = "v-dremio-product";
    public static final String SOFTWARE = "v-software";

    // Authentication Modes
    public static final String AUTHENTICATION = "authentication";
    public static final String BASIC = "basic";

    // SSL
    public static final String SSL = "sslmode";
    public static final String REQUIRE = "require";
    public static final String NOT_REQUIRE = "";

    // Advanced Properties
    public static final String ENGINE = "v-engine";
    public static final String QUEUE = "v-routing-queue";
    public static final String TAG = "v-routing-tag";
}
