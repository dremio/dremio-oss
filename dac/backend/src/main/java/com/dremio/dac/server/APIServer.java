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
package com.dremio.dac.server;

import com.dremio.common.perf.Timer;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.RestApiServer;
import javax.inject.Inject;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.internal.util.PropertiesHelper;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

/** Dremio Rest API Server. */
@RestApiServer(pathSpec = "/api/v3/*", tags = "oss")
public class APIServer extends ResourceConfig {
  @Inject
  public APIServer(ScanResult result) {
    try (Timer.TimedBlock b = Timer.time("new APIServer")) {
      init(result);
    }
  }

  protected void init(ScanResult result) {
    // FILTERS
    register(JSONPrettyPrintFilter.class);
    register(MediaTypeFilter.class);

    // RESOURCES
    for (Class<?> resource : result.getAnnotatedClasses(APIResource.class)) {
      register(resource);
    }

    // Enable request contextualization.
    register(new AuthenticationBinder());

    // FEATURES
    register(DACAuthFilterFeature.class);
    register(DACJacksonJaxbJsonFeature.class);
    register(DACExceptionMapperFeature.class);

    // EXCEPTION MAPPERS
    register(RestApiJsonParseExceptionMapper.class);
    register(RestApiJsonMappingExceptionMapper.class);

    // PROPERTIES
    property(ServerProperties.RESPONSE_SET_STATUS_OVER_SEND_ERROR, "true");

    final String disableMoxy =
        PropertiesHelper.getPropertyNameForRuntime(
            CommonProperties.MOXY_JSON_FEATURE_DISABLE, getConfiguration().getRuntimeType());
    property(disableMoxy, true);
  }
}
