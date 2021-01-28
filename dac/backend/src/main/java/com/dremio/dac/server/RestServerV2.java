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

import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.internal.util.PropertiesHelper;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.mvc.freemarker.FreemarkerMvcFeature;

import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.explore.bi.PowerBIMessageBodyGenerator;
import com.dremio.dac.explore.bi.QlikAppMessageBodyGenerator;
import com.dremio.dac.explore.bi.TableauMessageBodyGenerator;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;

import freemarker.core.HTMLOutputFormat;
import freemarker.template.Configuration;

/**
 * Dremio Rest Server.
 */
public class RestServerV2 extends ResourceConfig {
  public static final String FIRST_TIME_API_ENABLE = "dac.rest.config.first-time.enable";
  public static final String TEST_API_ENABLE = "dac.rest.config.test-resources.enable";
  public static final String ERROR_STACKTRACE_ENABLE = "dac.rest.config.stacktrace.enable";
  public static final String DAC_AUTH_FILTER_DISABLE = "dac.rest.config.auth.disable";
  public static final String EE_DAC_AUTH_FILTER_DISABLE = "dac.rest.config.ee-auth.disable";
  public static final String JSON_PRETTYPRINT_ENABLE = "dac.rest.config.json-prettyprint.enable";

  public RestServerV2(ScanResult result) {
    try (TimedBlock b = Timer.time("new RestServer")) {
      init(result);
    }
  }

  protected void init(ScanResult result) {
    // FILTERS //
    register(JSONPrettyPrintFilter.class);
    register(MediaTypeFilter.class);

    // RESOURCES //
    for (Class<?> resource : result.getAnnotatedClasses(RestResource.class)) {
      register(resource);
    }

    // FEATURES
    property(FreemarkerMvcFeature.TEMPLATE_OBJECT_FACTORY, getFreemarkerConfiguration());
    register(FreemarkerMvcFeature.class);
    register(MultiPartFeature.class);
    register(FirstTimeFeature.class);
    register(DACAuthFilterFeature.class);
    register(DACExceptionMapperFeature.class);
    register(DACJacksonJaxbJsonFeature.class);
    register(JSONJobDataFilter.class);
    register(TestResourcesFeature.class);

    // LISTENERS //
    register(TimingApplicationEventListener.class);

    // EXCEPTION MAPPERS //
    register(JsonParseExceptionMapper.class);
    register(JsonMappingExceptionMapper.class);

    // BODY WRITERS //
    registerBIToolMessageBodyGenerators();

    // PROPERTIES //
    property(ServerProperties.RESPONSE_SET_STATUS_OVER_SEND_ERROR, "true");

    final String disableMoxy = PropertiesHelper.getPropertyNameForRuntime(CommonProperties.MOXY_JSON_FEATURE_DISABLE,
        getConfiguration().getRuntimeType());
    property(disableMoxy, true);
  }

  protected void registerBIToolMessageBodyGenerators() {
    //  BODY WRITERS //
    register(QlikAppMessageBodyGenerator.class);
    register(TableauMessageBodyGenerator.class);
    register(PowerBIMessageBodyGenerator.class);
    property(TableauMessageBodyGenerator.CUSTOMIZATION_ENABLED, false);
  }

  private Configuration getFreemarkerConfiguration() {
    Configuration configuration = new Configuration(Configuration.VERSION_2_3_26);
    configuration.setOutputFormat(HTMLOutputFormat.INSTANCE);
    configuration.setClassForTemplateLoading(getClass(), "/");
    return configuration;
  }
}
