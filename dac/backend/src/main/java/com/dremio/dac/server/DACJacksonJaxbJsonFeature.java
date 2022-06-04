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

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.inject.Inject;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.internal.InternalProperties;
import org.glassfish.jersey.internal.util.PropertiesHelper;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.explore.model.VirtualDatasetUIMixin;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.util.JSONUtil;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.server.BootStrapContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

/**
 * DAC Feature to add Jackson provider
 */
public class DACJacksonJaxbJsonFeature implements Feature {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(DACJacksonJaxbJsonFeature.class);
  private static final String JSON_FEATURE_CLASSNAME = DACJacksonJaxbJsonFeature.class.getSimpleName();

  /**
   * Jackson provider performing input validation
   */
  public static final class DACJacksonJaxbJsonProvider extends JacksonJaxbJsonProvider {
    private final InputValidation validation = new InputValidation();

    @Inject
    public DACJacksonJaxbJsonProvider(Configuration configuration, BootStrapContext context, ConnectionReader connectionReader) {
      this.setMapper(newObjectMapper(configuration, context.getClasspathScan(), connectionReader));
    }

    @Override
    public Object readFrom(Class<Object> type,
        Type genericType,
        Annotation[] annotations,
        javax.ws.rs.core.MediaType mediaType,
        javax.ws.rs.core.MultivaluedMap<String,String> httpHeaders,
        InputStream entityStream) throws IOException {
      Object o = super.readFrom(type, genericType, annotations, mediaType, httpHeaders, entityStream);
      // Some POST requests does not accept body and we need to enhance the requests to accept body and also to make it backwards compatible
      // For e.g: new_untitled API
      if (o != null) {
        validation.validate(o);
      }
      return o;
    }

    protected ObjectMapper newObjectMapper(Configuration configuration, ScanResult scanResult, ConnectionReader connectionReader) {
      Boolean property = PropertyHelper.getProperty(configuration, RestServerV2.JSON_PRETTYPRINT_ENABLE);
      final boolean prettyPrint = property != null && property;

      ObjectMapper mapper = prettyPrint ? JSONUtil.prettyMapper() : JSONUtil.mapper();

      JSONUtil.registerStorageTypes(mapper, scanResult, connectionReader);
      mapper.addMixIn(VirtualDatasetUI.class, VirtualDatasetUIMixin.class);
      return mapper;
    }
  }

  @Override
  public boolean configure(FeatureContext context) {
    final Configuration config = context.getConfiguration();

    final String jsonFeature = CommonProperties.getValue(config.getProperties(), config.getRuntimeType(),
      InternalProperties.JSON_FEATURE, JSON_FEATURE_CLASSNAME, String.class);
      // Other JSON providers registered.
      if (!JSON_FEATURE_CLASSNAME.equalsIgnoreCase(jsonFeature)) {
        LOGGER.error("Another JSON provider has been registered: {}", jsonFeature);
        return false;
      }

      // Disable other JSON providers.
      context.property(PropertiesHelper.getPropertyNameForRuntime(InternalProperties.JSON_FEATURE, config.getRuntimeType()),
        JSON_FEATURE_CLASSNAME);

    context.register(DACJacksonJaxbJsonProvider.class);

    return true;
  }
}
