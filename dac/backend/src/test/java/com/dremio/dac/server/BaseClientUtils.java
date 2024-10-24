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

import static com.dremio.common.utils.PathUtils.getKeyJoiner;
import static com.dremio.common.utils.PathUtils.getPathJoiner;
import static com.dremio.dac.server.FamilyExpectation.SUCCESS;
import static org.glassfish.jersey.CommonProperties.FEATURE_AUTO_DISCOVERY_DISABLE;

import com.dremio.common.SentinelSecure;
import com.dremio.common.perf.Timer;
import com.dremio.dac.explore.model.DataPOJO;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.ViewFieldTypeMixin;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.util.JSONUtil;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.test.DremioTest;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import java.io.IOException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

/** Utils for web client. */
public class BaseClientUtils {

  protected static final MediaType JSON = MediaType.APPLICATION_JSON_TYPE;
  protected static final String API_LOCATION = "apiv2";

  protected static <T> JsonDeserializer<T> newJsonDeserializer(final Class<? extends T> clazz) {
    return new JsonDeserializer<T>() {
      @Override
      public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        return p.readValueAs(clazz);
      }
    };
  }

  public static ObjectMapper newClientObjectMapper() {
    ObjectMapper objectMapper = JSONUtil.prettyMapper();
    JSONUtil.registerStorageTypes(
        objectMapper,
        DremioTest.CLASSPATH_SCAN_RESULT,
        ConnectionReader.of(DremioTest.CLASSPATH_SCAN_RESULT, DremioTest.DEFAULT_SABOT_CONFIG));
    objectMapper
        .registerModule(
            new SimpleModule()
                .addDeserializer(JobDataFragment.class, newJsonDeserializer(DataPOJO.class)))
        .addMixIn(ViewFieldType.class, ViewFieldTypeMixin.class);
    objectMapper.setFilterProvider(
        new SimpleFilterProvider()
            .addFilter(SentinelSecure.FILTER_NAME, SentinelSecureFilter.TEST_ONLY));
    return objectMapper;
  }

  public static Client newClient(ObjectMapper mapper) {
    final JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    provider.setMapper(mapper);

    return ClientBuilder.newBuilder()
        .property(FEATURE_AUTO_DISCOVERY_DISABLE, true)
        .register(provider)
        .register(MultiPartFeature.class)
        .build();
  }

  protected Response expectStatus(Response.StatusType status, Invocation i) {
    return expect(new StatusExpectation(status), i);
  }

  protected <T> T expectStatus(Response.StatusType status, Invocation i, GenericType<T> c) {
    return readEntity(expect(new StatusExpectation(status), i), c);
  }

  protected <T> T expectStatus(Response.StatusType status, Invocation i, Class<T> c) {
    return readEntity(expect(new StatusExpectation(status), i), new GenericType<T>(c));
  }

  protected Response expectSuccess(Invocation i) {
    return expect(SUCCESS, i);
  }

  protected <T> T expectSuccess(Invocation i, Class<T> c) {
    return expectSuccess(i, new GenericType<T>(c));
  }

  protected <T> T expectSuccess(Invocation i, GenericType<T> c) {
    return readEntity(expectSuccess(i), c);
  }

  protected <T> T expectError(FamilyExpectation error, Invocation i, Class<T> c) {
    return readEntity(expect(error, i), new GenericType<T>(c));
  }

  private <T> T readEntity(Response response, GenericType<T> c) {
    response.bufferEntity();
    try {
      return response.readEntity(c);
    } catch (ProcessingException e) {
      String body = response.readEntity(String.class);
      throw new RuntimeException("Error deserializing entity with " + c + " : " + body, e);
    }
  }

  protected Response expect(ResponseExpectation expectation, Invocation i) {
    try (Timer.TimedBlock b = Timer.time("request")) {
      Response response = i.invoke();
      response.bufferEntity();
      try {
        expectation.validate(response);
      } catch (AssertionError e) {
        // this will show the body of the response in the error message
        // if an error occurred it will show the server side error.
        // response.toString() does not show the content
        String body = response.readEntity(String.class);
        throw new AssertionError(
            String.format("%s\n%s\n%s", e.getMessage(), response.toString(), body), e);
      }
      return response;
    }
  }

  protected String versionedResourcePath(DatasetUI datasetUI) {
    return getPathJoiner()
        .join(
            "/dataset",
            getKeyJoiner().join(datasetUI.getFullPath()),
            "version",
            datasetUI.getDatasetVersion());
  }

  protected String resourcePath(DatasetUI datasetUI) {
    return getPathJoiner().join("/dataset", getKeyJoiner().join(datasetUI.getFullPath()));
  }
}
