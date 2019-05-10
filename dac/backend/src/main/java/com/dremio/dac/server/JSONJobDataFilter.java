/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;

import com.dremio.dac.explore.model.DataJsonOutput;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.jaxrs.cfg.EndpointConfigBase;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterInjector;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterModifier;

/**
 * A filter that forces job data to write numbers as strings if {@code headerKey} is presented in request headers
 */
public class JSONJobDataFilter implements ContainerResponseFilter {

  private static final class NumberAsStringWriter extends ObjectWriterModifierChain {

    public NumberAsStringWriter(ObjectWriterModifier modifier) {
      super(modifier);
    }

    @Override
    public ObjectWriter modify(EndpointConfigBase<?> endpoint, MultivaluedMap<String, Object> responseHeaders,
                               Object valueToWrite, ObjectWriter w, JsonGenerator g) throws IOException {
      ObjectWriter writer = super.modify(endpoint, responseHeaders, valueToWrite, w, g);

      return DataJsonOutput.setNumbersAsStrings(writer, true);
    }
  };

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
    throws IOException {

    if (!APPLICATION_JSON_TYPE.equals(responseContext.getMediaType())
      || !WebServer.X_DREMIO_JOB_DATA_NUMBERS_AS_STRINGS_SUPPORTED_VALUE
        .equals(requestContext.getHeaderString(WebServer.X_DREMIO_JOB_DATA_NUMBERS_AS_STRINGS))) {
      return;
    }

    ObjectWriterInjector.set(new NumberAsStringWriter(ObjectWriterInjector.getAndClear()));
  }
}
