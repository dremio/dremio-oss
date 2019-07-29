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

import javax.ws.rs.core.MultivaluedMap;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.jaxrs.cfg.EndpointConfigBase;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterModifier;

/**
 * Object writer modifier that calls {@code previous} modifier before altering a current writer.
 */
public class ObjectWriterModifierChain extends ObjectWriterModifier {

  private final ObjectWriterModifier previous;

  public ObjectWriterModifierChain(ObjectWriterModifier previous) {
    this.previous = previous;
  }

  @Override
  public ObjectWriter modify(EndpointConfigBase<?> endpoint, MultivaluedMap<String, Object> responseHeaders,
                             Object valueToWrite, ObjectWriter w, JsonGenerator g) throws IOException {
    ObjectWriter writer = w;
    if (previous != null) {
      writer = previous.modify(endpoint, responseHeaders, valueToWrite, w, g);
    }
    return writer;
  }
}
