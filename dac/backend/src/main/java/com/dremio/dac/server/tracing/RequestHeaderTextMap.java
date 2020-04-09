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
package com.dremio.dac.server.tracing;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import javax.ws.rs.core.MultivaluedMap;

import io.opentracing.propagation.TextMap;

/**
 * Custom TextMap that supports request headers.
 */
public class RequestHeaderTextMap implements TextMap {
  private final MultivaluedMap<String, String> headers;

  public RequestHeaderTextMap(MultivaluedMap<String, String> headers) {
    this.headers = headers;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<Map.Entry<String, String>> iterator() {
    return (Iterator<Map.Entry<String, String>>)(Object) headers.entrySet().stream()
      .flatMap(e -> e.getValue() != null ?
        e.getValue().stream().map(v -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), v)) :
        Stream.of(new AbstractMap.SimpleImmutableEntry<>(e.getKey(), (String) null))
      ).iterator();
  }

  @Override
  public void put(String key, String value) {
    throw new UnsupportedOperationException();
  }
}
