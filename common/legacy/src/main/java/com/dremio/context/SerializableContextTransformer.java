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
package com.dremio.context;

import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;
import java.util.Map;

/** Transformer for deserializing context objects from gRPC headers. */
public interface SerializableContextTransformer {
  /** Helper for converting a set of gRPC headers into a map which this transformer can consume. */
  static Map<String, String> convert(final Metadata headers) {
    final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    headers
        .keys()
        .forEach(
            (key) -> {
              final String value =
                  headers.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
              if (value != null) {
                builder.put(key, value);
              }
            });
    return builder.build();
  }

  /**
   * Constructs a context object from the provided map. If the headers are not present, the builder
   * should be returned without modification.
   *
   * @param builder A RequestContext object to extend off of.
   */
  RequestContext deserialize(final Map<String, String> headers, RequestContext builder);
}
