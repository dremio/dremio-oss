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

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;

import io.grpc.Metadata;

/**
 * Request Context Holder for executor token
 */
public class ExecutorToken implements SerializableContext {
  public static final RequestContext.Key<ExecutorToken> CTX_KEY = RequestContext.newKey(
    "executor_token_key");

  // Note: Public due to usage in some interceptors which do not need deserialization.
  public static final Metadata.Key<String> TOKEN_HEADER_KEY =
    Metadata.Key.of("x-dremio-token-key", Metadata.ASCII_STRING_MARSHALLER);

  private final String executorToken;

  public ExecutorToken(String executorToken) {
    this.executorToken = executorToken;
  }

  public String getExecutorToken() {
    return executorToken;
  }

  @Override
  public void serialize(ImmutableMap.Builder<String, String> builder) {
    builder.put(TOKEN_HEADER_KEY.name(), executorToken);
  }

  public static class Transformer implements SerializableContextTransformer {
    @Override
    public RequestContext deserialize(final Map<String, String> headers, RequestContext builder) {
      if (headers.containsKey(TOKEN_HEADER_KEY.name()) && StringUtils.isNotEmpty(headers.get(TOKEN_HEADER_KEY.name()))) {
        return builder.with(
          ExecutorToken.CTX_KEY,
          new ExecutorToken(headers.get(TOKEN_HEADER_KEY.name())));
      }

      return builder;
    }
  }
}
