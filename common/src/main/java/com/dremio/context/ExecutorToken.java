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

/**
 * Request Context Holder for executor token
 */
public class ExecutorToken {
  public static final RequestContext.Key<ExecutorToken> CTX_KEY = RequestContext.newKey(
    "executor_token_key");

  private final String executorToken;

  public ExecutorToken(String executorToken) {
    this.executorToken = executorToken;
  }

  public String getExecutorToken() {
    return executorToken;
  }

}
