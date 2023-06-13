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
package com.dremio.sabot.exec;

import java.util.function.Consumer;

import com.dremio.common.exceptions.UserException;

/**
 * This implementation of HeapClawBackStrategy is to be used on coordinator side.
 */
public class CoordinatorHeapClawBackStrategy implements HeapClawBackStrategy {
  private final Consumer<CancelQueryContext> cancelConsumer;
  // CancelQueryContext for cancelling queries in planning phase.
  private static final CancelQueryContext CANCEL_QUERY_CONTEXT =
    new CancelQueryContext(UserException.OOM_MSG,
      "Query cancelled by coordinator heap monitor",
      true);

  public CoordinatorHeapClawBackStrategy(Consumer<CancelQueryContext> cancelConsumer) {
    this.cancelConsumer = cancelConsumer;
  }

  @Override
  public void clawBack() {
    cancelConsumer.accept(CANCEL_QUERY_CONTEXT);
  }

  public static CancelQueryContext getCancelQueryContext() {
    return CANCEL_QUERY_CONTEXT;
  }
}
