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

import com.dremio.common.exceptions.UserException;
import java.util.function.Consumer;

/** This implementation of HeapClawBackStrategy is to be used on the coordinator side. */
public class CoordinatorHeapClawBackStrategy implements HeapClawBackStrategy {
  private final Consumer<CancelQueryContext> cancelConsumer;
  // CancelQueryContext for cancelling queries in the planning phase.
  private static final CancelQueryContext CANCEL_QUERY_CONTEXT =
      new CancelQueryContext(
          UserException.OOM_MSG, "Query cancelled by coordinator heap monitor", true);

  public CoordinatorHeapClawBackStrategy(Consumer<CancelQueryContext> cancelConsumer) {
    this.cancelConsumer = cancelConsumer;
  }

  @Override
  public void clawBack(HeapClawBackContext heapClawBackContext) {
    // Currently, there's only one trigger for heap clawbacks on coordinators - heap monitor
    // If this changes, we won't be able to pass the constant CancelQueryContext like we do here,
    // and we'll need to react to what's passed in heapClawBackContext
    cancelConsumer.accept(CANCEL_QUERY_CONTEXT);
  }

  public static CancelQueryContext getCancelQueryContext() {
    return CANCEL_QUERY_CONTEXT;
  }
}
