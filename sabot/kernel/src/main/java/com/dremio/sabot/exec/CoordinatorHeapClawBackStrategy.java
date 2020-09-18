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

import java.util.Set;
import java.util.function.BiConsumer;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.google.common.collect.Sets;

/**
 * This implementation of HeapClawBackStrategy is to be used on coordinator side.
 */
public class CoordinatorHeapClawBackStrategy implements HeapClawBackStrategy {
  private final BiConsumer<Set<QueryState>, String> cancelBiConsumer;

  public CoordinatorHeapClawBackStrategy(BiConsumer<Set<QueryState>, String> biConsumer) {
    cancelBiConsumer = biConsumer;
  }

  @Override
  public void clawBack() {
    // Cancel queries in all QueryStates.
    cancelBiConsumer.accept(Sets.newHashSet(QueryState.values()),
                            UserException.MEMORY_ERROR_MSG);
  }
}
