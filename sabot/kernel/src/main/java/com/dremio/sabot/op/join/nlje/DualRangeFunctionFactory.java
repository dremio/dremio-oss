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
package com.dremio.sabot.op.join.nlje;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.record.VectorAccessible;

public interface DualRangeFunctionFactory {

  DualRange create(BufferAllocator allocator,
      VectorAccessible left,
      VectorAccessible right,
      int targetOutputSize,
      int targetGeneratedAtOnce,
      int[] buildCounts,
      LogicalExpression vectorExpression) throws Exception;
}
