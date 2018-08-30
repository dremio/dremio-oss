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
package com.dremio.exec.planner.physical.visitor;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.options.OptionManager;

public class MemoryEstimationVisitor extends BasePrelVisitor<Double, Void, RuntimeException> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryEstimationVisitor.class);

  public static boolean enoughMemory(Prel prel, OptionManager options, int numNodes) {
    long allottedMemory = options.getOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY).getNumVal() * numNodes;
    long estimatedMemory = (long) Math.ceil(prel.accept(new MemoryEstimationVisitor(), null) / (1024.0 * 1024.0));
    estimatedMemory += options.getOption(ExecConstants.NON_BLOCKING_OPERATORS_MEMORY_KEY).getNumVal() * numNodes;

    if (estimatedMemory > allottedMemory) {
      logger.debug("Estimated memory (" + estimatedMemory + ") exceeds maximum allowed (" + allottedMemory + ")");
    } else {
      logger.debug("Estimated memory (" + estimatedMemory + ") within maximum allowed (" + allottedMemory + ")");
    }
    return estimatedMemory <= allottedMemory;
  }
}
