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
package com.dremio.sabot.memory;

import com.dremio.common.config.SabotConfig;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.FragmentExecutors;
import com.dremio.sabot.exec.QueriesClerk;
import org.apache.arrow.memory.RootAllocator;

/** This class has a default implementation of the MemoryArbiter */
public class DefaultMemoryArbiter implements MemoryArbiter {
  public DefaultMemoryArbiter(RootAllocator rootAllocator) {}

  @Override
  public void taskDone(MemoryArbiterTask memoryArbiterTask) {}

  @Override
  public void startTask(MemoryArbiterTask memoryArbiterTask) {}

  /** The requested memory is always granted */
  @Override
  public boolean acquireMemoryGrant(MemoryArbiterTask memoryArbiterTask, long memoryGrantInBytes) {
    return true;
  }

  @Override
  public void releaseMemoryGrant(MemoryArbiterTask memoryArbiterTask) {}

  @Override
  public boolean removeFromBlocked(MemoryArbiterTask memoryArbiterTask) {
    return true;
  }

  @Override
  public void removeFromSpilling(MemoryTaskAndShrinkableOperator memoryTaskAndShrinkableOperator) {}

  @Override
  public void addTaskToQueue(MemoryArbiterTask memoryArbiterTask) {}

  @Override
  public void close() throws Exception {}

  public static class Factory implements MemoryArbiterFactory {
    @Override
    public MemoryArbiter newInstance(
        SabotConfig sabotConfig,
        RootAllocator rootAllocator,
        FragmentExecutors fragmentExecutors,
        QueriesClerk clerk,
        OptionManager options) {
      return new DefaultMemoryArbiter(rootAllocator);
    }
  }
}
