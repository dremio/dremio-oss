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
package com.dremio.dac.server;

import com.dremio.common.AutoCloseables;
import com.dremio.service.Service;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.memory.BufferAllocator;

/** Factory for BufferAllocators to inject child allocators. */
public class BufferAllocatorFactory implements Service {
  private final BufferAllocator allocator;

  public BufferAllocatorFactory(BufferAllocator bootStrapAllocator, String name) {
    allocator = bootStrapAllocator.newChildAllocator(name, 0, Long.MAX_VALUE);
  }

  public BufferAllocator newChildAllocator(String allocatorName) {
    return allocator.newChildAllocator(allocatorName, 0, Long.MAX_VALUE);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(allocator);
  }

  @Override
  public void start() {}

  @VisibleForTesting
  BufferAllocator getBaseAllocator() {
    return allocator;
  }
}
