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
package com.dremio.dac.resource;

import javax.annotation.PreDestroy;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.dac.server.BufferAllocatorFactory;
import com.google.common.base.Preconditions;

/**
 * Handles BufferAllocators for RestResources
 */
public class BaseResourceWithAllocator {
  private final BufferAllocatorFactory allocatorFactory;
  private final boolean needToClose;
  private BufferAllocator requestAllocator;


  // Regular use constructor
  public BaseResourceWithAllocator(BufferAllocatorFactory allocatorFactory) {
    this.allocatorFactory = Preconditions.checkNotNull(allocatorFactory, "BufferAllocatorFactory must be provided.");
    this.needToClose = true;
  }

  // Case for non-REST instantiations, assumes sub-class will manage allocator
  public BaseResourceWithAllocator(BufferAllocator allocator) {
    this.allocatorFactory = null;
    this.requestAllocator = Preconditions.checkNotNull(allocator, "Allocator must be provided.");
    this.needToClose = false;
  }

  @PreDestroy
  public void preDestroy() {
    if (requestAllocator != null && needToClose) {
      requestAllocator.close();
    }
  }

  public BufferAllocator getOrCreateAllocator(String name) {
    if (requestAllocator == null) {
      requestAllocator = allocatorFactory.newChildAllocator(getClass().getName() + "-" + name);
    }
    return requestAllocator;
  }
}
