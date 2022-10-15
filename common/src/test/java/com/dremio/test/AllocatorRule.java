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
package com.dremio.test;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.junit.rules.ExternalResource;

import com.dremio.common.config.SabotConfig;
import com.google.common.base.Preconditions;
import com.typesafe.config.ConfigValueFactory;

/**
 * Allocator rule to automatically create a valid Dremio buffer allocator
 */
public final class AllocatorRule extends ExternalResource {

  private final SabotConfig config;
  private BufferAllocator rootAllocator;

  private AllocatorRule(SabotConfig config) {
    this.config = config;
  }

  /**
   * Creates a root allocator rule using default Sabot config
   * @return
   */
  public static AllocatorRule defaultAllocator() {
    return new AllocatorRule(DremioTest.DEFAULT_SABOT_CONFIG);
  }

  /**
   * Creates a root allocator rule with the provided memory limit
   * @return
   */
  public static AllocatorRule withLimit(long limit) {
    return new AllocatorRule(DremioTest.DEFAULT_SABOT_CONFIG.withValue(RootAllocatorFactory.TOP_LEVEL_MAX_ALLOC,
        ConfigValueFactory.fromAnyRef(limit)));
  }


  public BufferAllocator newAllocator(String name, int initReservation, long maxAllocation) {
    Preconditions.checkState(rootAllocator != null, "Trying to allocate buffer before test is started");
    return rootAllocator.newChildAllocator(name, initReservation, maxAllocation);
  }


  @Override
  protected void before() throws Throwable {
    rootAllocator = RootAllocatorFactory.newRoot(config);
    super.before();
  }

  @Override
  protected void after() {
    super.after();
    if (rootAllocator != null) {
      rootAllocator.close();
    }
  }

}
