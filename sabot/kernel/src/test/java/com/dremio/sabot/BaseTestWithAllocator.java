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
package com.dremio.sabot;

import com.dremio.common.AutoCloseables;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

public abstract class BaseTestWithAllocator extends DremioTest {

  protected BufferAllocator allocator;

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setup() {
    this.allocator = allocatorRule.newAllocator(this.getClass().getSimpleName(), 0, Long.MAX_VALUE);
  }

  @After
  public void close() throws Exception {
    AutoCloseables.close(allocator);
  }
}
