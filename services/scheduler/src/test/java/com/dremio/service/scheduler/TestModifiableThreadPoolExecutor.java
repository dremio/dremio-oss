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

package com.dremio.service.scheduler;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.PositiveLongValidator;

/**
 * Tests ModifiableThreadPoolExecutor
 */
public class TestModifiableThreadPoolExecutor {
  private final PositiveLongValidator option = new PositiveLongValidator("test-option", Integer.MAX_VALUE,1);
  private ThreadPoolExecutor threadPoolExecutor;
  private ModifiableThreadPoolExecutor modifiableThreadPoolExecutor;
  private OptionManager optionManager;

  @Before
  public void setUp() throws Exception {
    threadPoolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue<Runnable>());
    optionManager = mock(OptionManager.class);
    when(optionManager.getOption(option)).thenReturn(1L);
    modifiableThreadPoolExecutor = new ModifiableThreadPoolExecutor(threadPoolExecutor, option, optionManager);
  }

  @After
  public void tearDown() throws Exception {
    threadPoolExecutor.shutdown();
  }

  /**
   * When option value is changed, verify that corePoolSize and maxPoolSize
   * are modified to expected value.
   */
  @Test
  public void testModifyOption() {
    // before
    assertEquals(1, threadPoolExecutor.getCorePoolSize());

    // modify
    long newPoolSize = 10;
    when(optionManager.getOption(option)).thenReturn(newPoolSize);
    modifiableThreadPoolExecutor.onChange(); // simulate option onChange trigger.

    // after
    assertEquals(newPoolSize, threadPoolExecutor.getCorePoolSize());
    assertEquals(threadPoolExecutor.getCorePoolSize(), threadPoolExecutor.getMaximumPoolSize());
  }
}
