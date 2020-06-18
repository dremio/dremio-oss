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
package com.dremio.exec.server.options;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;

public class TestEagerCachingOptionManager {
  private OptionManager optionManager;
  private OptionList optionList;
  private OptionValue optionValueA;
  private OptionValue optionValueB;
  private OptionValue optionValueC;

  @Before
  public void setup() {
    optionManager = mock(OptionManager.class);

    optionList = new OptionList();
    optionValueA = OptionValue.createDouble(OptionValue.OptionType.SYSTEM, "testOptionA", 2);
    optionValueB = OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "testOptionB", true);
    optionValueC = OptionValue.createString(OptionValue.OptionType.SYSTEM, "testOptionC", "someValue");
    optionList.add(optionValueA);
    optionList.add(optionValueB);
    optionList.add(optionValueC);

    when(optionManager.getNonDefaultOptions()).thenReturn(optionList);
  }

  @Test
  public void testInitialization() {
    new EagerCachingOptionManager(optionManager);
    verify(optionManager, times(1)).getNonDefaultOptions();
  }

  @Test
  public void testGetOption() {
    final OptionManager eagerCachingOptionManager = new EagerCachingOptionManager(optionManager);
    assertEquals(optionValueA, eagerCachingOptionManager.getOption(optionValueA.getName()));
    // getOption should not touch underlying option manager
    verify(optionManager, times(0)).getOption(optionValueA.getName());
  }

  @Test
  public void testSetOption() {
    final OptionManager eagerCachingOptionManager = new EagerCachingOptionManager(optionManager);
    final OptionValue newOption = OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "newOption", true);
    eagerCachingOptionManager.setOption(newOption);
    // setOption should write-through to underlying option manager
    verify(optionManager, times(1)).setOption(newOption);
  }

  @Test
  public void testDeleteOption() {
    final OptionManager eagerCachingOptionManager = new EagerCachingOptionManager(optionManager);
    eagerCachingOptionManager.deleteOption(optionValueC.getName(), OptionValue.OptionType.SYSTEM);
    assertNull(eagerCachingOptionManager.getOption(optionValueC.getName()));

    // deleteOption should write-through to underlying option manager
    verify(optionManager, times(1)).deleteOption(optionValueC.getName(), OptionValue.OptionType.SYSTEM);
  }

  @Test
  public void testDeleteAllOptions() {
    final OptionManager eagerCachingOptionManager = new EagerCachingOptionManager(optionManager);
    eagerCachingOptionManager.deleteAllOptions(OptionValue.OptionType.SYSTEM);

    assertNull(eagerCachingOptionManager.getOption(optionValueA.getName()));
    assertNull(eagerCachingOptionManager.getOption(optionValueB.getName()));
    assertNull(eagerCachingOptionManager.getOption(optionValueC.getName()));

    // deleteOption should write-through to underlying option manager
    verify(optionManager, times(1)).deleteAllOptions(OptionValue.OptionType.SYSTEM);
  }
}
