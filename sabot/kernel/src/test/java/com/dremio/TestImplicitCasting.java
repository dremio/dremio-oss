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
package com.dremio;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.resolver.TypeCastRules;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.google.common.collect.Lists;

public class TestImplicitCasting {
  @Test
  public void testTimeStampAndTime() {
    final List<MinorType> inputTypes = Lists.newArrayList();
    inputTypes.add(MinorType.TIME);
    inputTypes.add(MinorType.TIMESTAMP);
    final OptionManager optionManager = mock(OptionManager.class);
    Mockito.when(optionManager.getOption(PlannerSettings.ENABLE_DECIMAL_V2_KEY)).thenReturn
      (OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, PlannerSettings
        .ENABLE_DECIMAL_V2_KEY, true));
    final MinorType result = TypeCastRules.getLeastRestrictiveType(inputTypes);

    assertEquals(result, MinorType.TIME);
  }
}
