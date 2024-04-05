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
package com.dremio.options;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.dremio.options.OptionValue.OptionType;
import com.google.common.collect.TreeMultimap;
import java.util.SortedSet;
import org.junit.Test;

/** Test for {@code OptionList} classes */
public class TestOptionList {

  @Test
  public void testMergeOptionLists() {
    final OptionValue optionValue0 =
        OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "test-option0", false);
    final OptionValue optionValue1 =
        OptionValue.createLong(OptionValue.OptionType.SYSTEM, "test-option1", 2);
    final OptionValue optionValue2 =
        OptionValue.createLong(OptionValue.OptionType.SESSION, "test-option2", 4);
    final OptionValue optionValue3 =
        OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "test-option0", true);
    final OptionValue optionValue4 =
        OptionValue.createLong(OptionValue.OptionType.SESSION, "test-option1", 2);
    final OptionList localList = new OptionList();
    localList.add(optionValue0);
    localList.add(optionValue1);
    localList.add(optionValue2);
    final OptionList changedList = new OptionList();
    changedList.add(optionValue3);
    changedList.add(optionValue4);
    final OptionList expectedList = new OptionList();
    expectedList.add(optionValue3);
    expectedList.add(optionValue4);
    expectedList.add(optionValue1);
    expectedList.add(optionValue2);
    changedList.mergeIfNotPresent(localList);
    assertEquals(expectedList, changedList);
  }

  @Test
  public void testOptionValueCompareOrder() {
    // Check OptionType order: QUERY > SESSION > SYSTEM > BOOT
    final OptionValue optionValue0 =
        OptionValue.createLong(OptionValue.OptionType.SYSTEM, "test-option", 2);
    final OptionValue optionValue1 =
        OptionValue.createLong(OptionValue.OptionType.SESSION, "test-option", 3);
    final OptionValue optionValue2 =
        OptionValue.createLong(OptionValue.OptionType.QUERY, "test-option", 4);
    final TreeMultimap<String, OptionValue> optionMap = TreeMultimap.create();
    optionMap.put(optionValue0.getName(), optionValue0);
    optionMap.put(optionValue1.getName(), optionValue1);
    optionMap.put(optionValue2.getName(), optionValue2);
    final OptionType maxType = optionMap.get("test-option").last().getType();
    final OptionType minType = optionMap.get("test-option").first().getType();
    assertEquals(OptionType.QUERY, maxType);
    assertEquals(OptionType.SYSTEM, minType);
    SortedSet<OptionValue> values = optionMap.get("nonexisting-test-option");
    assertThat(values).isNotNull().isEmpty();
  }
}
