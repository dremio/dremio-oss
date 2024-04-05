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
package com.dremio.service.reflection.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.dremio.datastore.VersionExtractor;
import com.dremio.service.reflection.proto.ReflectionGoal;
import org.junit.Test;

/** Test functionality on the ReflectionGoalsStore. */
public class TestReflectionGoalsStore {

  @Test
  public void testVersionExtractorWithNumericTag() {
    final VersionExtractor<ReflectionGoal> goalVersionExtractor =
        new ReflectionGoalsStore.ReflectionGoalVersionExtractor();
    final String oldNumericTag = "1";
    final String newNonNumericTag = "testTag";
    final ReflectionGoal goal =
        ReflectionGoal.getDefaultInstance().newMessage().setTag(oldNumericTag).setVersion(null);

    goalVersionExtractor.setTag(goal, newNonNumericTag);
    assertEquals(newNonNumericTag, goal.getTag());
    assertEquals(Long.valueOf(oldNumericTag), goal.getVersion());
  }

  @Test
  public void testVersionExtractorWithNonNumericTag() {
    final VersionExtractor<ReflectionGoal> goalVersionExtractor =
        new ReflectionGoalsStore.ReflectionGoalVersionExtractor();
    final String oldNonNumericTag = "abc";
    final String newNonNumericTag = "testTag";
    final ReflectionGoal goal =
        ReflectionGoal.getDefaultInstance().newMessage().setTag(oldNonNumericTag).setVersion(null);

    goalVersionExtractor.setTag(goal, newNonNumericTag);
    assertEquals(newNonNumericTag, goal.getTag());
    assertNull(goal.getVersion());
  }
}
