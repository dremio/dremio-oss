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
package com.dremio.exec.planner.physical.rule.computation;

import org.junit.Assert;
import org.junit.Test;

public class JoinSideTest {

  @Test
  public void testMerge() {
    // BOTH
    Assert.assertEquals(JoinSide.BOTH, JoinSide.BOTH.merge(JoinSide.BOTH));
    Assert.assertEquals(JoinSide.BOTH, JoinSide.BOTH.merge(JoinSide.EMPTY));
    Assert.assertEquals(JoinSide.BOTH, JoinSide.BOTH.merge(JoinSide.LEFT));
    Assert.assertEquals(JoinSide.BOTH, JoinSide.BOTH.merge(JoinSide.RIGHT));

    // EMPTY
    Assert.assertEquals(JoinSide.BOTH, JoinSide.EMPTY.merge(JoinSide.BOTH));
    Assert.assertEquals(JoinSide.EMPTY, JoinSide.EMPTY.merge(JoinSide.EMPTY));
    Assert.assertEquals(JoinSide.LEFT, JoinSide.EMPTY.merge(JoinSide.LEFT));
    Assert.assertEquals(JoinSide.RIGHT, JoinSide.EMPTY.merge(JoinSide.RIGHT));

    // LEFT
    Assert.assertEquals(JoinSide.BOTH, JoinSide.LEFT.merge(JoinSide.BOTH));
    Assert.assertEquals(JoinSide.LEFT, JoinSide.LEFT.merge(JoinSide.EMPTY));
    Assert.assertEquals(JoinSide.LEFT, JoinSide.LEFT.merge(JoinSide.LEFT));
    Assert.assertEquals(JoinSide.BOTH, JoinSide.LEFT.merge(JoinSide.RIGHT));

    // RIGHT
    Assert.assertEquals(JoinSide.BOTH, JoinSide.RIGHT.merge(JoinSide.BOTH));
    Assert.assertEquals(JoinSide.RIGHT, JoinSide.RIGHT.merge(JoinSide.EMPTY));
    Assert.assertEquals(JoinSide.BOTH, JoinSide.RIGHT.merge(JoinSide.LEFT));
    Assert.assertEquals(JoinSide.RIGHT, JoinSide.RIGHT.merge(JoinSide.RIGHT));
  }
}
