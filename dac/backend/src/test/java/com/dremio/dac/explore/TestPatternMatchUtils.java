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
package com.dremio.dac.explore;

import static com.dremio.dac.explore.PatternMatchUtils.CharType.DIGIT;
import static com.dremio.dac.explore.PatternMatchUtils.CharType.WORD;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.regex.Matcher;

import org.junit.Test;

/**
 * Tests for {@link PatternMatchUtils}
 */
public class TestPatternMatchUtils {
  @Test
  public void testRegex() {
    assertTrue(DIGIT.isTypeOf("123"));
    assertFalse(DIGIT.isTypeOf("abc"));
    assertTrue(WORD.isTypeOf("abc"));
    assertTrue(WORD.isTypeOf("123"));
    assertFalse(WORD.isTypeOf("a,bc"));
    assertFalse(WORD.isTypeOf("a bc"));
    Matcher matcher = WORD.matcher("abc def ghi");
    assertTrue(matcher.find());
  }
}
