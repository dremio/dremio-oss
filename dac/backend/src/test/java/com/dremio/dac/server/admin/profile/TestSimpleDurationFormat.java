/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.server.admin.profile;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Test translation of millisecond durations into human readable format
 */
public class TestSimpleDurationFormat {
  private void validateDurationFormat(long durationInMillisec, String expected) {
    assertEquals(expected, SimpleDurationFormat.format(durationInMillisec));
  }

  @Test
  public void testCompactTwoDigitMilliSec() {
    validateDurationFormat(45, "0.045s");
  }

  @Test
  public void testCompactSecMillis() {
    validateDurationFormat(4545, "4.545s");
  }

  @Test
  public void testCompactMinSec() {
    validateDurationFormat(454534, "7m34s");
  }

  @Test
  public void testCompactHourMin() {
    validateDurationFormat(4545342, "1h15m");
  }

  @Test
  public void testCompactHalfDayHourMin() {
    validateDurationFormat(45453420, "12h37m");
  }

  @Test
  public void testCompactOneDayHourMin() {
    validateDurationFormat(45453420 + 86400000, "1d12h37m");
  }

  @Test
  public void testCompactManyDayHourMin() {
    validateDurationFormat(45453420 + 20*86400000, "20d12h37m");
  }
}

