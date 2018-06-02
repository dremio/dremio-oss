/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package org.joda.time.chrono;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Locale;

import org.junit.Test;

public class DayOfWeekFromSundayDateTimeFieldTest {

  private static final DayOfWeekFromSundayDateTimeField instance =
      new DayOfWeekFromSundayDateTimeField(
          ISOChronology.getInstanceUTC(),
          ISOChronology.getInstanceUTC().days());

  @Test
  public void get() {
    assertEquals(instance.get(1526173261000L), 1);
    assertEquals(instance.get(1526259661000L), 2);
    assertEquals(instance.get(1526086861000L), 7);
  }

  @Test
  public void getAsText() {
    assertTrue("Sunday".equalsIgnoreCase(instance.getAsText(1526173261000L)));
    assertTrue("Monday".equalsIgnoreCase(instance.getAsText(1526259661000L)));
    assertTrue("Saturday".equalsIgnoreCase(instance.getAsText(1526086861000L)));
  }

  @Test
  public void getAsShortText() {
    assertTrue("Sun".equalsIgnoreCase(instance.getAsShortText(1526173261000L)));
    assertTrue("Mon".equalsIgnoreCase(instance.getAsShortText(1526259661000L)));
    assertTrue("Sat".equalsIgnoreCase(instance.getAsShortText(1526086861000L)));
  }

  @Test
  public void getAsTextFieldValue() {
    assertTrue("Sunday".equalsIgnoreCase(instance.getAsText(1, Locale.getDefault())));
    assertTrue("Monday".equalsIgnoreCase(instance.getAsText(2, Locale.getDefault())));
    assertTrue("Saturday".equalsIgnoreCase(instance.getAsText(7, Locale.getDefault())));
  }

  @Test
  public void getAsShortTextFieldValue() {
    assertTrue("Sun".equalsIgnoreCase(instance.getAsShortText(1, Locale.getDefault())));
    assertTrue("Mon".equalsIgnoreCase(instance.getAsShortText(2, Locale.getDefault())));
    assertTrue("Sat".equalsIgnoreCase(instance.getAsShortText(7, Locale.getDefault())));
  }
}