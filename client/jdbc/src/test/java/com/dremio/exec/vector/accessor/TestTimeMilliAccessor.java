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
package com.dremio.exec.vector.accessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.time.LocalTime;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeMilliVector;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.vector.accessor.sql.TimePrintMillis;

public class TestTimeMilliAccessor {

  private static final int NON_NULL_VALUE_MS = 58447234;  // 16:14:07.234
  private static final LocalTime NON_NULL_VALUE = LocalTime.of(16, 14, 07, 234000000);
  private static final Calendar PST_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("PST"));
  private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  private SqlAccessor accessor;
  private TimeMilliVector valueVector;

  @Before
  public void setUp() {
    valueVector = new TimeMilliVector("t", new RootAllocator());
    valueVector.allocateNew(2);
    valueVector.set(0, NON_NULL_VALUE_MS);
    valueVector.setNull(1);

    accessor = new TimeMilliAccessor(valueVector, UTC_CALENDAR.getTimeZone());
  }

  @Test
  public void testIsNull() throws Exception {
    assertNotNull(accessor.getObject(0));
    assertNotNull(accessor.getTime(0, PST_CALENDAR));
    assertNull(accessor.getObject(1));
    assertNull(accessor.getTime(1, PST_CALENDAR));
  }

  @Test
  public void testGetObject() throws Exception {
    assertEquals(new TimePrintMillis(NON_NULL_VALUE), accessor.getObject(0));
  }

  @Test(expected=NullPointerException.class)
  public void testNullCalendar() throws InvalidAccessException {
    accessor.getTime(0, null);
  }

  @Test(expected=NullPointerException.class)
  public void testCreationWithNullTimeZone() {
    new TimeMilliAccessor(valueVector, null);
  }

  @Test
  public void testGetTime() throws Exception {
    assertEquals(new TimePrintMillis(NON_NULL_VALUE.plusHours(8)), accessor.getTime(0, PST_CALENDAR));
    assertEquals(new TimePrintMillis(NON_NULL_VALUE), accessor.getTime(0, UTC_CALENDAR));
  }
}
