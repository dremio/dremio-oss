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

package com.dremio.common.util;

import static com.dremio.common.util.DateTimes.isoFormattedLocalDateToJavaTimeMillis;
import static com.dremio.common.util.DateTimes.isoFormattedLocalDateToMillis;
import static com.dremio.common.util.DateTimes.isoFormattedLocalTimestampToJavaTimeMillis;
import static com.dremio.common.util.DateTimes.isoFormattedLocalTimestampToMillis;
import static com.dremio.common.util.DateTimes.millisToIsoFormattedLocalDateString;
import static com.dremio.common.util.DateTimes.millisToIsoFormattedLocalTimestampString;
import static com.dremio.common.util.DateTimes.toMillis;
import static com.dremio.common.util.DateTimes.toMillisOfDay;

import com.dremio.test.DremioTest;
import java.sql.Timestamp;
import org.joda.time.LocalDateTime;
import org.junit.Assert;
import org.junit.Test;

public class TestDateTimes extends DremioTest {
  @Test
  public void testLocalDateTimeToMillisConversion() {
    // Arrange
    final String formattedDateTime = "2016-01-01 13:05:01.456";
    final org.joda.time.LocalDateTime localDateTime =
        new LocalDateTime(Timestamp.valueOf(formattedDateTime));

    // Act
    final long milliseconds = toMillis(localDateTime);

    // Assert
    Assert.assertEquals(1451653501456L, milliseconds);
  }

  @Test
  public void testDateTimeToMillisConversion() {
    // Arrange
    final String formattedDateTime = "2024-08-29T19:10:53+0800";
    final org.joda.time.DateTime dateTime = org.joda.time.DateTime.parse(formattedDateTime);

    // Act
    final long milliseconds = toMillis(dateTime);

    // Assert
    Assert.assertEquals(1724929853000L, milliseconds);
  }

  @Test
  public void testMillisOfDayWithoutTime() {
    // Arrange
    final String formattedDateTime = "2024-08-29";
    final org.joda.time.DateTime dateTime = org.joda.time.DateTime.parse(formattedDateTime);

    // Act
    final long millisOfDay = toMillisOfDay(dateTime);

    // Assert
    Assert.assertEquals(0, millisOfDay);
  }

  @Test
  public void testMillisOfDayWithTimezone() {
    // Arrange
    final String formattedDateTime = "2024-08-29T19:10:53+0800";
    final org.joda.time.DateTime dateTime = org.joda.time.DateTime.parse(formattedDateTime);

    // Act
    final long millisOfDay = toMillisOfDay(dateTime);

    // Assert
    Assert.assertEquals(40253000, millisOfDay);
  }

  @Test
  public void testMillisOfDayWithUtc() {
    // Arrange
    final String formattedDateTime = "2024-08-29T19:10:53+0000";
    final org.joda.time.DateTime dateTime = org.joda.time.DateTime.parse(formattedDateTime);

    // Act
    final long millisOfDay = toMillisOfDay(dateTime);

    // Assert
    Assert.assertEquals(69053000, millisOfDay);
  }

  @Test
  public void testIsoFormattedLocalDateToMillisConversion() {
    // Arrange
    final String isoFormattedLocalDate = "2011-12-03";

    // Act
    final long milliseconds = isoFormattedLocalDateToMillis(isoFormattedLocalDate);

    // Assert
    Assert.assertEquals(1322870400000L, milliseconds);
  }

  @Test
  public void testIsoFormattedLocalTimestampToMillisConversion() {
    // Arrange
    final String isoFormattedLocalTimestamp = "2011-12-03 23:15:46";

    // Act
    final long milliseconds = isoFormattedLocalTimestampToMillis(isoFormattedLocalTimestamp);

    // Assert
    Assert.assertEquals(1322954146000L, milliseconds);
  }

  @Test
  public void testIsoFormattedLocalDateToJavaTimeMillisConversion() {
    // Arrange
    final String jdbcEscapeString = "2011-12-03";

    // Act
    final long milliseconds = isoFormattedLocalDateToJavaTimeMillis(jdbcEscapeString);

    // Assert
    Assert.assertEquals(1322870400000L, milliseconds);
  }

  @Test
  public void testIsoFormattedLocalTimestampToJavaTimeMillisConversion() {
    // Arrange
    final String isoFormattedLocalTimestamp = "2011-12-03 23:15:46";

    // Act
    final long milliseconds =
        isoFormattedLocalTimestampToJavaTimeMillis(isoFormattedLocalTimestamp);

    // Assert
    Assert.assertEquals(1322954146000L, milliseconds);
  }

  @Test
  public void testMillisToIsoFormattedLocalDateFormatting() {
    // Arrange
    final long milliseconds = 1322870400000L;

    // Act
    final String formattedDate = millisToIsoFormattedLocalDateString(milliseconds);

    // Assert
    Assert.assertEquals("2011-12-03", formattedDate);
  }

  @Test
  public void testMillisToIsoFormattedLocalTimestampFormatting() {
    // Arrange
    final long milliseconds = 1322954146000L;

    // Act
    final String formattedDate = millisToIsoFormattedLocalTimestampString(milliseconds);

    // Assert
    Assert.assertEquals("2011-12-03 23:15:46", formattedDate);
  }
}
