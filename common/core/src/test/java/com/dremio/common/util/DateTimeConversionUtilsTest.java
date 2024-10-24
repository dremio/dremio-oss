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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.assertj.core.api.Assertions;
import org.junit.Test;

/** Tests for {@link DateTimeConversionUtils} */
public class DateTimeConversionUtilsTest {

  @Test
  public void testToOffsetDateTimeFromString() {
    OffsetDateTime dateTime = DateTimeConversionUtils.toOffsetDateTime("2018-08-24T12:15:22Z");
    assertThat(dateTime).isNotNull();
    assertThat(dateTime.getOffset()).isEqualTo(ZoneOffset.UTC);
    assertThat(dateTime.getMonth()).isEqualTo(Month.AUGUST);
    assertThat(dateTime.getDayOfMonth()).isEqualTo(24);
    assertThat(dateTime.getYear()).isEqualTo(2018);
    assertThat(dateTime.getHour()).isEqualTo(12);
    assertThat(dateTime.getMinute()).isEqualTo(15);
    assertThat(dateTime.getSecond()).isEqualTo(22);

    assertThat(DateTimeConversionUtils.toOffsetDateTime("")).isNull();
  }

  @Test
  public void testToProtoTimeFromOffsetDateTime() {
    OffsetDateTime dateTime = DateTimeConversionUtils.toOffsetDateTime("2022-04-26T09:10:26Z");
    com.google.protobuf.Timestamp protoTimestamp = DateTimeConversionUtils.toProtoTime(dateTime);

    Assertions.assertThat(protoTimestamp).isNotNull();
    Assertions.assertThat(protoTimestamp.getSeconds()).isEqualTo(dateTime.toEpochSecond());

    com.google.protobuf.Timestamp protoTimestampWithTimeZone =
        DateTimeConversionUtils.toProtoTime(
            DateTimeConversionUtils.toOffsetDateTime("2022-04-26T11:10:26+02:00"));

    Assertions.assertThat(protoTimestampWithTimeZone).isNotNull();
    Assertions.assertThat(protoTimestampWithTimeZone.getSeconds())
        .isEqualTo(dateTime.toEpochSecond());
  }
}
