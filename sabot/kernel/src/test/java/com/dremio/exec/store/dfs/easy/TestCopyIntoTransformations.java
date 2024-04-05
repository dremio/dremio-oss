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
package com.dremio.exec.store.dfs.easy;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.store.easy.EasyFormatUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.joda.time.DateTime;
import org.junit.Test;

public class TestCopyIntoTransformations {
  private static final int MILLISECONDS_PER_DAY = 86400000;
  private final Field sampleStringField =
      new Field("sampleStringField", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
  private final ExtendedFormatOptions extendedFormatOptions =
      new ExtendedFormatOptions(false, false, null, null, null, null);

  @Test
  public void testEmptyAsNull() {
    final String emptyString = "";
    extendedFormatOptions.setEmptyAsNull(true);
    final String result =
        (String)
            EasyFormatUtils.getValue(sampleStringField, emptyString, this.extendedFormatOptions);
    assertThat(result).isNull();
    this.extendedFormatOptions.setEmptyAsNull(false);
  }

  @Test
  public void testNullIf() {
    List<String> nullIfExpressions = new ArrayList<>();
    nullIfExpressions.add("NA");
    nullIfExpressions.add("None");

    String testString1 = "NA";
    String testString2 = "Invalid string";
    String testString3 = "None";

    this.extendedFormatOptions.setNullIfExpressions(nullIfExpressions);

    String result1 =
        (String)
            EasyFormatUtils.getValue(sampleStringField, testString1, this.extendedFormatOptions);
    String result2 =
        (String)
            EasyFormatUtils.getValue(sampleStringField, testString2, this.extendedFormatOptions);
    String result3 =
        (String)
            EasyFormatUtils.getValue(sampleStringField, testString3, this.extendedFormatOptions);

    assertThat(result1).isNull();
    assertThat(result2).isEqualTo("Invalid string");
    assertThat(result3).isNull();

    this.extendedFormatOptions.setNullIfExpressions(new ArrayList<>());
  }

  @Test
  public void testDateTransform() {
    String dateTest1 = "2022-10-23";
    String dateTest2 = "23-10-2022";

    String dateFormatDefault = "YYYY-MM-DD";
    String dateFormatCustom = "DD-MM-YYYY";

    Field sampleDateField =
        new Field("sampleDateField", FieldType.nullable(CompleteType.DATE.getType()), null);

    // Check default date format handling.
    int dateTest1DaysFromEpoch =
        (int) EasyFormatUtils.getValue(sampleDateField, dateTest1, this.extendedFormatOptions);
    checkDatesAreEqual(dateTest1DaysFromEpoch, dateTest1, dateFormatDefault);

    // Check if date transformation works properly when we supply our own date format.
    this.extendedFormatOptions.setDateFormat("DD-MM-YYYY");
    int dateTest2DaysFromEpoch =
        (int) EasyFormatUtils.getValue(sampleDateField, dateTest2, this.extendedFormatOptions);
    checkDatesAreEqual(dateTest2DaysFromEpoch, dateTest2, dateFormatCustom);
    this.extendedFormatOptions.setDateFormat(null);
  }

  @Test
  public void testTimeTransform() {
    String timeTest1 = "02:40:23.005";
    String timeTest2 = "01:36 PM";
    String timeTestWithTimezone = "19:35:00 LKT";

    String defaultTimeFormat = "HH24:MI:SS.FFF";
    String customTimeFormat = "HH12:MI AMPM";
    String timeFormatWithTimezoneAbbr = "HH24:MI:SS TZD";

    Field sampleTimeField =
        new Field("sampleTimeField", FieldType.nullable(CompleteType.TIME.getType()), null);

    // Check handling for default time format
    Long timeTest1Result =
        (Long) (EasyFormatUtils.getValue(sampleTimeField, timeTest1, this.extendedFormatOptions));
    // The above method returns 1000 * (time in milliseconds) whereas we want the time only in
    // milliseconds in this case.
    timeTest1Result /= 1_000;
    checkResultCorrespondsToTime(timeTest1Result, timeTest1, defaultTimeFormat);

    // Check handling for time transformation when we give our own time format.
    this.extendedFormatOptions.setTimeFormat(customTimeFormat);
    Long timeTest2Result =
        (Long) EasyFormatUtils.getValue(sampleTimeField, timeTest2, this.extendedFormatOptions);
    timeTest2Result /= 1_000;
    checkResultCorrespondsToTime(timeTest2Result, timeTest2, customTimeFormat);
    this.extendedFormatOptions.setTimeFormat(null);
  }

  @Test
  public void testTimestampTransform() {
    String timestampTest1 = "2022-10-23 02:40:23.005";
    String timestampTest2 = "23-10-2022 01:36 PM";

    String defaultTimestampFormat = "YYYY-MM-DD HH24:MI:SS.FFF";
    String customTimestampFormat = "DD-MM-YYYY HH12:MI AMPM";

    Field sampleTimestampField =
        new Field(
            "sampleTimestampField", FieldType.nullable(CompleteType.TIMESTAMP.getType()), null);

    Long timestampTest1Result =
        (Long)
            EasyFormatUtils.getValue(
                sampleTimestampField, timestampTest1, this.extendedFormatOptions);
    timestampTest1Result /= 1_000;
    checkResultCorrespondsToTime(timestampTest1Result, timestampTest1, defaultTimestampFormat);

    this.extendedFormatOptions.setTimeStampFormat(customTimestampFormat);
    Long timestampTest2Result =
        (Long)
            EasyFormatUtils.getValue(
                sampleTimestampField, timestampTest2, this.extendedFormatOptions);
    timestampTest2Result /= 1_000;
    checkResultCorrespondsToTime(timestampTest2Result, timestampTest2, customTimestampFormat);
  }

  private void checkDatesAreEqual(int daysFromEpoch, String date, String dateFormat) {
    long millisFromEpoch = Long.valueOf(daysFromEpoch) * MILLISECONDS_PER_DAY;
    DateTime dateNew = new DateTime(millisFromEpoch);
    DateTime dateFromString =
        DateTime.parse(date, DateFunctionsUtils.getISOFormatterForFormatString(dateFormat));
    assertThat(dateNew).isEqualByComparingTo(dateFromString);
  }

  private void checkResultCorrespondsToTime(
      Long timeInMilli, String timeToCompareWith, String timeFormat) {
    DateTime timeFromResult = new DateTime(timeInMilli);
    DateTime timeActual =
        DateTime.parse(
            timeToCompareWith, DateFunctionsUtils.getISOFormatterForFormatString(timeFormat));
    assertThat(timeFromResult).isEqualByComparingTo(timeActual);
  }
}
