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
/*
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
package io.airlift.tpch;

import static java.util.Locale.ENGLISH;

import java.math.BigDecimal;
import java.util.List;

import com.google.common.collect.ImmutableList;

public final class GenerateUtils {
  //
  // Partitioning Utils
  //

  public static long calculateRowCount(int scaleBase, double scaleFactor, int part, int partCount) {
    long totalRowCount = (long) (scaleBase * scaleFactor);
    long rowCount = totalRowCount / partCount;
    if (part == partCount) {
      // for the last part, add the remainder rows
      rowCount = rowCount + (totalRowCount % partCount);
    }
    return rowCount;
  }

  public static long calculateStartIndex(int scaleBase, double scaleFactor, int part, int partCount) {
    long totalRowCount = (long) (scaleBase * scaleFactor);

    long rowsPerPart = totalRowCount / partCount;
    return rowsPerPart * (part - 1);
  }

  //
  // Date Utils
  //

  /**
   * The value of 1970-01-01 in the date generator system
   */
  public static final int GENERATED_DATE_EPOCH_OFFSET = 83966;

  public static final int MIN_GENERATE_DATE = 92001;
  private static final int CURRENT_DATE = 95168;
  public static final int TOTAL_DATE_RANGE = 2557;

  private static final int[] MONTH_YEAR_DAY_START = new int[] { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334,
      365, };

  private static final List<String> DATE_STRING_INDEX = makeDateStringIndex();

  public static int toEpochDate(int generatedDate) {
    return generatedDate - GENERATED_DATE_EPOCH_OFFSET;
  }

  public static String formatDate(int epochDate) {
    return DATE_STRING_INDEX.get(epochDate - (MIN_GENERATE_DATE - GENERATED_DATE_EPOCH_OFFSET));
  }

  private static List<String> makeDateStringIndex() {
    ImmutableList.Builder<String> dates = ImmutableList.builder();
    for (int i = 0; i < TOTAL_DATE_RANGE; i++) {
      dates.add(makeDate(i + 1));
    }

    return dates.build();
  }

  private static String makeDate(int index) {
    int y = julian(index + MIN_GENERATE_DATE - 1) / 1000;
    int d = julian(index + MIN_GENERATE_DATE - 1) % 1000;

    int m = 0;
    while (d > MONTH_YEAR_DAY_START[m] + leapYearAdjustment(y, m)) {
      m++;
    }
    int dy = d - MONTH_YEAR_DAY_START[m - 1] - ((isLeapYear(y) && m > 2) ? 1 : 0);

    return String.format(ENGLISH, "19%02d-%02d-%02d", y, m, dy);
  }

  private static int leapYearAdjustment(int year, int month) {
    return ((isLeapYear(year) && (month) >= 2) ? 1 : 0);
  }

  public static boolean isInPast(int date) {
    return julian(date) <= CURRENT_DATE;
  }

  private static int julian(int date) {
    int offset = date - MIN_GENERATE_DATE;
    int result = MIN_GENERATE_DATE;

    while (true) {
      int year = result / 1000;
      int yearEnd = year * 1000 + 365 + (isLeapYear(year) ? 1 : 0);
      if (result + offset <= yearEnd) {
        break;
      }

      offset -= yearEnd - result + 1;
      result += 1000;
    }
    return (result + offset);
  }

  private static boolean isLeapYear(int year) {
    return year % 4 == 0 && year % 100 != 0;
  }

  //
  // Format utils
  //

  public static String formatMoney(long value) {
    // todo there must be a better way to do this
    return new BigDecimal(value).divide(new BigDecimal(100)).setScale(2).toString();
  }
}
