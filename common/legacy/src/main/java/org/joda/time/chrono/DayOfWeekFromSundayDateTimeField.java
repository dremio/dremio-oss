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
package org.joda.time.chrono;

import java.util.Locale;
import org.joda.time.Chronology;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DurationField;
import org.joda.time.field.PreciseDurationDateTimeField;

/**
 * Variant of {@link GJDayOfWeekDateTimeField} that counts Sunday as 1, Monday as 2, ..., and
 * Saturday as 7.
 */
final class DayOfWeekFromSundayDateTimeField extends PreciseDurationDateTimeField {

  /** Serialization version */
  @SuppressWarnings("unused")
  private static final long serialVersionUID = 8020294841735395909L;

  private final Chronology chronology;

  /** Restricted constructor. */
  DayOfWeekFromSundayDateTimeField(Chronology chronology, DurationField days) {
    super(DateTimeFieldType.dayOfWeek(), days);
    this.chronology = chronology;
  }

  private static int map(int i) {
    // 1 -> 2, 2 -> 3, ..., 7 -> 1
    return (i % 7) + 1;
  }

  private static int reverse(int i) {
    // 1 -> 7, 2 -> 1, ..., 7 -> 6
    final int tmp = (i + 6) % 7;
    return tmp == 0 ? 7 : tmp;
  }

  /**
   * Get the value of the specified time instant.
   *
   * @param instant the time instant in millis to query
   * @return the day of the week extracted from the input
   */
  @Override
  public int get(long instant) {
    return map(chronology.dayOfWeek().get(instant));
  }

  /**
   * Get the textual value of the specified time instant.
   *
   * @param fieldValue the field value to query
   * @param locale the locale to use
   * @return the day of the week, such as 'Monday'
   */
  @Override
  public String getAsText(int fieldValue, Locale locale) {
    return chronology.dayOfWeek().getAsText(reverse(fieldValue), locale);
  }

  /**
   * Get the abbreviated textual value of the specified time instant.
   *
   * @param fieldValue the field value to query
   * @param locale the locale to use
   * @return the day of the week, such as 'Mon'
   */
  @Override
  public String getAsShortText(int fieldValue, Locale locale) {
    return chronology.dayOfWeek().getAsShortText(reverse(fieldValue), locale);
  }

  /**
   * Convert the specified text and locale into a value.
   *
   * @param text the text to convert
   * @param locale the locale to convert using
   * @return the value extracted from the text
   * @throws IllegalArgumentException if the text is invalid
   */
  @Override
  protected int convertText(String text, Locale locale) {
    return map(GJLocaleSymbols.forLocale(locale).dayOfWeekTextToValue(text));
  }

  @Override
  public DurationField getRangeDurationField() {
    return chronology.weeks();
  }

  /**
   * Get the minimum value that this field can have.
   *
   * @return the field's minimum value
   */
  @Override
  public int getMinimumValue() {
    // returns 1 which corresponds to SUNDAY for this impl
    return DateTimeConstants.MONDAY;
  }

  /**
   * Get the maximum value that this field can have.
   *
   * @return the field's maximum value
   */
  @Override
  public int getMaximumValue() {
    // returns 7 which corresponds to SATURDAY for this impl
    return DateTimeConstants.SUNDAY;
  }

  /**
   * Get the maximum length of the text returned by this field.
   *
   * @param locale the locale to use
   * @return the maximum textual length
   */
  @Override
  public int getMaximumTextLength(Locale locale) {
    return chronology.dayOfWeek().getMaximumTextLength(locale);
  }

  /**
   * Get the maximum length of the abbreviated text returned by this field.
   *
   * @param locale the locale to use
   * @return the maximum abbreviated textual length
   */
  @Override
  public int getMaximumShortTextLength(Locale locale) {
    return chronology.dayOfWeek().getMaximumShortTextLength(locale);
  }

  /** Serialization singleton */
  private Object readResolve() {
    return chronology.dayOfWeek();
  }

  @Override
  public String toString() {
    return "DayOfWeekFromSundayDateTimeField[" + getName() + ']';
  }
}
