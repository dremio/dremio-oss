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

import org.joda.time.Chronology;
import org.joda.time.DateTimeZone;

/**
 * {@link Chronology} that wraps around any base chronology, except for {@link #dayOfWeek()} function, in which case,
 * the field is replaced with {@link DayOfWeekFromSundayDateTimeField}.
 */
public class DayOfWeekFromSundayChronology extends AssembledChronology {

  /**
   * Serialization version.
   */
  private static final long serialVersionUID = 9086460675149718250L;

  private static final DayOfWeekFromSundayChronology ISO_INSTANCE_UTC = withBase(ISOChronology.getInstanceUTC());

  /**
   * Gets an instance of DayOfWeekFromSundayChronology with the given chronology as the base.
   *
   * @param base base chronology
   * @return wrapped chronology
   */
  public static DayOfWeekFromSundayChronology withBase(Chronology base) {
    return new DayOfWeekFromSundayChronology(base);
  }

  /**
   * Gets an instance of the DayOfWeekFromSundayChronology wrapped around ISOChronology.
   * The time zone of the returned instance is UTC.
   *
   * @return a singleton UTC instance of the chronology
   */
  public static DayOfWeekFromSundayChronology getISOInstanceInUTC() {
    return ISO_INSTANCE_UTC;
  }

  private DayOfWeekFromSundayChronology(Chronology base) {
    super(base, null);
  }

  @Override
  public Chronology withUTC() {
    return new DayOfWeekFromSundayChronology(getBase().withUTC());
  }

  @Override
  public Chronology withZone(DateTimeZone zone) {
    return new DayOfWeekFromSundayChronology(getBase().withZone(zone));
  }

  @Override
  protected void assemble(Fields fields) {
    fields.dayOfWeek = new DayOfWeekFromSundayDateTimeField(getBase(), getBase().days());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DayOfWeekFromSundayChronology) {
      DayOfWeekFromSundayChronology chrono = (DayOfWeekFromSundayChronology) obj;
      return getZone().equals(chrono.getZone());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return "DayOfWeekFromSundayChronology".hashCode() * 11 + getZone().hashCode();
  }

  @Override
  public String toString() {
    return "[DayOfWeekFromSundayChronology " + getBase().toString() + "]";
  }

}
