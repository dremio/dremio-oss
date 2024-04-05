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
package com.dremio.service.namespace.dataset;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import java.security.SecureRandom;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * The version of a dataset
 *
 * <p>Versions are not strictly ordered. They are meant to simplify range queries. All versions
 * created after a version should be greater than getLowerBound() All versions created before a
 * version should be lower than getUpperBound()
 */
public class DatasetVersion implements Comparable<DatasetVersion> {
  public static final DatasetVersion MIN_VERSION = DatasetVersion.fromExistingVersion(0L);
  public static final DatasetVersion MAX_VERSION =
      DatasetVersion.fromExistingVersion(Long.MAX_VALUE);
  public static final DatasetVersion NONE = new DatasetVersion(-1, true);

  private static final long ORIGIN;
  private static final int BITS_FOR_RAND;
  private static final long MASK;

  static {
    try {
      ORIGIN = new SimpleDateFormat("yyyy-MM-dd").parse("2015-08-17").getTime();
      long end = new SimpleDateFormat("yyyy-MM-dd").parse("2115-08-17").getTime();
      BITS_FOR_RAND =
          Long.numberOfLeadingZeros(end - ORIGIN) - 1; // -1 to make sure it stays positive
      MASK = (1 << BITS_FOR_RAND) - 1;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  private static final long MIN_TIMESTAMP = MIN_VERSION.getTimestamp();
  private static final long MAX_TIMESTAMP = MAX_VERSION.getTimestamp();

  private final long value;

  /**
   * Creates a new unique dataset version.
   *
   * @return a unique dataset version
   */
  public static DatasetVersion newVersion() {
    long t = System.currentTimeMillis();
    long r = Holder.numberGenerator.nextLong();
    return new DatasetVersion(t, r);
  }

  /**
   * Creates a dataset version for a pre-existing version.
   *
   * @param version the pre-existing version
   * @return a dataset version representing the passed in version
   */
  public static DatasetVersion fromExistingVersion(Long version) {
    Preconditions.checkNotNull(version, "No dataset version provided");
    return new DatasetVersion(version);
  }

  @VisibleForTesting
  public static DatasetVersion forTesting(long t, long r) {
    return new DatasetVersion(t, r);
  }

  @VisibleForTesting
  public static DatasetVersion forTesting(Ticker ticker) {
    long t = ticker.read();
    long r = Holder.numberGenerator.nextLong();
    return new DatasetVersion(t, r);
  }

  /**
   * Creates a dataset version for a pre-existing version.
   *
   * @param version a pre-existing dataset version
   */
  @JsonCreator
  public DatasetVersion(String version) {
    this(parseLong(version));
  }

  private static long parseLong(String version) {
    try {
      return Long.parseLong(version, 16);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("invalid version: " + version);
    }
  }

  private DatasetVersion(long t, long r) {
    this(versionValue(t, r));
  }

  private DatasetVersion(long value) {
    this(value, false);
  }

  private DatasetVersion(long value, boolean isNone) {
    // super cheesy way of creating NONE instance
    if (isNone) {
      this.value = -1;
      return;
    }

    if (value < 0) {
      throw new IllegalArgumentException("versions are positive: " + value);
    }
    this.value = value;
  }

  @JsonValue
  public String getVersion() {
    String string = Long.toString(this.value, 16);
    if (string.length() < 16) {
      StringBuilder sb = new StringBuilder();
      for (int i = string.length(); i < 16; ++i) {
        sb.append('0');
      }
      sb.append(string);
      string = sb.toString();
    }
    return string;
  }

  @Override
  public String toString() {
    return getVersion();
  }

  public long getValue() {
    return value;
  }

  public long getTimestamp() {
    return (value >>> BITS_FOR_RAND) + ORIGIN;
  }

  public DatasetVersion getLowerBound() {
    long t = this.getTimestamp();
    if (t == MIN_TIMESTAMP) {
      return MIN_VERSION;
    }
    return new DatasetVersion(t - 1, 0L);
  }

  public DatasetVersion getUpperBound() {
    long t = this.getTimestamp();
    if (t == MAX_TIMESTAMP) {
      return MAX_VERSION;
    }
    return new DatasetVersion(t + 1, 0L);
  }

  private static long versionValue(long timestamp, long r) {
    if (timestamp > MAX_TIMESTAMP) {
      throw new IllegalArgumentException("timestamp is after max timestamp: " + timestamp);
    }
    long t = timestamp - ORIGIN;
    if (t < 0) {
      throw new IllegalArgumentException("timestamp should be after origin: " + timestamp);
    }
    if (Long.numberOfLeadingZeros(t) < BITS_FOR_RAND) {
      throw new IllegalArgumentException(
          String.format(
              "timestamp should fit in the time range: %s %s %s",
              timestamp, Long.numberOfLeadingZeros(t), BITS_FOR_RAND));
    }
    return (t << BITS_FOR_RAND) | (r & MASK);
  }

  /*
   * The random number generator used by this class to create random
   * based versions. In a holder class to defer initialization until needed.
   */
  private static final class Holder {
    static final SecureRandom numberGenerator = new SecureRandom();
  }

  @Override
  public int hashCode() {
    return (int) value;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DatasetVersion other = (DatasetVersion) obj;
    return value == other.value;
  }

  @Override
  public int compareTo(DatasetVersion o) {
    return Long.compare(value, o.value);
  }
}
