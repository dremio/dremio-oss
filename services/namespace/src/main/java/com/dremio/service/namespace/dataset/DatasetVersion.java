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
package com.dremio.service.namespace.dataset;

import java.security.SecureRandom;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * The version of a dataset
 *
 * Versions are not strictly ordered.
 * They are meant to simplify range queries.
 * All versions created after a version should be greater than getLowerBound()
 * All versions created before a version should be lower than getUpperBound()
 */
public class DatasetVersion implements Comparable<DatasetVersion> {

  public static final DatasetVersion MIN_VERSION = new DatasetVersion(0);
  public static final DatasetVersion MAX_VERSION = new DatasetVersion(Long.MAX_VALUE);
  public static final DatasetVersion NONE = new DatasetVersion(-1, true);

  private final long value;

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

  public DatasetVersion(long t, long r) {
    this(versionValue(t, r));
  }

  public DatasetVersion(long value) {
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
      for (int i = string.length(); i < 16; ++i){
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
    return (value >>> bitsForRand) + origin;
  }

  public DatasetVersion getLowerBound() {
    long t = this.getTimestamp();
    if (t == minTimestamp) {
      return MIN_VERSION;
    }
    return new DatasetVersion(t - 1, 0L);
  }

  public DatasetVersion getUpperBound() {
    long t = this.getTimestamp();
    if (t == maxTimestamp) {
      return MAX_VERSION;
    }
    return new DatasetVersion(t + 1, 0L);
  }

  private static final long origin;
  private static final int bitsForRand;
  private static final long mask;
  static {
    try {
      origin = new SimpleDateFormat("yyyy-MM-dd").parse("2015-08-17").getTime();
      long end = new SimpleDateFormat("yyyy-MM-dd").parse("2115-08-17").getTime();
      bitsForRand = Long.numberOfLeadingZeros(end - origin) - 1; // -1 to make sure it stays positive
      mask = (1 << bitsForRand) - 1;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }
  private static final long minTimestamp = MIN_VERSION.getTimestamp();
  private static final long maxTimestamp = MAX_VERSION.getTimestamp();

  public static DatasetVersion newVersion() {
    long t = System.currentTimeMillis();
    long r = Holder.numberGenerator.nextLong();
    return new DatasetVersion(t, r);
  }

  private static long versionValue(long timestamp, long r) {
    if (timestamp > maxTimestamp) {
      throw new IllegalArgumentException("timestamp is after max timestamp: " + timestamp);
    }
    long t = timestamp - origin;
    if (t < 0) {
      throw new IllegalArgumentException("timestamp should be after origin: " + timestamp);
    }
    if (Long.numberOfLeadingZeros(t) < bitsForRand) {
      throw new IllegalArgumentException(String.format("timestamp should fit in the time range: %s %s %s", timestamp, Long.numberOfLeadingZeros(t), bitsForRand));
    }
    return (t << bitsForRand) | (r & mask);
  }

  /*
   * The random number generator used by this class to create random
   * based versions. In a holder class to defer initialization until needed.
   */
  private static class Holder {
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
    DatasetVersion other = (DatasetVersion)obj;
    return value == other.value;
  }

  @Override
  public int compareTo(DatasetVersion o) {
    return Long.compare(value, o.value);
  }
}
