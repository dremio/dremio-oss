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

package com.dremio.dac.datatype;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * A {@code (quantity, byte-unit)} pair, where {@code quantity} is non-negative and {@code
 * byte-unit} is one of the following:
 *
 * <ul>
 *   <li>bytes
 *   <li>kilobytes
 *   <li>megabytes
 *   <li>gigabytes
 * </ul>
 *
 * Technically, this class deals in powers of two, so it might be more accurate to describe the last
 * 3 types above as kibibytes, mebibytes, and gibibytes, respectively.
 *
 * @see ScaledBytesDeserializer
 */
public final class ScaledBytes {

  /**
   * User-facing gloss, intended to appear as a description where this type appears in our OpenAPI
   * spec
   */
  public static final String DESCRIPTION =
      "A string in the form \"`integer` `unit`\".  The following units are supported: B, KB, MB, and GB.";

  /** Maps each of a scale's names to its representative enum-value */
  private static final ImmutableMap<String, Scale> LOWERCASE_NAME_TO_SCALE;

  static {
    ImmutableMap.Builder<String, Scale> builder = ImmutableMap.builder();

    for (Scale u : Scale.values()) {
      for (String name : u.getSerializedNames()) {
        builder.put(name.toLowerCase(), u);
      }
    }

    LOWERCASE_NAME_TO_SCALE = builder.build();
  }

  private final long quantity;

  private final Scale scale;

  private ScaledBytes(long quantity, Scale scale) {
    this.quantity = quantity;
    this.scale = scale;
    // Only accept values that can be converted to any other unit without overflow.
    // Technically, this check is slightly more restrictive than the representable range,
    // but it still allows representing magnitudes up to approximately an exabyte.
    final long maxValue = 1L << (60 - scale.getPowerOfTwo());
    if (this.quantity < 0 || maxValue < this.quantity) {
      final String msg =
          String.format(
              "The quantity \"%s\" exceeds the supported range for unit \"%s\", which is [%s, %s], inclusive",
              this.quantity, this.scale, 0, maxValue);
      throw new IllegalArgumentException(msg);
    }
  }

  /** Map a scale's name to its enum-value, case-insensitive */
  public static Scale getScaleByName(String name) {
    return LOWERCASE_NAME_TO_SCALE.get(name.toLowerCase());
  }

  /** Static factory */
  public static ScaledBytes of(long quantity, Scale scale) {
    return new ScaledBytes(quantity, scale);
  }

  /**
   * Convert to another scale.
   *
   * <p>If the return value is a smaller quantity than {@link #getQuantity()}, implying moving from
   * a smaller scale to a larger scale, then the conversion is done by truncation, not by rounding.
   */
  public long getQuantity(Scale returnScale) {
    int shift = returnScale.getPowerOfTwo() - scale.getPowerOfTwo();

    return 0 < shift ? quantity >> shift : quantity << -shift;
  }

  public long getQuantity() {
    return quantity;
  }

  public Scale getScale() {
    return scale;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScaledBytes that = (ScaledBytes) o;
    return quantity == that.quantity && scale == that.scale;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(quantity, scale);
  }

  @Override
  public String toString() {
    return "ScaledBytes{" + quantity + " " + scale + '}';
  }

  /** A power-of-two unit of bytes */
  public enum Scale {
    BYTES(0, ImmutableList.of("B", "bytes")),
    KILOBYTES(10, ImmutableList.of("KB", "kilobytes", "kibibytes")),
    MEGABYTES(20, ImmutableList.of("MB", "megabytes", "mebibytes")),
    GIGABYTES(30, ImmutableList.of("GB", "gigabytes", "gibibytes"));

    private final int powerOfTwo;
    private final ImmutableList<String> serializedNames;

    Scale(int powerOfTwo, ImmutableList<String> serializedNames) {
      this.powerOfTwo = powerOfTwo;
      this.serializedNames = serializedNames;
    }

    /**
     * Gives this scale's unit multiplier as a power of two.
     *
     * <p>That is, given a scalar quantity representing some amount of this scale's units, the
     * equivalent number of bytes is obtained by multiplying that quantity by result of calling
     * {@link Math#pow(double, double)} with the parameters {@code 2} and this method's return
     * value.
     */
    public int getPowerOfTwo() {
      return powerOfTwo;
    }

    /** A list of one or more equivalent/synonymous names for this data-size unit. */
    public ImmutableList<String> getSerializedNames() {
      return serializedNames;
    }

    /**
     * This name is used for serialization.
     *
     * @see ScaledBytesSerializer
     */
    public String getPreferredName() {
      return getSerializedNames().get(0);
    }
  }
}
