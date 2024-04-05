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
package com.dremio.exec.expr.fn.impl;

public class MaskShowFirstNTransformer extends MaskTransformer {
  int charCount = 4;

  public void init(
      long charCount,
      String maskedUpperChar,
      String maskedLowerChar,
      String maskedDigitChar,
      String maskedOtherChar,
      int maskedNumber,
      int maskedDayValue,
      int maskedMonthValue,
      int maskedYearValue) {
    super.init(
        maskedUpperChar,
        maskedLowerChar,
        maskedDigitChar,
        maskedOtherChar,
        maskedNumber,
        maskedDayValue,
        maskedMonthValue,
        maskedYearValue);
    this.charCount = (int) charCount;

    if (charCount < 0) {
      this.charCount = 0;
    }
  }

  @Override
  public String transform(final CharSequence value) {
    final StringBuilder ret = new StringBuilder(value.length());

    for (int i = 0; i < charCount; i++) {
      ret.appendCodePoint(value.charAt(i));
    }

    for (int i = charCount; i < value.length(); i++) {
      ret.appendCodePoint(transformChar(value.charAt(i)));
    }

    return ret.toString();
  }

  @Override
  public Integer transform(final Integer value) {
    int val = value;

    if (value < 0) {
      val *= -1;
    }

    // count number of digits in the value
    int digitCount = 0;
    for (int v = val; v != 0; v /= 10) {
      digitCount++;
    }

    // number of digits to mask from the end
    final int maskCount = digitCount - charCount;

    if (maskCount <= 0) {
      return value;
    }

    int ret = 0;
    int pos = 1;
    for (int i = 0; val != 0; i++) {
      if (i < maskCount) { // mask this digit
        ret += maskedNumber * pos;
      } else { // retain this digit
        ret += ((val % 10) * pos);
      }

      val /= 10;
      pos *= 10;
    }

    if (value < 0) {
      ret *= -1;
    }

    return ret;
  }

  @Override
  public Long transform(final Long value) {
    long val = value;

    if (value < 0) {
      val *= -1;
    }

    // count number of digits in the value
    int digitCount = 0;
    for (long v = val; v != 0; v /= 10) {
      digitCount++;
    }

    // number of digits to mask from the end
    final int maskCount = digitCount - charCount;

    if (maskCount <= 0) {
      return value;
    }

    long ret = 0;
    long pos = 1;
    for (int i = 0; val != 0; i++) {
      if (i < maskCount) { // mask this digit
        ret += (maskedNumber * pos);
      } else { // retain this digit
        ret += ((val % 10) * pos);
      }

      val /= 10;
      pos *= 10;
    }

    if (value < 0) {
      ret *= -1;
    }

    return ret;
  }
}
