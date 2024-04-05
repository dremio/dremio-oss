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

public class MaskTransformers {
  public static MaskTransformer newTransformer(
      String mode,
      int charCount,
      String maskedUpperChar,
      String maskedLowerChar,
      String maskedDigitChar,
      String maskedOtherChar,
      int maskedNumber,
      int maskedDayValue,
      int maskedMonthValue,
      int maskedYearValue) {
    switch (mode) {
      case "FULL":
        {
          MaskTransformer transformer = new MaskTransformer();
          transformer.init(
              maskedUpperChar,
              maskedLowerChar,
              maskedDigitChar,
              maskedOtherChar,
              maskedNumber,
              maskedDayValue,
              maskedMonthValue,
              maskedYearValue);
          return transformer;
        }
      case "FIRST_N":
        {
          MaskFirstNTransformer transformer = new MaskFirstNTransformer();
          transformer.init(
              charCount,
              maskedUpperChar,
              maskedLowerChar,
              maskedDigitChar,
              maskedOtherChar,
              maskedNumber,
              maskedDayValue,
              maskedMonthValue,
              maskedYearValue);
          return transformer;
        }
      case "LAST_N":
        {
          MaskLastNTransformer transformer = new MaskLastNTransformer();
          transformer.init(
              charCount,
              maskedUpperChar,
              maskedLowerChar,
              maskedDigitChar,
              maskedOtherChar,
              maskedNumber,
              maskedDayValue,
              maskedMonthValue,
              maskedYearValue);
          return transformer;
        }
      case "SHOW_FIRST_N":
        {
          MaskShowFirstNTransformer transformer = new MaskShowFirstNTransformer();
          transformer.init(
              charCount,
              maskedUpperChar,
              maskedLowerChar,
              maskedDigitChar,
              maskedOtherChar,
              maskedNumber,
              maskedDayValue,
              maskedMonthValue,
              maskedYearValue);
          return transformer;
        }
      case "SHOW_LAST_N":
        {
          MaskShowLastNTransformer transformer = new MaskShowLastNTransformer();
          transformer.init(
              charCount,
              maskedUpperChar,
              maskedLowerChar,
              maskedDigitChar,
              maskedOtherChar,
              maskedNumber,
              maskedDayValue,
              maskedMonthValue,
              maskedYearValue);
          return transformer;
        }
      default:
        throw new RuntimeException("Unknown mask mode: " + mode);
    }
  }
}
