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

import org.joda.time.MutableDateTime;

public class MaskTransformer {
  public static final int MASKED_UPPERCASE           = 'X';
  public static final int MASKED_LOWERCASE           = 'x';
  public static final int MASKED_DIGIT               = 'n';
  public static final int MASKED_OTHER_CHAR          = -1;
  public static final int MASKED_NUMBER              = 1;
  public static final int MASKED_DAY_COMPONENT_VAL   = 1;
  public static final int MASKED_MONTH_COMPONENT_VAL = 0;
  public static final int MASKED_YEAR_COMPONENT_VAL  = 0;
  public static final int UNMASKED_VAL               = -1;

  int maskedUpperChar  = MASKED_UPPERCASE;
  int maskedLowerChar  = MASKED_LOWERCASE;
  int maskedDigitChar  = MASKED_DIGIT;
  int maskedOtherChar  = MASKED_OTHER_CHAR;
  long maskedNumber     = MASKED_NUMBER;
  long maskedDayValue   = MASKED_DAY_COMPONENT_VAL;
  long maskedMonthValue = MASKED_MONTH_COMPONENT_VAL;
  long maskedYearValue  = MASKED_YEAR_COMPONENT_VAL;

  public static void main(String[] arg) {
    String s = "Hello-12345";
    MaskTransformer transformer = new MaskTransformer();
    transformer.init("X", "x", "n", "-1", -1, -1, -1, -1);
    System.out.println(transformer.transform(s));
  }

  public void init(String maskedUpperChar, String maskedLowerChar, String maskedDigitChar,
    String maskedOtherChar, long maskedNumber, long maskedDayValue, long maskedMonthValue, long maskedYearValue) {

    this.maskedUpperChar  = getCharArg(maskedUpperChar, MASKED_UPPERCASE);
    this.maskedLowerChar  = getCharArg(maskedLowerChar, MASKED_LOWERCASE);
    this.maskedDigitChar  = getCharArg(maskedDigitChar, MASKED_DIGIT);
    this.maskedOtherChar  = getCharArg(maskedOtherChar, MASKED_OTHER_CHAR);
    this.maskedNumber     = maskedNumber;
    this.maskedDayValue   = maskedDayValue;
    this.maskedMonthValue = maskedMonthValue;
    this.maskedYearValue  = maskedYearValue;

    if(maskedNumber < 0 || maskedNumber > 9) {
      this.maskedNumber = MASKED_NUMBER;
    }

    if(maskedDayValue != UNMASKED_VAL) {
      if(maskedDayValue < 1 || maskedDayValue > 31) {
        this.maskedDayValue = MASKED_DAY_COMPONENT_VAL;
      }
    }

    if(maskedMonthValue != UNMASKED_VAL) {
      if(maskedMonthValue < 0 || maskedMonthValue > 11) {
        this.maskedMonthValue = MASKED_MONTH_COMPONENT_VAL;
      }
    }
  }

  int getCharArg(String arg, int defaultValue) {
    if (arg == null || arg.isEmpty()) {
      return defaultValue;
    }
    try {
      if (arg.length() > 1 && Integer.parseInt(arg) == UNMASKED_VAL) {
        return UNMASKED_VAL;
      }
    } catch (NumberFormatException e) {
      // ignore
    }
    return arg.charAt(0);
  }

  public String transform(final CharSequence val) {
    StringBuilder ret = new StringBuilder(val.length());

    for(int i = 0; i < val.length(); i++) {
      ret.appendCodePoint(transformChar(val.charAt(i)));
    }

    return ret.toString();
  }

  public Byte transform(final Byte value) {
    byte val = value;

    if(value < 0) {
      val *= -1;
    }

    byte ret = 0;
    int  pos = 1;
    while(val != 0) {
      ret += maskedNumber * pos;

      val /= 10;
      pos *= 10;
    }

    if(value < 0) {
      ret *= -1;
    }

    return ret;
  }

  public Short transform(final Short value) {
    short val = value;

    if(value < 0) {
      val *= -1;
    }

    short ret = 0;
    int   pos = 1;
    while(val != 0) {
      ret += maskedNumber * pos;

      val /= 10;
      pos *= 10;
    }

    if(value < 0) {
      ret *= -1;
    }

    return ret;
  }

  public Integer transform(final Integer value) {
    int val = value;

    if(value < 0) {
      val *= -1;
    }

    int ret = 0;
    int pos = 1;
    while(val != 0) {
      ret += maskedNumber * pos;

      val /= 10;
      pos *= 10;
    }

    if(value < 0) {
      ret *= -1;
    }

    return ret;
  }

  public Long transform(final Long value) {
    long val = value;

    if(value < 0) {
      val *= -1;
    }

    long ret = 0;
    long pos = 1;
    for(int i = 0; val != 0; i++) {
      ret += maskedNumber * pos;

      val /= 10;
      pos *= 10;
    }

    if(value < 0) {
      ret *= -1;
    }

    return ret;
  }

  public void transform(final MutableDateTime dateTime) {
    if (maskedYearValue != UNMASKED_VAL) {
      //dateTime.setYear((int) (maskedYearValue + 1900));
      dateTime.setYear((int) (maskedYearValue));
    }
    if (maskedMonthValue != UNMASKED_VAL) {
      dateTime.setMonthOfYear((int) (maskedMonthValue + 1));
    }
    if (maskedDayValue != UNMASKED_VAL) {
      dateTime.setDayOfMonth(((int) maskedDayValue));
    }
  }

  protected int transformChar(final int c) {
    switch(Character.getType(c)) {
    case Character.UPPERCASE_LETTER:
      if(maskedUpperChar != UNMASKED_VAL) {
        return maskedUpperChar;
      }
      break;

    case Character.LOWERCASE_LETTER:
      if(maskedLowerChar != UNMASKED_VAL) {
        return maskedLowerChar;
      }
      break;

    case Character.DECIMAL_DIGIT_NUMBER:
      if(maskedDigitChar != UNMASKED_VAL) {
        return maskedDigitChar;
      }
      break;

    default:
      if(maskedOtherChar != UNMASKED_VAL) {
        return maskedOtherChar;
      }
      break;
    }

    return c;
  }


}
