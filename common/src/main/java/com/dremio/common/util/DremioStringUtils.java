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
package com.dremio.common.util;

import org.apache.arrow.vector.util.DateUtility;
import org.apache.commons.lang3.StringEscapeUtils;
import org.joda.time.Period;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;

public class DremioStringUtils {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioStringUtils.class);

  /**
   * Converts the long number into more human readable string.
   */
  public static String readable(long bytes) {
    int unit = 1024;
    long absBytes = Math.abs(bytes);
    if (absBytes < unit) {
      return bytes + " B";
    }
    int exp = (int) (Math.log(absBytes) / Math.log(unit));
    char pre = ("KMGTPE").charAt(exp-1);
    return String.format("%s%.1f %ciB", (bytes == absBytes ? "" : "-"), absBytes / Math.pow(unit, exp), pre);
  }


  /**
   * Unescapes any Java literals found in the {@code String}.
   * For example, it will turn a sequence of {@code '\'} and
   * {@code 'n'} into a newline character, unless the {@code '\'}
   * is preceded by another {@code '\'}.
   *
   * @param input  the {@code String} to unescape, may be null
   * @return a new unescaped {@code String}, {@code null} if null string input
   */
  public static final String unescapeJava(String input) {
    return StringEscapeUtils.unescapeJava(input);
  }

  /**
   * Escapes the characters in a {@code String} according to Java string literal
   * rules.
   *
   * Deals correctly with quotes and control-chars (tab, backslash, cr, ff,
   * etc.) so, for example, a tab becomes the characters {@code '\\'} and
   * {@code 't'}.
   *
   * Example:
   * <pre>
   * input string: He didn't say, "Stop!"
   * output string: He didn't say, \"Stop!\"
   * </pre>
   *
   * @param input  String to escape values in, may be null
   * @return String with escaped values, {@code null} if null string input
   */
  public static final String escapeJava(String input) {
    return StringEscapeUtils.escapeJava(input);
  }

  public static final String escapeNewLines(String input) {
    if (input == null) {
      return null;
    }
    StringBuilder result = new StringBuilder();
    boolean sawNewline = false;
    for (int i = 0; i < input.length(); i++) {
      char curChar = input.charAt(i);
      if (curChar == '\r' || curChar == '\n') {
        if (sawNewline) {
          continue;
        }
        sawNewline = true;
        result.append("\\n");
      } else {
        sawNewline = false;
        result.append(curChar);
      }
    }
    return result.toString();
  }

  /**
   * Copied form commons-lang 2.x code as common-lang 3.x has this API removed.
   * (http://commons.apache.org/proper/commons-lang/article3_0.html#StringEscapeUtils.escapeSql)
   * @param str
   * @return
   */
  public static String escapeSql(String str) {
    return (str == null) ? null : str.replace("'", "''");
  }

  /**
   * Return a printable representation of a byte buffer, escaping the non-printable
   * bytes as '\\xNN' where NN is the hexadecimal representation of such bytes.
   *
   * This function does not modify  the {@code readerIndex} and {@code writerIndex}
   * of the byte buffer.
   */
  public static String toBinaryString(ByteBuf buf, int strStart, int strEnd) {
    StringBuilder result = new StringBuilder();
    for (int i = strStart; i < strEnd ; ++i) {
      appendByte(result, buf.getByte(i));
    }
    return result.toString();
  }

  /**
   * Return a printable representation of a byte array, escaping the non-printable
   * bytes as '\\xNN' where NN is the hexadecimal representation of such bytes.
   */
  public static String toBinaryString(byte[] buf) {
    return toBinaryString(buf, 0, buf.length);
  }

  /**
   * Return a printable representation of a byte array, escaping the non-printable
   * bytes as '\\xNN' where NN is the hexadecimal representation of such bytes.
   */
  public static String toBinaryString(byte[] buf, int strStart, int strEnd) {
    StringBuilder result = new StringBuilder();
    for (int i = strStart; i < strEnd ; ++i) {
      appendByte(result, buf[i]);
    }
    return result.toString();
  }

  private static void appendByte(StringBuilder result, byte b) {
    int ch = b & 0xFF;
    if (   (ch >= '0' && ch <= '9')
        || (ch >= 'A' && ch <= 'Z')
        || (ch >= 'a' && ch <= 'z')
        || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0 ) {
        result.append((char)ch);
    } else {
      result.append(String.format("\\x%02X", ch));
    }
  }

  private static void appendByteNoFormat(StringBuilder result, byte b) {
    int ch = b & 0xFF;
    result.append(String.format("%02X", ch));
  }

  public static String toBinaryStringNoFormat(ArrowBuf buf, int strStart, int strEnd) {
    StringBuilder result = new StringBuilder();
    for (int i = strStart; i < strEnd ; ++i) {
      appendByteNoFormat(result, buf.getByte(i));
    }
    return result.toString();
  }

  /**
   * Parses a hex encoded binary string and write to an output buffer.
   *
   * This function does not modify  the {@code readerIndex} and {@code writerIndex}
   * of the byte buffer.
   *
   * @return Index in the byte buffer just after the last written byte.
   */
  public static int parseBinaryString(ByteBuf str, int strStart, int strEnd, ByteBuf out) {
    int dstEnd = 0;
    for (int i = strStart; i < strEnd; i++) {
      byte b = str.getByte(i);
      if (b == '\\'
          && strEnd > i+3
          && (str.getByte(i+1) == 'x' || str.getByte(i+1) == 'X')) {
        // ok, take next 2 hex digits.
        byte hd1 = str.getByte(i+2);
        byte hd2 = str.getByte(i+3);
        if (isHexDigit(hd1) && isHexDigit(hd2)) { // [a-fA-F0-9]
          // turn hex ASCII digit -> number
          b = (byte) ((toBinaryFromHex(hd1) << 4) + toBinaryFromHex(hd2));
          i += 3; // skip 3
        }
      }
      out.setByte(dstEnd++, b);
    }
    return dstEnd;
  }

  /**
   * Formats a period similar to Oracle INTERVAL YEAR TO MONTH data type<br>
   * For example, the string "+21-02" defines an interval of 21 years and 2 months
   */
  public static String formatIntervalYear(final Period p) {
    long months = p.getYears() * (long) DateUtility.yearsToMonths + p.getMonths();
    boolean neg = false;
    if (months < 0) {
      months = -months;
      neg = true;
    }
    final int years = (int) (months / DateUtility.yearsToMonths);
    months = months % DateUtility.yearsToMonths;

    return String.format("%c%03d-%02d", neg ? '-':'+', years, months);
  }

  /**
   * Formats a period similar to Oracle INTERVAL DAY TO SECOND data type.<br>
   * For example, the string "-001 18:25:16.766" defines an interval of - 1 day 18 hours 25 minutes 16 seconds and 766 milliseconds
   */
  public static String formatIntervalDay(final Period p) {
    long millis = p.getDays() * (long) DateUtility.daysToStandardMillis + JodaDateUtility.millisFromPeriod(p);

    boolean neg = false;
    if (millis < 0) {
      millis = -millis;
      neg = true;
    }

    final int days = (int) (millis / DateUtility.daysToStandardMillis);
    millis = millis % DateUtility.daysToStandardMillis;

    final int hours  = (int) (millis / DateUtility.hoursToMillis);
    millis     = millis % DateUtility.hoursToMillis;

    final int minutes = (int) (millis / DateUtility.minutesToMillis);
    millis      = millis % DateUtility.minutesToMillis;

    final int seconds = (int) (millis / DateUtility.secondsToMillis);
    millis      = millis % DateUtility.secondsToMillis;

    return String.format("%c%03d %02d:%02d:%02d.%03d", neg ? '-':'+', days, hours, minutes, seconds, millis);
  }

  /**
   * Takes a ASCII digit in the range A-F0-9 and returns
   * the corresponding integer/ordinal value.
   * @param ch  The hex digit.
   * @return The converted hex value as a byte.
   */
  public static byte toBinaryFromHex(byte ch) {
    if ( ch >= 'A' && ch <= 'F' ) {
      return (byte) ((byte)10 + (byte) (ch - 'A'));
    } else if ( ch >= 'a' && ch <= 'f' ) {
      return (byte) ((byte)10 + (byte) (ch - 'a'));
    }
    return (byte) (ch - '0');
  }

  public static boolean isHexDigit(byte c) {
    return (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') || (c >= '0' && c <= '9');
  }

}
