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
package com.dremio.common.expression.fn;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JodaDateValidator {
  private static final Map<String, String> toJodaMappings = new HashMap<>();

  static {
    // Era
    toJodaMappings.put("ad", "G");
    toJodaMappings.put("bc", "G");
    // Meridian
    toJodaMappings.put("am", "a");
    toJodaMappings.put("pm", "a");
    // Century
    toJodaMappings.put("cc", "C");
    // Week of year
    toJodaMappings.put("ww", "ww");
    // Day of week
    toJodaMappings.put("d", "e");
    // Day name of week
    toJodaMappings.put("dy", "E");
    toJodaMappings.put("day", "EEEE");
    // Year
    toJodaMappings.put("yyyy", "yyyy");
    toJodaMappings.put("yy", "yy");
    // Day of year
    toJodaMappings.put("ddd", "DDD");
    // Month
    toJodaMappings.put("mm", "MM");
    toJodaMappings.put("mon", "MMM");
    toJodaMappings.put("month", "MMMM");
    // Day of month
    toJodaMappings.put("dd", "dd");
    // Hour of day
    toJodaMappings.put("hh", "hh");
    toJodaMappings.put("hh12", "hh");
    toJodaMappings.put("hh24", "HH");
    // Minutes
    toJodaMappings.put("mi", "mm");
    // Seconds
    toJodaMappings.put("ss", "ss");
    // Milliseconds
    toJodaMappings.put("f", "S");
    toJodaMappings.put("ff", "SS");
    toJodaMappings.put("fff", "SSS");
    // Timezone
    toJodaMappings.put("tzd", "z");
    toJodaMappings.put("tzo", "ZZ");
    toJodaMappings.put("tzh:tzm", "ZZ");
  }

  private static String[] getMatches(String pattern, boolean exactMatch) {
    // we are case insensitive
    String lowercasePattern = pattern.toLowerCase();
    List<String> matches = new ArrayList<>();

    for (String mapping : toJodaMappings.keySet()) {
      if (mapping.startsWith(lowercasePattern) && (!exactMatch || (mapping.length() == pattern.length()))) {
        matches.add(mapping);
      }
    }

    return matches.toArray(new String[0]);
  }

  private static String[] getPotentialMatches(String pattern) {
    return getMatches(pattern, false);
  }

  private static String[] getExactMatches(String pattern) {
    return getMatches(pattern, true);
  }

  /**
   * Validates and converts {@param format} to the Joda equivalent
   *
   * @param format date format
   * @return date format converted to joda format
   */
  public static String toJodaFormat(String format) throws ParseException {
    StringBuilder builder = new StringBuilder();
    StringBuilder buffer = new StringBuilder();
    Boolean isInQuotedText = false;

    for (int i = 0; i < format.length(); i++) {
      char currentChar = format.charAt(i);

      // logic before we append to the buffer
      if (currentChar == '"') {
        if (isInQuotedText) {
          // we are done with a quoted block
          isInQuotedText = false;

          // joda uses ' for quoting
          builder.append('\'');
          builder.append(buffer.toString());
          builder.append('\'');

          // clear buffer
          buffer.setLength(0);
          continue;
        } else {
          if (buffer.length() > 0) {
            // if we have content in the buffer and hit a quote, we have an parse error
            throw new ParseException("Invalid date format string '"+format+"' at position " + i, i);
          }

          isInQuotedText = true;
          continue;
        }
      }

      // handle special characters we want to simply pass through, but only if not in quoted and the buffer is empty
      if (!isInQuotedText && buffer.length() == 0 && ("*-/,.;: ".indexOf(currentChar) != -1)) {
        builder.append(currentChar);
        continue;
      }

      // append to the buffer
      buffer.append(currentChar);

      // nothing else to do if we are in quoted text
      if (isInQuotedText) {
        continue;
      }

      // check how many matches we have for our buffer
      String[] potentialList = getPotentialMatches(buffer.toString());
      int potentialCount = potentialList.length;

      if (potentialCount >= 1) {
        // one potential and the length match
        if (potentialCount == 1 && potentialList[0].length() == buffer.length()) {
          // we have a match!
          builder.append(toJodaMappings.get(potentialList[0]));
          buffer.setLength(0);
        } else {
          // Some patterns (like MON, MONTH) can cause ambiguity, such as "MON:".  "MON" will have two potential
          // matches, but "MON:" will match nothing, so we want to look ahead when we match "MON" and check if adding
          // the next char leads to 0 potentials.  If it does, we go ahead and treat the buffer as matched (if a
          // potential match exists that matches the buffer)
          if (format.length() - 1 > i) {
            String lookAheadPattern = (buffer.toString() + format.charAt(i+1)).toLowerCase();
            boolean lookAheadMatched = false;

            // we can query potentialList to see if it has anything that matches the lookahead pattern
            for (String potential : potentialList) {
              if (potential.startsWith(lookAheadPattern)) {
                lookAheadMatched = true;
                break;
              }
            }

            if (!lookAheadMatched) {
              // check if any of the potential matches are the same length as our buffer, we do not want to match "MO:"
              boolean matched = false;
              for (String potential : potentialList) {
                if (potential.length() == buffer.length()) {
                  matched = true;
                  break;
                }
              }

              if (matched) {
                builder.append(toJodaMappings.get(buffer.toString().toLowerCase()));
                buffer.setLength(0);
                continue;
              }
            }
          }
        }
      } else {
        // no potential matches found
        throw new ParseException("Invalid date format string '"+format+"' at position " + i, i);
      }
    }

    if (buffer.length() > 0) {
      // Some patterns (like MON, MONTH) can cause us to reach this point with a valid buffer value as MON has 2 valid
      // potential matches, so double check here
      String[] exactMatches = getExactMatches(buffer.toString());
      if (exactMatches.length == 1 && exactMatches[0].length() == buffer.length()) {
        builder.append(toJodaMappings.get(exactMatches[0]));
      } else {
        // we didn't successfully parse the entire string
        int pos = format.length() - buffer.length();
        throw new ParseException("Invalid date format string '"+format+"' at position " + pos, pos);
      }
    }

    return builder.toString();
  }
}
