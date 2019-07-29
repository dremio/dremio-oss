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
package com.dremio.dac.explore;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dremio.dac.proto.model.dataset.IndexType;

/**
 * Contains utility classes and methods for pattern matching purposes.
 */
public class PatternMatchUtils {

  public static List<Match> findMatches(Matcher matcher, String matchee, IndexType type) {
    matcher.reset(matchee);
    List<Match> matches = new ArrayList<>();
    switch (type) {
      case INDEX:
      case INDEX_BACKWARDS:
        while (matcher.find()) {
          matches.add(match(matcher));
        }
        break;
      case CAPTURE_GROUP:
        if (matcher.matches()) {
          for (int i = 0; i < matcher.groupCount(); i++) {
            matches.add(groupMatch(matcher, i));
          }
        }
        break;
      default:
        throw new UnsupportedOperationException(type.name());
    }
    return matches;
  }

  public static Match match(Matcher matcher) {
    return new Match(matcher.start(), matcher.end());
  }

  /**
   *
   * @param matcher
   * @param i group index starting at 0
   * @return
   */
  public static Match groupMatch(Matcher matcher, int i) {
    return new Match(matcher.start(i + 1), matcher.end(i + 1));
  }

  enum CharType {
    DIGIT("\\d+"), // \d  A digit: [0-9]
    WORD("\\w+"); // \w  A word character: [a-zA-Z_0-9]

    private final Pattern p;

    CharType(String regex) {
      p = Pattern.compile(regex);
    }

    boolean isTypeOf(String s) {
      return p.matcher(s).matches();
    }

    boolean isTypeOf(char c) {
      return isTypeOf(String.valueOf(c));
    }

    String pattern() {
      return p.pattern();
    }

    Matcher matcher(String s) {
      return p.matcher(s);
    }
  }

  /**
   * An extract match
   */
  public static class Match {
    private final int start;
    private final int end;
    public Match(int start, int end) {
      super();
      this.start = start;
      this.end = end;
    }
    public int start() {
      return start;
    }
    public int end() {
      return end;
    }
    public int length() {
      return end - start;
    }
    @Override
    public String toString() {
      return format("Match(%s, %s)", start, end);
    }
  }
}
