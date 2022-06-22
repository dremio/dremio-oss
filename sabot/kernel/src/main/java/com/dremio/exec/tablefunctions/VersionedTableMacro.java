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
package com.dremio.exec.tablefunctions;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;

import com.dremio.exec.catalog.TableVersionContext;

public abstract class VersionedTableMacro implements TableMacro {

  // This regex pattern matches a sequence of bare or double-quoted identifier segments, separated by a dot.  Within
  // a quoted identifier, any character is legal, except for \r, \n, and unescaped ".  Quotes can be escaped via
  // two double quotes in sequence - "".  This matches Calcite's identifier parsing.
  private static final Pattern SPLIT_PATTERN = Pattern.compile("\\G(\\\"(?:[^\\r\\n\"]|\\\"\\\")+\\\"|[^.\"]+)(?:\\.|$)");

  static List<String> splitTableIdentifier(String tableIdentifier) {
    List<String> result = new ArrayList<>();
    Matcher matcher = SPLIT_PATTERN.matcher(tableIdentifier);
    int endOfLastMatch = 0;
    while (matcher.find()) {
      String id = matcher.group(1);
      // strip quotes and escaped quotes from quoted ids
      if (id.charAt(0) == '\"') {
        id = id.substring(1, id.length() - 1).replace("\"\"", "\"");
      }
      result.add(id);
      endOfLastMatch = matcher.end();
    }

    if (endOfLastMatch != tableIdentifier.length()) {
      throw new IllegalArgumentException("Invalid table identifier");
    }

    return result;
  }

  @Override
  public TranslatableTable apply(final List<? extends Object> arguments) {
    // This should never be called
    throw new UnsupportedOperationException();
  }

  public abstract TranslatableTable apply(List<? extends Object> arguments, TableVersionContext tableVersionContext);
}
