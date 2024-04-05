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
package com.dremio.plugins.elastic.planning.functions;

import com.dremio.plugins.elastic.ElasticsearchConstants;
import com.dremio.plugins.elastic.planning.rules.SchemaField.NullReference;
import com.dremio.plugins.elastic.planning.rules.SchemaField.ReferenceType;
import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FunctionRender {
  public static final String EMPTY = ".empty";
  public static final String CONTAINSKEY = ".containsKey";
  public static final String EQ_NULL = " == null";
  public static final String EQ_THEN = " ? %s : ";
  public static final String EQ_OR = " || ";
  public static final String EQ_NOT = "!";
  public static final String LEFT_PARENTHESIS = "(\"";
  public static final String RIGHT_PARENTHESIS = "\")";

  private final String script;
  private final Iterable<NullReference> nulls;

  public FunctionRender(String script, Iterable<NullReference> nulls) {
    this.script = script;
    this.nulls = nulls;
  }

  public String getScript() {
    return script;
  }

  public Iterable<NullReference> getNulls() {
    return nulls;
  }

  /**
   * Renders this as null guarded.
   *
   * @param nullReplacement is the value to return when a null value is caught
   * @param variationDetected true, if there is field variation for indexes in alias; false,
   *     otherwise.
   * @return a null guarded version of the provided script.
   */
  public String getNullGuardedScript(String nullReplacement, boolean variationDetected) {
    List<NullReference> inputListToCheck = ImmutableList.copyOf(nulls);
    Set<NullReference> checkRepeats = new HashSet<>();
    String toReturn = "";
    for (int i = 0; i < inputListToCheck.size(); i++) {
      NullReference toCheck = inputListToCheck.get(i);
      if (i == 0) {
        toReturn += "(";
      }

      if (!checkRepeats.contains(toCheck)) {
        if (checkRepeats.size() > 0) {
          toReturn += EQ_OR;
        }
        if (toCheck.getReferenceType() == ReferenceType.SOURCE) {
          toReturn += toCheck.getValue() + EQ_NULL;
        } else if (toCheck.getReferenceType() == ReferenceType.DOC) {
          if (variationDetected) {
            // Adds doc.containsKey check to avoid the no field found error.
            toReturn += EQ_NOT;
            toReturn += ElasticsearchConstants.DOC + CONTAINSKEY + LEFT_PARENTHESIS;
            toReturn += toCheck.getFullPath();
            toReturn += RIGHT_PARENTHESIS;
            toReturn += EQ_OR;
          }
          toReturn += toCheck.getValue() + EMPTY;
        } else {
          throw new UnsupportedOperationException(
              "Unknown reference type." + toCheck.getReferenceType());
        }
        checkRepeats.add(toCheck);
      }
    }

    if (checkRepeats.size() > 0) {
      toReturn += (")" + String.format(EQ_THEN, nullReplacement));
    }

    return toReturn + script;
  }

  public String getNullGuardedScript() {
    return this.getNullGuardedScript("null", false);
  }

  public String getNullGuardedScript(boolean variationDetected) {
    return this.getNullGuardedScript("null", variationDetected);
  }
}
