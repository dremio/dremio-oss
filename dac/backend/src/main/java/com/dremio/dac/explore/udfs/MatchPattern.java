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
package com.dremio.dac.explore.udfs;

import static com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8;

import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.dac.explore.ReplaceRecommender;
import com.dremio.dac.explore.ReplaceRecommender.ReplaceMatcher;
import com.dremio.dac.explore.udfs.DremioUDFUtils.ExampleUDFOutputDerivation;
import com.dremio.dac.proto.model.dataset.ReplacePatternRule;
import com.dremio.dac.proto.model.dataset.ReplaceSelectionType;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.impl.StringFunctionHelpers;

/**
 * Function to check if a given input contains a pattern with given options.
 */
public class MatchPattern {
  public static final String GEN_EXAMPLE = "match_pattern_example";

  public static ReplaceSelectionType initSelectionType(NullableVarCharHolder value) {
    return ReplaceSelectionType.valueOf(StringFunctionHelpers.toStringFromUTF8(value.start, value.end, value.buffer));
  }

  public static ReplaceMatcher initMatcher(NullableVarCharHolder patternStr, ReplaceSelectionType selectionType, Boolean ignoreCase) {
    String pattern = toStringFromUTF8(patternStr.start, patternStr.end, patternStr.buffer);
    return ReplaceRecommender.getMatcher(
        new ReplacePatternRule(selectionType)
            .setIgnoreCase(ignoreCase)
            .setSelectionPattern(pattern));
  }

  public static Boolean match(ReplaceMatcher matcher,String value) {
    return matcher.matches(value) != null;
  }

  /**
   * Generates a position (start and offset) of substring that is matching the given pattern in input.
   * Used for generating card examples.
   *
   * 1. in : input column
   * 2. pattern : pattern
   * 3. index : which occurrence of pattern in input to select.
   * 4. index type : Type of index. One of [ INDEX, INDEX_BACKWARDS, CAPTURE_GROUP ]
   */
  @FunctionTemplate(name = GEN_EXAMPLE, scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL, derivation = ExampleUDFOutputDerivation.class)
  public static class MatchExample implements SimpleFunction {

    @Param private NullableVarCharHolder in;
    @Param(constant=true) private NullableVarCharHolder selectionTypeStr;
    @Param(constant=true) private NullableVarCharHolder patternStr;
    @Param(constant=true) private NullableBitHolder ignoreCaseInt;

    @Output private ComplexWriter out;

    @Workspace private com.dremio.dac.explore.ReplaceRecommender.ReplaceMatcher matcher;
    @Workspace private com.dremio.dac.proto.model.dataset.ReplaceSelectionType selectionType;

    @Override
    public void setup() {
      selectionType = com.dremio.dac.explore.udfs.MatchPattern.initSelectionType(selectionTypeStr);
      matcher = com.dremio.dac.explore.udfs.MatchPattern.initMatcher(patternStr, selectionType, ignoreCaseInt.value == 1);
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        out.rootAsList(); // calling this sets the ComplexWriter as a list type.
        return;
      }

      String inputString = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer);
      com.dremio.dac.explore.PatternMatchUtils.Match match = matcher.matches(inputString);
      if (match != null) {
        com.dremio.dac.explore.udfs.DremioUDFUtils.writeCardExample(out,
            new com.dremio.dac.proto.model.dataset.CardExamplePosition(match.start(), match.end() - match.start()));
      } else {
        out.rootAsList(); // calling this sets the ComplexWriter as a list type.
      }
    }
  }
}
