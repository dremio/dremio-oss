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
package com.dremio.dac.explore.udfs;

import static com.dremio.dac.proto.model.dataset.IndexType.INDEX_BACKWARDS;
import static com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8;
import static com.dremio.exec.expr.fn.impl.StringFunctionUtil.compilePattern;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import javax.inject.Inject;

import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.dac.explore.PatternMatchUtils;
import com.dremio.dac.explore.PatternMatchUtils.Match;
import com.dremio.dac.explore.udfs.DremioUDFUtils.ExampleUDFOutputDerivation;
import com.dremio.dac.proto.model.dataset.IndexType;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;

/**
 * Functions to extract a regular expression pattern
 */
public class ExtractPattern {
  public static final String APPLY = "extract_pattern";
  public static final String GEN_EXAMPLE = "extract_pattern_example";

  public static IndexType initType(NullableVarCharHolder indexType, FunctionErrorContext errCtx) {
    final String indexName = toStringFromUTF8(indexType.start, indexType.end, indexType.buffer);
    try {
      return IndexType.valueOf(indexName);
    } catch (IllegalArgumentException e) {
      throw errCtx.error()
        .message("Illegal indexType '%s'", indexName)
        .build();
    }
  }

  public static Matcher initMatcher(NullableVarCharHolder pattern, FunctionErrorContext errCtx) {
    return compilePattern(
        toStringFromUTF8(pattern.start, pattern.end, pattern.buffer), errCtx)
        .matcher("");
  }

  public static Match extract(Matcher matcher, String matchee, IndexType type, int index) {
    matcher.reset(matchee);
    switch (type) {
      case INDEX:
      case INDEX_BACKWARDS:
        List<Match> matches = new ArrayList<Match>();
        while (matcher.find()) {
          matches.add(PatternMatchUtils.match(matcher));
        }
        int length = matches.size();
        int i = (type == INDEX_BACKWARDS) ? length - 1 - index : index;
        if (i < matches.size() && i >= 0) {
          return matches.get(i);
        }
        break;
      case CAPTURE_GROUP:
        if (matcher.matches()) {
          if (index >= 0 && index < matcher.groupCount()) {
            return PatternMatchUtils.groupMatch(matcher, index);
          }
        }
        break;
      default:
        throw new UnsupportedOperationException(type.name());
    }
    return null;
  }

  /**
   * Extract the substring of given pattern from input.
   *
   * Parameters:
   * 1. in : input column
   * 2. pattern : pattern
   * 3. index : which occurrence of pattern in input to select.
   * 4. index type : Type of index. One of [ INDEX, INDEX_BACKWARDS, CAPTURE_GROUP ]
   */
  @FunctionTemplate(name = APPLY, scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ExtractPatternFunction implements SimpleFunction {

    @Param private NullableVarCharHolder in;
    @Param(constant=true) private NullableVarCharHolder pattern;
    @Param(constant=true) private NullableIntHolder index;
    @Param(constant=true) private NullableVarCharHolder indexType;
    @Output private NullableVarCharHolder out;
    @Workspace private java.util.regex.Matcher matcher;
    @Workspace private com.dremio.dac.proto.model.dataset.IndexType t;
    @Inject private FunctionErrorContext errCtx;

    @Override
    public void setup() {
      matcher = com.dremio.dac.explore.udfs.ExtractPattern.initMatcher(pattern, errCtx);
      t = com.dremio.dac.explore.udfs.ExtractPattern.initType(indexType, errCtx);
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        out.isSet = 0;
        return;
      }

      final String i = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer);
      com.dremio.dac.explore.PatternMatchUtils.Match result = com.dremio.dac.explore.udfs.ExtractPattern.extract(matcher, i, t, index.value);
      if (result == null) {
        out.isSet = 0;
      } else {
        out.buffer = in.buffer;
        out.start = com.dremio.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(io.netty.buffer.NettyArrowBuf.unwrapBuffer(in.buffer),
          in.start, in.end, result.start(), errCtx);
        out.end = com.dremio.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(io.netty.buffer.NettyArrowBuf.unwrapBuffer(in.buffer),
          in.start, in.end, result.end(), errCtx);
        out.isSet = 1;
      }
    }
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
  public static class ExtractPatternExample implements SimpleFunction {

    @Param private NullableVarCharHolder in;
    @Param(constant=true) private NullableVarCharHolder pattern;
    @Param(constant=true) private NullableIntHolder index;
    @Param(constant=true) private NullableVarCharHolder indexType;

    @Output private ComplexWriter out;

    @Workspace private java.util.regex.Matcher matcher;
    @Workspace private com.dremio.dac.proto.model.dataset.IndexType t;

    @Inject private FunctionErrorContext errCtx;

    @Override
    public void setup() {
      matcher = com.dremio.dac.explore.udfs.ExtractPattern.initMatcher(pattern, errCtx);
      t = com.dremio.dac.explore.udfs.ExtractPattern.initType(indexType, errCtx);
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        out.rootAsList(); // calling this sets the ComplexWriter as a list type.
        return;
      }

      final String inputString = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer);
      com.dremio.dac.explore.PatternMatchUtils.Match result = com.dremio.dac.explore.udfs.ExtractPattern.extract(matcher, inputString, t, index.value);
      if (result != null) {
        com.dremio.dac.explore.udfs.DremioUDFUtils.writeCardExample(out,
            new com.dremio.dac.proto.model.dataset.CardExamplePosition(result.start(), result.end() - result.start()));
      } else {
        out.rootAsList(); // calling this sets the ComplexWriter as a list type.
      }
    }
  }
}
