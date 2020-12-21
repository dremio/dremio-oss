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

import static com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.dac.explore.PatternMatchUtils;
import com.dremio.dac.explore.PatternMatchUtils.Match;
import com.dremio.dac.explore.udfs.DremioUDFUtils.ExampleUDFOutputDerivation;
import com.dremio.dac.proto.model.dataset.SplitPositionType;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.OutputDerivation;

/**
 * Functions to split string to list
 */
public class SplitPattern {
  public static final String REGEXP_SPLIT = "regexp_split";
  public static final String REGEXP_SPLIT_POSITIONS = "regexp_split_positions";

  public static List<Match> splitRegex(Matcher matcher, String matchee) {
    matcher.reset(matchee);
    List<Match> matches = new ArrayList<Match>();
    while (matcher.find()) {
      matches.add(PatternMatchUtils.match(matcher));
    }
    return matches;
  }

  public static boolean range(int start, int end, int length) {
  return start >= 0 && end >= start && end <= length;
  }

  public static SplitPositionType initPositionType(NullableVarCharHolder value) {
    return SplitPositionType.valueOf(toStringFromUTF8(value.start, value.end, value.buffer));
  }

  public static Matcher initMatcher(NullableVarCharHolder pattern) {
    return Pattern.compile(
        toStringFromUTF8(pattern.start, pattern.end, pattern.buffer)
    ).matcher("");
  }

  public static void split(ListWriter writer, ArrowBuf in, int start, int length, java.util.List<Match> matches, SplitPositionType position, int param) {
    //param can be index in case of positionType == INDEX or maxCount in case of positionType == ALL
    if (position != SplitPositionType.ALL) {
      Match m = null;
      switch (position) {
        case FIRST:
          m = matches.get(0);
          break;
        case LAST:
          m = matches.get(matches.size() - 1);
          break;
        default:
          if (param < matches.size()) {
            m = matches.get(param);
          }
      }
      if (m != null) {
        if (com.dremio.dac.explore.udfs.SplitPattern.range(0, m.start(), length)) {
          writer.varChar().writeVarChar(start, start + m.start(), in);
        }
        if (com.dremio.dac.explore.udfs.SplitPattern.range(m.end(), length, length)) {
          writer.varChar().writeVarChar(start + m.end(), start + length, in);
        }
      } else {
        writer.varChar().writeVarChar(start, start + length, in);
      }
    } else {
      int p = 0;
      for (Match m : matches) {
        if (com.dremio.dac.explore.udfs.SplitPattern.range(p, m.start(), length)) {
          writer.varChar().writeVarChar(start + p, start + m.start(), in);
          param--;
        }
        p = m.end();
        if (param <= 0) {
          break;
        }
      }
      if (com.dremio.dac.explore.udfs.SplitPattern.range(p, length, length) && param > 0) {
        writer.varChar().writeVarChar(start + p, start + length, in);
      }
    }
  }

  /**
   * {@link OutputDerivation} for {@link RegexpSplit}
   */
  public static final class RegexpSplitOutputDerivation implements OutputDerivation {

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      return new CompleteType(
          ArrowType.List.INSTANCE,
          CompleteType.VARCHAR.toField(ListVector.DATA_VECTOR_NAME)
      );
    }
  }

  /**
   * Split given input based on the given delimiter. Delimiter is a regex.
   *
   * Parameters:
   * 1. pattern: delimiter regex
   * 2. Delimiter position to consider: One of [ALL (consider all occurrences of delimiter), FIRST (first occurrence), LAST, INDEX (specific number occurrence)]
   * 3. Index of delimiter position to consider (when delimiter position to consider in INDEX)
   */
  @FunctionTemplate(name = REGEXP_SPLIT, scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL, derivation = RegexpSplitOutputDerivation.class)
  public static class RegexpSplit implements SimpleFunction {

    @Param private NullableVarCharHolder in;
    @Param(constant=true) private NullableVarCharHolder pattern;
    @Param(constant=true) private NullableVarCharHolder positionTypeStr;
    @Param(constant=true) private NullableIntHolder param;
    @Output private ComplexWriter out;

    @Workspace private java.util.regex.Matcher matcher;
    @Workspace private com.dremio.dac.proto.model.dataset.SplitPositionType positionType;

    @Inject private FunctionErrorContext errCtx;

    @Override
    public void setup() {
      matcher = com.dremio.dac.explore.udfs.SplitPattern.initMatcher(pattern);
      positionType = com.dremio.dac.explore.udfs.SplitPattern.initPositionType(positionTypeStr);
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        out.rootAsList(); // calling this sets the ComplexWriter as a list type.
        return;
      }

      final int length = com.dremio.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(io.netty.buffer.NettyArrowBuf.unwrapBuffer(in.buffer),
        in.start, in.end, errCtx);
      final String v = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer);

      java.util.List<Match> matches = com.dremio.dac.explore.udfs.SplitPattern.splitRegex(matcher, v);

      org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter writer = out.rootAsList();
      writer.startList();
      if (matches.size() > 0) {
        com.dremio.dac.explore.udfs.SplitPattern.split(writer, in.buffer, in.start, length, matches, positionType, param.value);
      } else {
        writer.varChar().writeVarChar(in.start, in.end, in.buffer);
      }
      writer.endList();
    }
  }

  /**
   * Generates a list of positions for generating card examples. Each position is a map containing
   * "start" and "end".
   *
   * Parameters:
   * 1. input: input column
   * 2. pattern: delimiter (regex)
   */
  @FunctionTemplate(name = REGEXP_SPLIT_POSITIONS, scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL, derivation = ExampleUDFOutputDerivation.class)
  public static class SplitExample implements SimpleFunction {

    @Param private NullableVarCharHolder in;
    @Param(constant=true) private NullableVarCharHolder pattern;
    @Output private ComplexWriter out;

    @Workspace private java.util.regex.Matcher matcher;

    @Override
    public void setup() {
      matcher = com.dremio.dac.explore.udfs.SplitPattern.initMatcher(pattern);
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        out.rootAsList(); // calling this sets the ComplexWriter as a list type.
        return;
      }

      final String inputString = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer);

      java.util.List<com.dremio.dac.explore.PatternMatchUtils.Match> matches = com.dremio.dac.explore.udfs.SplitPattern.splitRegex(matcher, inputString);

      if (matches.size() > 0) {
        com.dremio.dac.proto.model.dataset.CardExamplePosition[] positions = new com.dremio.dac.proto.model.dataset.CardExamplePosition[matches.size()];

        for(int i = 0; i < matches.size(); i++) {
          com.dremio.dac.explore.PatternMatchUtils.Match match = (com.dremio.dac.explore.PatternMatchUtils.Match) matches.get(i);
          positions[i] = new com.dremio.dac.proto.model.dataset.CardExamplePosition(match.start(), match.end() - match.start());
        }

        com.dremio.dac.explore.udfs.DremioUDFUtils.writeCardExample(out, positions);
      } else {
        out.rootAsList(); // calling this sets the ComplexWriter as a list type.
      }
    }
  }
}
