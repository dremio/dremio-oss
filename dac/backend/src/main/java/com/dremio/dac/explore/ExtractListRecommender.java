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

import static com.dremio.dac.proto.model.dataset.ExtractListRuleType.multiple;
import static com.dremio.dac.proto.model.dataset.ExtractListRuleType.single;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.JSONElementLocator.ArrayJsonPathElement;
import com.dremio.dac.explore.JSONElementLocator.JsonPath;
import com.dremio.dac.explore.JSONElementLocator.JsonSelection;
import com.dremio.dac.explore.model.extract.Selection;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.Direction;
import com.dremio.dac.proto.model.dataset.ExtractListRule;
import com.dremio.dac.proto.model.dataset.ExtractRuleMultiple;
import com.dremio.dac.proto.model.dataset.ExtractRuleSingle;
import com.dremio.dac.proto.model.dataset.ListSelection;
import com.dremio.dac.proto.model.dataset.Offset;
import com.dremio.dac.service.errors.ClientErrorException;

/**
 * Extract list transformation recommendation suggestions and generating examples and number of matches in sample data
 * for each recommendation.
 */
public class ExtractListRecommender extends Recommender<ExtractListRule, Selection> {
  private static final Logger logger = LoggerFactory.getLogger(ExtractListRecommender.class);

  @Override
  public List<ExtractListRule> getRules(Selection selection, DataType selColType) {
    checkArgument(selColType == DataType.LIST, "Extract list items is supported only on LIST type columns");
    JsonSelection jsonSelection;
    try {
      jsonSelection = new JSONElementLocator(selection.getCellText()).locate(selection.getOffset(), selection.getOffset() + selection.getLength());
    } catch (IOException e) {
      throw new ClientErrorException(String.format("invalid JSON: %s", selection.getCellText()), e);
    }

    ArrayJsonPathElement start = extractArrayIndex(jsonSelection.getStart());
    ArrayJsonPathElement end = extractArrayIndex(jsonSelection.getEnd());

    List<ExtractListRule> rules = new ArrayList<>();
    if (start == end) {
      rules.add(new ExtractListRule(single).setSingle(new ExtractRuleSingle(start.getPosition())));
    } else {
      ListSelection[] selections = {
          new ListSelection(fromTheStart(start), fromTheStart(end)),
          new ListSelection(fromTheStart(start), fromTheEnd(end)),
          new ListSelection(fromTheEnd(start), fromTheStart(end)),
          new ListSelection(fromTheEnd(start), fromTheEnd(end))
      };
      for (ListSelection listSelection : selections) {
        rules.add((new ExtractListRule(multiple)
            .setMultiple(
                new ExtractRuleMultiple(listSelection)
            )));
      }
    }

    return rules;
  }

  @Override
  public TransformRuleWrapper<ExtractListRule> wrapRule(ExtractListRule rule) {
    switch (rule.getType()) {
      case single:
        return new ExtractListSingleTransformRuleWrapper(rule);
      case multiple:
        return new ExtractListMultipleTransformRuleWrapper(rule);
      default:
        throw UserException.unsupportedError()
            .message("Unsupported list extract type: " + rule.getType())
            .build(logger);
    }
  }

  private static class ExtractListSingleTransformRuleWrapper extends TransformRuleWrapper<ExtractListRule> {
    private final ExtractListRule rule;

    ExtractListSingleTransformRuleWrapper(ExtractListRule rule) {
      this.rule = rule;
    }

    @Override
    public String getMatchFunctionExpr(String input) {
      final String subList = getFunctionExpr(input);
      return String.format("%s IS NOT NULL", subList);
    }

    @Override
    public boolean canGenerateExamples() {
      return false;
    }

    @Override
    public String getExampleFunctionExpr(String input) {
      throw new UnsupportedOperationException("Example generation is not supported for extract list transform.");
    }

    @Override
    public String getFunctionExpr(String expr, Object... args) {
      return String.format("%s[%d]", expr, rule.getSingle().getIndex());
    }

    @Override
    public ExtractListRule getRule() {
      return rule;
    }

    @Override
    public String describe() {
      return "Element: " + String.valueOf(getRule().getSingle().getIndex());
    }
  }

  private static class ExtractListMultipleTransformRuleWrapper extends TransformRuleWrapper<ExtractListRule> {
    private final ExtractListRule rule;

    ExtractListMultipleTransformRuleWrapper(ExtractListRule rule) {
      this.rule = rule;
    }

    @Override
    public String getMatchFunctionExpr(String input) {
      final String subList = getFunctionExpr(input);

      // If the sublist contains at least one element, then a match is found.
      return String.format("%s(%s) > 0", "array_length", subList);
    }

    @Override
    public boolean canGenerateExamples() {
      return false;
    }

    @Override
    public String getExampleFunctionExpr(String input) {
      throw new UnsupportedOperationException("Example generation is not supported for extract list transform.");
    }

    @Override
    public String getFunctionExpr(String expr, Object... args) {
      final ListSelection sel = rule.getMultiple().getSelection();
      return String.format("%s(%s, %d, %s)",
          "sublist",
          expr,
          getOffset(sel.getStart()),
          getLength(expr, sel.getStart(), sel.getEnd())
      );
    }

    @Override
    public ExtractListRule getRule() {
      return rule;
    }

    @Override
    public String describe() {
      ListSelection s = rule.getMultiple().getSelection();
      return ExtractRecommender.describePlacement(s.getStart(), s.getEnd()) ;
    }
  }

  private static ArrayJsonPathElement extractArrayIndex(JsonPath path) {
    if (path.size() == 1 && path.last().isArray()) {
      return path.last().asArray();
    }
    throw new ClientErrorException(String.format("not an array selection: %s", path));
  }

  private static Offset fromTheEnd(ArrayJsonPathElement a) {
    return new Offset(a.getCount() - a.getPosition() - 1, Direction.FROM_THE_END);
  }

  private static Offset fromTheStart(ArrayJsonPathElement a) {
    return new Offset(a.getPosition(), Direction.FROM_THE_START);
  }

  private static int getOffset(Offset start) {
    // sublist takes offset in range [1, length]
    return start.getDirection() == Direction.FROM_THE_END ? -1 * (start.getValue() + 1) : (start.getValue() + 1);
  }

  private static String getLength(String expr, Offset start, Offset end) {
    // Both ends are inclusive
    if (start.getDirection() == Direction.FROM_THE_END) {
      if (end.getDirection() == Direction.FROM_THE_END) {
        // (length(str) - end - 1) - (length(str) - start - 1) + 1 = start - end
        return String.valueOf(start.getValue() - end.getValue() + 1);
      }

      // end - (length(str) - start - 1) + 1 = -length(str) + start + end + 2
      return format("-array_length(%s) + %d", expr, start.getValue() + end.getValue() + 2);
    }

    if (end.getDirection() == Direction.FROM_THE_END) {
      // (length(str) - end - 1) - start + 1 = length(str) - (start + end)
      return format("array_length(%s) - %d", expr, start.getValue() + end.getValue());
    }

    // end - start
    return String.valueOf(end.getValue() - start.getValue() + 1);
  }
}
