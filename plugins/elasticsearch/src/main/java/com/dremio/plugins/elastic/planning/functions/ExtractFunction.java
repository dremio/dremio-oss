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

import com.dremio.common.expression.CompleteType;
import com.dremio.plugins.elastic.planning.rules.SchemaField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

public class ExtractFunction extends ElasticFunction {

  public ExtractFunction() {
    super("extract", "extract");
  }

  @Override
  public FunctionRender render(FunctionRenderer renderer, RexCall call) {
    checkArity(call, 2);

    final String unit =
        ((RexLiteral) call.getOperands().get(0)).getValue().toString().toLowerCase();

    final RexNode fieldOperand = call.getOperands().get(1);
    if (!(fieldOperand instanceof RexInputRef)) {
      throw new RuntimeException(
          "Cannot pushdown extract " + unit + " on a derived column, " + call);
    }

    final SchemaField schemaField = (SchemaField) fieldOperand;
    final CompleteType type = schemaField.getCompleteType();
    if (!type.isTemporal()) {
      throw new RuntimeException(
          String.format(
              "Cannot pushdown extract %s of field %s of type %s.",
              unit, schemaField.getPath().getAsUnescapedPath(), type));
    }

    FunctionRender render = schemaField.accept(renderer.getVisitor());

    return new FunctionRender(
        getExtractScript(unit, render, renderer.isUsingPainless()),
        renderer.isUsingPainless() ? render.getNulls() : EMPTY);
  }

  private String getExtractScript(String unit, FunctionRender render, boolean painless) {
    final String scriptFieldRef = painless ? render.getScript() : render.getNullGuardedScript();
    if (painless) {
      String s = String.format("LocalDateTime.ofInstant(%s, ZoneOffset.UTC)", scriptFieldRef);
      switch (unit) {
        case "year":
          return s + ".getYear()";
        case "month":
          return s + ".getMonthValue()";
        case "dow":
          return s + ".getDayOfWeek().getValue()";
        case "day":
          return s + ".getDayOfMonth()";
        case "hour":
          return s + ".getHourOfDay()";
        case "minute":
          return s + ".getMinute()";
        case "second":
          return s + ".getSecond()";
        default:
          throw new RuntimeException("Unsupported unit for extract pushdown: " + unit);
      }
    } else {
      switch (unit) {
        case "year":
          return scriptFieldRef + ".year";
        case "month":
          return scriptFieldRef + ".monthOfYear";
        case "dow":
          return scriptFieldRef + ".dayOfWeek";
        case "day":
          return scriptFieldRef + ".dayOfMonth";
        case "hour":
          return scriptFieldRef + ".hourOfDay";
        case "minute":
          return scriptFieldRef + ".minuteOfHour";
        case "second":
          return scriptFieldRef + ".secondOfMinute";
        default:
          throw new RuntimeException("Unsupported unit for extract pushdown: " + unit);
      }
    }
  }
}
