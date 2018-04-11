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
package com.dremio.exec.expr;

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;

import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JVar;

public class GetSetVectorHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GetSetVectorHelper.class);

  // eval.add(getValueAccessor.arg(indexVariable).arg(out.getHolder()));

  public static void read(CompleteType ct, JExpression vector, JBlock eval, HoldingContainer out, JCodeModel model,
                          JExpression indexVariable) {

    MinorType type = ct.toMinorType();

      eval.assign(out.getIsSet(), vector.invoke("isSet").arg(indexVariable));
      eval = eval._if(out.getIsSet().eq(JExpr.lit(1)))._then();
      switch (type) {
      case BIGINT:
      case FLOAT4:
      case FLOAT8:
      case INT:
      case MONEY:
      case SMALLINT:
      case TINYINT:
      case UINT1:
      case UINT2:
      case UINT4:
      case UINT8:
      case INTERVALYEAR:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case BIT:
        eval.assign(out.getValue(), vector.invoke("get").arg(indexVariable));
        return;
      case DECIMAL:
        eval.assign(out.getHolder().ref("scale"), JExpr.lit(ct.getType(Decimal.class).getScale()));
        eval.assign(out.getHolder().ref("precision"), JExpr.lit(ct.getType(Decimal.class).getPrecision()));
        eval.assign(out.getHolder().ref("start"), JExpr.lit(TypeHelper.getSize(getArrowMinorType(type))).mul(indexVariable));
        eval.assign(out.getHolder().ref("buffer"), vector.invoke("getDataBuffer"));
        return;
      case INTERVALDAY: {
        JVar start = eval.decl(model.INT, "start", JExpr.lit(TypeHelper.getSize(getArrowMinorType(type))).mul(indexVariable));
        eval.assign(out.getHolder().ref("days"), vector.invoke("getDataBuffer").invoke("getInt").arg(start));
        eval.assign(out.getHolder().ref("milliseconds"), vector.invoke("getDataBuffer").invoke("getInt").arg(start.plus(JExpr.lit(4))));
        return;
      }
      case VARBINARY:
      case VARCHAR:
         eval.assign(out.getHolder().ref("buffer"), vector.invoke("getDataBuffer"));
         JVar se = eval.decl(model.LONG, "startEnd", vector.invoke("getStartEnd").arg(indexVariable));
         eval.assign(out.getHolder().ref("start"), JExpr.cast(model._ref(int.class), se));
         eval.assign(out.getHolder().ref("end"), JExpr.cast(model._ref(int.class), se.shr(JExpr.lit(32))));
        return;

      }

    // fallback.
    eval.add(vector.invoke("get").arg(indexVariable).arg(out.getHolder()));
  }

  public static JInvocation write(MinorType type, JVar vector, HoldingContainer in, JExpression indexVariable, String setMethodName) {

    JInvocation setMethod = vector.invoke(setMethodName).arg(indexVariable);

    if (type == MinorType.UNION) {
      return setMethod.arg(in.getHolder());
    } else {
      setMethod = setMethod.arg(in.f("isSet"));
    }

    switch (type) {
    case BIGINT:
    case FLOAT4:
    case FLOAT8:
    case INT:
    case MONEY:
    case SMALLINT:
    case TINYINT:
    case UINT1:
    case UINT2:
    case UINT4:
    case UINT8:
    case INTERVALYEAR:
    case DATE:
    case TIME:
    case TIMESTAMP:
    case BIT:
    case DECIMAL9:
    case DECIMAL18:
      return setMethod //
          .arg(in.getValue());
    case DECIMAL28DENSE:
    case DECIMAL28SPARSE:
    case DECIMAL38DENSE:
    case DECIMAL38SPARSE:
    case DECIMAL:
      return setMethod //
          .arg(in.f("start")) //
          .arg(in.f("buffer"));
    case INTERVAL:{
      return setMethod //
          .arg(in.f("months")) //
          .arg(in.f("days")) //
          .arg(in.f("milliseconds"));
    }
    case INTERVALDAY: {
      return setMethod //
          .arg(in.f("days")) //
          .arg(in.f("milliseconds"));
    }
    case VAR16CHAR:
    case VARBINARY:
    case VARCHAR:
      return setMethod //
          .arg(in.f("start")) //
          .arg(in.f("end")) //
          .arg(in.f("buffer"));
    }


    return setMethod.arg(in.getHolder());

  }

}
