/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.expr.fn;

import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions.IntExpression;
import com.dremio.common.expression.ValueExpressions.LongExpression;

public class DerivationShortcuts {
  public static int prec(LogicalExpression e){
    return e.getCompleteType().getType(Decimal.class).getPrecision();
  }

  public static int scale(LogicalExpression e){
    return e.getCompleteType().getType(Decimal.class).getScale();
  }

  public static int val(LogicalExpression e){
    if(e instanceof IntExpression){
      return ((IntExpression) e).getInt();
    }else if(e instanceof LongExpression ){
      return (int) ((LongExpression) e).getLong();
    } else {
      throw new IllegalStateException("Tried to retrieve constant value from non constant expression " + e);
    }
  }
}
