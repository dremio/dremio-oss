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
package com.dremio.dac.explore.model;

import com.dremio.dac.model.common.Acceptor;
import com.dremio.dac.model.common.EnumTypeIdResolver;
import com.dremio.dac.model.common.TypesEnum;
import com.dremio.dac.model.common.VisitorException;
import com.dremio.dac.proto.model.dataset.ExpCalculatedField;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.ExpConvertCase;
import com.dremio.dac.proto.model.dataset.ExpConvertType;
import com.dremio.dac.proto.model.dataset.ExpExtract;
import com.dremio.dac.proto.model.dataset.ExpFieldTransformation;
import com.dremio.dac.proto.model.dataset.ExpMeasure;
import com.dremio.dac.proto.model.dataset.ExpTrim;
import com.dremio.dac.proto.model.dataset.Expression;
import com.dremio.dac.proto.model.dataset.ExpressionType;
import com.dremio.dac.util.JSONUtil;
import com.dremio.datastore.Converter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

/**
 * Expression base class
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonTypeIdResolver(EnumTypeIdResolver.class)
@TypesEnum(types = ExpressionType.class, format = "com.dremio.dac.proto.model.dataset.Exp%s")
public class ExpressionBase {

  public static final Acceptor<ExpressionBase, ExpressionVisitor<?>, Expression> acceptor = new Acceptor<ExpressionBase, ExpressionVisitor<?>, Expression>(){};

  public final <T> T accept(ExpressionVisitor<T> visitor) throws VisitorException {
    return acceptor.accept(visitor, this);
  }

  public Expression wrap() {
    return acceptor.wrap(this);
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }

  /**
   * Type safe mechanism to handle all possible transforms
   *
   * @param <T>
   */
  public abstract static class ExpressionVisitor<T> {
    public T visit(Expression e) {
      return unwrap(e).accept(this);
    }
    public abstract T visit(ExpColumnReference col) throws Exception;
    @Deprecated // use ExpFieldTransformation
    public abstract T visit(ExpConvertCase changeCase) throws Exception;
    @Deprecated // use ExpFieldTransformation
    public abstract T visit(ExpExtract extract) throws Exception;
    @Deprecated // use ExpFieldTransformation
    public abstract T visit(ExpTrim trim) throws Exception;
    public abstract T visit(ExpCalculatedField calculatedField) throws Exception;
    public abstract T visit(ExpFieldTransformation fieldTransformation) throws Exception;
    public abstract T visit(ExpConvertType convertType) throws Exception;
    public abstract T visit(ExpMeasure measure) throws Exception;
  }

  /**
   * Base visitor that traverses into expressions where possible.
   * @param <T> return type from the recursive visit of the expression tree
   */
  public abstract static class ExpressionVisitorBase<T> extends ExpressionVisitor<T> {
    @Override
    public abstract T visit(ExpColumnReference col) throws Exception;

    @Override
    public T visit(ExpConvertCase changeCase) throws Exception {
      return visit(changeCase.getOperand());
    }

    @Override
    public T visit(ExpExtract extract) throws Exception {
      return visit(extract.getOperand());
    }

    @Override
    public T visit(ExpTrim trim) throws Exception {
      return visit(trim.getOperand());
    }

    public abstract T visit(ExpCalculatedField calculatedField) throws Exception;

    @Override
    public T visit(ExpFieldTransformation fieldTransformation) throws Exception {
      return visit(fieldTransformation.getOperand());
    }

    @Override
    public T visit(ExpConvertType convertType) throws Exception {
      return visit(convertType.getOperand());
    }

    @Override
    public T visit(ExpMeasure measure) throws Exception {
      return visit(measure.getOperand());
    }
  }

  public static ExpressionBase unwrap(Expression t) {
    return acceptor.unwrap(t);
  }

  public static Converter<ExpressionBase, Expression> converter() {
    return acceptor.converter();
  }
}
