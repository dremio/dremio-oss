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
import com.dremio.dac.proto.model.dataset.Transform;
import com.dremio.dac.proto.model.dataset.TransformAddCalculatedField;
import com.dremio.dac.proto.model.dataset.TransformConvertCase;
import com.dremio.dac.proto.model.dataset.TransformConvertToSingleType;
import com.dremio.dac.proto.model.dataset.TransformCreateFromParent;
import com.dremio.dac.proto.model.dataset.TransformDrop;
import com.dremio.dac.proto.model.dataset.TransformExtract;
import com.dremio.dac.proto.model.dataset.TransformField;
import com.dremio.dac.proto.model.dataset.TransformFilter;
import com.dremio.dac.proto.model.dataset.TransformGroupBy;
import com.dremio.dac.proto.model.dataset.TransformJoin;
import com.dremio.dac.proto.model.dataset.TransformLookup;
import com.dremio.dac.proto.model.dataset.TransformRename;
import com.dremio.dac.proto.model.dataset.TransformSort;
import com.dremio.dac.proto.model.dataset.TransformSorts;
import com.dremio.dac.proto.model.dataset.TransformSplitByDataType;
import com.dremio.dac.proto.model.dataset.TransformTrim;
import com.dremio.dac.proto.model.dataset.TransformType;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.util.JSONUtil;
import com.dremio.datastore.Converter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

/**
 * Transform base class
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonTypeIdResolver(EnumTypeIdResolver.class)
@TypesEnum(types = TransformType.class, format = "com.dremio.dac.proto.model.dataset.Transform%s")
public abstract class TransformBase  {

  public static final Acceptor<TransformBase, TransformVisitor<?>, Transform> acceptor = new Acceptor<TransformBase, TransformVisitor<?>, Transform>(){};

  public final <T> T accept(TransformVisitor<T> visitor) throws VisitorException {
    return acceptor.accept(visitor, this);
  }

  public Transform wrap() {
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
  public interface TransformVisitor<T> {
    T visit(TransformLookup lookup) throws Exception;
    T visit(TransformJoin join) throws Exception;
    T visit(TransformSort sort) throws Exception;
    T visit(TransformSorts sortMultiple) throws Exception;
    T visit(TransformDrop drop) throws Exception;
    T visit(TransformRename rename) throws Exception;
    T visit(TransformConvertCase convertCase) throws Exception;
    T visit(TransformTrim trim) throws Exception;
    T visit(TransformExtract extract) throws Exception;
    T visit(TransformAddCalculatedField addCalculatedField) throws Exception;
    T visit(TransformUpdateSQL updateSQL) throws Exception;
    T visit(TransformField field) throws Exception;
    T visit(TransformConvertToSingleType convertToSingleType) throws Exception;
    T visit(TransformSplitByDataType splitByDataType) throws Exception;
    T visit(TransformGroupBy groupBy) throws Exception;
    T visit(TransformFilter filter) throws Exception;
    T visit(TransformCreateFromParent createFromParent) throws Exception;
  }

  public static TransformBase unwrap(Transform t) {
    return acceptor.unwrap(t);
  }

  public static Converter<TransformBase, Transform> converter() {
    return acceptor.converter();
  }
}
