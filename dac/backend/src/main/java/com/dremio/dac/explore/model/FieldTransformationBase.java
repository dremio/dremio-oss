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
import com.dremio.dac.proto.model.dataset.FieldConvertCase;
import com.dremio.dac.proto.model.dataset.FieldConvertDateToNumber;
import com.dremio.dac.proto.model.dataset.FieldConvertDateToText;
import com.dremio.dac.proto.model.dataset.FieldConvertFloatToDecimal;
import com.dremio.dac.proto.model.dataset.FieldConvertFloatToInteger;
import com.dremio.dac.proto.model.dataset.FieldConvertFromJSON;
import com.dremio.dac.proto.model.dataset.FieldConvertListToText;
import com.dremio.dac.proto.model.dataset.FieldConvertNumberToDate;
import com.dremio.dac.proto.model.dataset.FieldConvertTextToDate;
import com.dremio.dac.proto.model.dataset.FieldConvertToJSON;
import com.dremio.dac.proto.model.dataset.FieldConvertToTypeIfPossible;
import com.dremio.dac.proto.model.dataset.FieldConvertToTypeWithPatternIfPossible;
import com.dremio.dac.proto.model.dataset.FieldExtract;
import com.dremio.dac.proto.model.dataset.FieldExtractList;
import com.dremio.dac.proto.model.dataset.FieldExtractMap;
import com.dremio.dac.proto.model.dataset.FieldReplaceCustom;
import com.dremio.dac.proto.model.dataset.FieldReplacePattern;
import com.dremio.dac.proto.model.dataset.FieldReplaceRange;
import com.dremio.dac.proto.model.dataset.FieldReplaceValue;
import com.dremio.dac.proto.model.dataset.FieldSimpleConvertToType;
import com.dremio.dac.proto.model.dataset.FieldSplit;
import com.dremio.dac.proto.model.dataset.FieldTransformation;
import com.dremio.dac.proto.model.dataset.FieldTransformationType;
import com.dremio.dac.proto.model.dataset.FieldTrim;
import com.dremio.dac.proto.model.dataset.FieldUnnestList;
import com.dremio.dac.util.JSONUtil;
import com.dremio.datastore.Converter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

/**
 * Expression base class
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonTypeIdResolver(EnumTypeIdResolver.class)
@TypesEnum(types = FieldTransformationType.class, format = "com.dremio.dac.proto.model.dataset.Field%s")
public class FieldTransformationBase {

  public static final Acceptor<FieldTransformationBase, FieldTransformationVisitor<?>, FieldTransformation> acceptor = new Acceptor<FieldTransformationBase, FieldTransformationVisitor<?>, FieldTransformation>(){};

  public final <T> T accept(FieldTransformationVisitor<T> visitor) throws VisitorException {
    return acceptor.accept(visitor, this);
  }

  public FieldTransformation wrap() {
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
  public abstract static class FieldTransformationVisitor<T> {
    public T visit(FieldTransformation e) {
      return unwrap(e).accept(this);
    }

    public abstract T visit(FieldConvertCase convertCase) throws Exception;
    public abstract T visit(FieldTrim trim) throws Exception;
    public abstract T visit(FieldExtract extract) throws Exception;
    public abstract T visit(FieldConvertFloatToInteger floatToInt) throws Exception;
    public abstract T visit(FieldConvertFloatToDecimal floatToDec) throws Exception;
    public abstract T visit(FieldConvertDateToText dateToText) throws Exception;
    public abstract T visit(FieldConvertNumberToDate numberToDate) throws Exception;
    public abstract T visit(FieldConvertDateToNumber dateToNumber) throws Exception;
    public abstract T visit(FieldConvertTextToDate textToDate) throws Exception;
    public abstract T visit(FieldConvertListToText listToText) throws Exception;
    public abstract T visit(FieldConvertToJSON toJson) throws Exception;
    public abstract T visit(FieldUnnestList unnest) throws Exception;
    public abstract T visit(FieldReplacePattern replacePattern) throws Exception;
    public abstract T visit(FieldReplaceCustom replaceCustom) throws Exception;
    public abstract T visit(FieldReplaceValue replaceValue) throws Exception;
    public abstract T visit(FieldReplaceRange replaceRange) throws Exception;
    public abstract T visit(FieldExtractMap extract) throws Exception;
    public abstract T visit(FieldExtractList extract) throws Exception;
    public abstract T visit(FieldSplit split) throws Exception;
    public abstract T visit(FieldSimpleConvertToType toType) throws Exception;
    public abstract T visit(FieldConvertToTypeIfPossible toTypeIfPossible) throws Exception;
    public abstract T visit(FieldConvertToTypeWithPatternIfPossible toTypeIfPossible) throws Exception;
    public abstract T visit(FieldConvertFromJSON fromJson) throws Exception;
  }

  public static FieldTransformationBase unwrap(FieldTransformation t) {
    return acceptor.unwrap(t);
  }

  public static Converter<FieldTransformationBase, FieldTransformation> converter() {
    return acceptor.converter();
  }

}
