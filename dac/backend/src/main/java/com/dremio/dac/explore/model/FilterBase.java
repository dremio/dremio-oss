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
package com.dremio.dac.explore.model;

import com.dremio.dac.model.common.Acceptor;
import com.dremio.dac.model.common.EnumTypeIdResolver;
import com.dremio.dac.model.common.TypesEnum;
import com.dremio.dac.model.common.VisitorException;
import com.dremio.dac.proto.model.dataset.FilterByType;
import com.dremio.dac.proto.model.dataset.FilterCleanData;
import com.dremio.dac.proto.model.dataset.FilterConvertibleData;
import com.dremio.dac.proto.model.dataset.FilterConvertibleDataWithPattern;
import com.dremio.dac.proto.model.dataset.FilterCustom;
import com.dremio.dac.proto.model.dataset.FilterDefinition;
import com.dremio.dac.proto.model.dataset.FilterPattern;
import com.dremio.dac.proto.model.dataset.FilterRange;
import com.dremio.dac.proto.model.dataset.FilterType;
import com.dremio.dac.proto.model.dataset.FilterValue;
import com.dremio.dac.util.JSONUtil;
import com.dremio.datastore.Converter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

/**
 * Filter base class
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonTypeIdResolver(EnumTypeIdResolver.class)
@TypesEnum(types = FilterType.class, format = "com.dremio.dac.proto.model.dataset.Filter%s")
public class FilterBase {

  public static final Acceptor<FilterBase, FilterVisitor<?>, FilterDefinition> acceptor = new Acceptor<FilterBase, FilterVisitor<?>, FilterDefinition>(){};

  public final <T> T accept(FilterVisitor<T> visitor) throws VisitorException {
    return acceptor.accept(visitor, this);
  }

  public FilterDefinition wrap() {
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
  public abstract static class FilterVisitor<T> {
    public T visit(FilterDefinition e) {
      return unwrap(e).accept(this);
    }
    public abstract T visit(FilterCleanData clean) throws Exception;
    public abstract T visit(FilterByType byType) throws Exception;
    public abstract T visit(FilterRange range) throws Exception;
    public abstract T visit(FilterCustom custom) throws Exception;
    public abstract T visit(FilterValue value) throws Exception;
    public abstract T visit(FilterPattern value) throws Exception;
    public abstract T visit(FilterConvertibleData value) throws Exception;
    public abstract T visit(FilterConvertibleDataWithPattern value) throws Exception;
  }

  public static FilterBase unwrap(FilterDefinition t) {
    return acceptor.unwrap(t);
  }

  public static Converter<FilterBase, FilterDefinition> converter() {
    return acceptor.converter();
  }
}
