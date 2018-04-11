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
import com.dremio.dac.proto.model.dataset.From;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.FromSubQuery;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.FromType;
import com.dremio.dac.util.JSONUtil;
import com.dremio.datastore.Converter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

/**
 * Transform base class
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonTypeIdResolver(EnumTypeIdResolver.class)
@TypesEnum(types = FromType.class, format = "com.dremio.dac.proto.model.dataset.From%s")
public abstract class FromBase  {

  public static final Acceptor<FromBase, FromVisitor<?>, From> acceptor = new Acceptor<FromBase, FromVisitor<?>, From>(){};

  public final <T> T accept(FromVisitor<T> visitor) throws VisitorException {
    return acceptor.accept(visitor, this);
  }

  public From wrap() {
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
  public abstract static class FromVisitor<T> {
    public T visit(From e) {
      return unwrap(e).accept(this);
    }
    public abstract T visit(FromTable table) throws Exception;
    public abstract T visit(FromSQL sql) throws Exception;
    public abstract T visit(FromSubQuery subQuery) throws Exception;
  }

  public static FromBase unwrap(From t) {
    return acceptor.unwrap(t);
  }

  public static Converter<FromBase, From> converter() {
    return acceptor.converter();
  }
}
