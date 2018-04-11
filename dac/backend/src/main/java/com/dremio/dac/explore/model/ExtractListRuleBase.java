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
import com.dremio.dac.proto.model.dataset.ExtractListRule;
import com.dremio.dac.proto.model.dataset.ExtractListRuleType;
import com.dremio.dac.proto.model.dataset.ExtractRuleMultiple;
import com.dremio.dac.proto.model.dataset.ExtractRuleSingle;
import com.dremio.dac.util.JSONUtil;
import com.dremio.datastore.Converter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

/**
 * ExtractListRule base class
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonTypeIdResolver(EnumTypeIdResolver.class)
@TypesEnum(types = ExtractListRuleType.class, format = "com.dremio.dac.proto.model.dataset.ExtractRule%s")
public abstract class ExtractListRuleBase  {

  public static final Acceptor<ExtractListRuleBase, ExtractListRuleVisitor<?>, ExtractListRule> acceptor = new Acceptor<ExtractListRuleBase, ExtractListRuleVisitor<?>, ExtractListRule>(){};

  public final <T> T accept(ExtractListRuleVisitor<T> visitor) throws VisitorException {
    return acceptor.accept(visitor, this);
  }

  public ExtractListRule wrap() {
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
  public abstract static class ExtractListRuleVisitor<T> {
    public T visit(ExtractListRule e) {
      return unwrap(e).accept(this);
    }

    public abstract T visit(ExtractRuleSingle single) throws Exception;
    public abstract T visit(ExtractRuleMultiple multiple) throws Exception;
  }

  public static ExtractListRuleBase unwrap(ExtractListRule t) {
    return acceptor.unwrap(t);
  }

  public static Converter<ExtractListRuleBase, ExtractListRule> converter() {
    return acceptor.converter();
  }
}
