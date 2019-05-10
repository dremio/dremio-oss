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
package com.dremio.exec.physical.config;

import java.util.Map;

import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.Options;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Dictionary lookup operator configuration.
 */
@Options
@JsonTypeName("dictionary_lookup")
public class DictionaryLookupPOP extends AbstractSingle {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DictionaryLookupPOP.class);

  private final Map<String, GlobalDictionaryFieldInfo> dictionaryEncodedFields;
  private final CatalogService catalogService;

  @JsonCreator
  public DictionaryLookupPOP(
      @JacksonInject CatalogService catalogService,
      @JsonProperty("props") OpProps props,
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("dictionaryEncodedFields") Map<String, GlobalDictionaryFieldInfo> dictionaryEncodedFields
      ) {
    super(props, child);
    this.dictionaryEncodedFields = dictionaryEncodedFields;
    this.catalogService = catalogService;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new DictionaryLookupPOP(catalogService, props, child, dictionaryEncodedFields);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitDictionaryLookup(this, value);
  }

  @JsonIgnore
  public CatalogService getCatalogService() {
    return catalogService;
  }

  public Map<String, GlobalDictionaryFieldInfo> getDictionaryEncodedFields() {
    return dictionaryEncodedFields;
  }


  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.DICTIONARY_LOOKUP_VALUE;
  }
}
