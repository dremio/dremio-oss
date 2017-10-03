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
package com.dremio.common.config;

import java.util.Set;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.FormatPluginConfigBase;
import com.dremio.common.logical.StoragePluginConfigBase;
import com.dremio.common.logical.data.LogicalOperatorBase;
import com.dremio.common.scanner.persistence.ScanResult;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;


public class LogicalPlanPersistence {
  private ObjectMapper mapper;

  public ObjectMapper getMapper() {
    return mapper;
  }

  public LogicalPlanPersistence(SabotConfig conf, ScanResult scanResult) {
    mapper = new ObjectMapper();

    SimpleModule deserModule = new SimpleModule("LogicalExpressionDeserializationModule")
        .addDeserializer(LogicalExpression.class, new LogicalExpression.De(conf))
        .addDeserializer(SchemaPath.class, new SchemaPath.De());

    mapper.registerModule(new AfterburnerModule());
    mapper.registerModule(deserModule);
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
    mapper.configure(Feature.ALLOW_COMMENTS, true);
    registerSubtypes(LogicalOperatorBase.getSubTypes(scanResult));
    registerSubtypes(StoragePluginConfigBase.getSubTypes(scanResult));
    registerSubtypes(FormatPluginConfigBase.getSubTypes(scanResult));
  }

  private <T> void registerSubtypes(Set<Class<? extends T>> types) {
    for (Class<? extends T> type : types) {
      mapper.registerSubtypes(type);
    }
  }
}
