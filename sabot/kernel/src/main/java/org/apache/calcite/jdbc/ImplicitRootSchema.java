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
package org.apache.calcite.jdbc;

import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RootSchema;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SchemaTreeProvider.MetadataStatsCollector;
import com.dremio.service.namespace.NamespaceService;

/**
 * Extend {@link SimpleCalciteSchema} to provide a custom root schema instance of type {@link RootSchema}.
 *
 * Note: reason for declaring this class in "org.apache.calcite.jdbc" module is package private scope of
 * {@link SimpleCalciteSchema} class and its constructor.
 */
public class ImplicitRootSchema extends SimpleCalciteSchema {
  public ImplicitRootSchema(final NamespaceService ns, final SabotContext sabotContext, final SchemaConfig schemaConfig,
                            MetadataStatsCollector metadataStatsCollector) {
    super(null, new RootSchema(ns, sabotContext, schemaConfig, metadataStatsCollector), "");
  }
}
