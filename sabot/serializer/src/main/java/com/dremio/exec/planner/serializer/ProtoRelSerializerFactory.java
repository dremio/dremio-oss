/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
package com.dremio.exec.planner.serializer;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTranslatableTable;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.serialization.DeserializationException;
import com.dremio.exec.planner.serialization.LogicalPlanDeserializer;
import com.dremio.exec.planner.serialization.LogicalPlanSerializer;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.planner.serializer.RelNodeSerde.PluginRetriever;
import com.dremio.exec.planner.serializer.RelNodeSerde.TableRetriever;
import com.dremio.exec.store.CatalogService;
import com.dremio.plan.serialization.PRelList;
import com.dremio.plan.serialization.PRelNodeTypes;
import com.dremio.plan.serialization.PRexInputRef;
import com.dremio.plan.serialization.PRexNodeTypes;
import com.dremio.service.namespace.NamespaceKey;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;

/**
 * Serializer factory that produces protobuf serialized RelNode trees.
 */
public class ProtoRelSerializerFactory extends RelSerializerFactory {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProtoRelSerializerFactory.class);

  static final TypeRegistry REGISTRY = TypeRegistry.newBuilder()
      .add(PRelNodeTypes.getDescriptor().getMessageTypes())
      .add(PRexNodeTypes.getDescriptor().getMessageTypes())
      .build();

  private static final Set<FieldDescriptor> ALWAYS_INCLUDE_VALUES = PRexInputRef.getDescriptor()
      .getFields()
      .stream()
      .filter(t -> t.getName().equalsIgnoreCase("index"))
      .collect(Collectors.toSet());

  private final RelSerdeRegistry serdeRegistry;

  public ProtoRelSerializerFactory(ScanResult scanResult) {
    super();
    this.serdeRegistry = new RelSerdeRegistry(scanResult);
  }

  @Override
  public LogicalPlanSerializer getSerializer(RelOptCluster cluster, FunctionImplementationRegistry registry) {
    return new LogicalPlanSerializer() {

      @Override
      public byte[] serializeToBytes(RelNode plan) {
        try {
          return ser(plan).toByteArray();
        } catch (Exception ex) {
          throw UserException.validationError(ex).message("Failure serializing plan.").build(logger);
        }
      }

      private PRelList ser(RelNode plan) {
        SqlOperatorConverter sqlOperatorConverter = new SqlOperatorConverter(registry);
        PRelList list = RelSerializer.serializeList(serdeRegistry, plan, sqlOperatorConverter);
        return list;
      }

      @Override
      public String serializeToJson(RelNode plan) {
        try {
          return JsonFormat.printer().usingTypeRegistry(REGISTRY).includingDefaultValueFields(ALWAYS_INCLUDE_VALUES).print(ser(plan));
        } catch (Exception ex) {
          throw UserException.validationError(ex).message("Failure serializing plan.").build(logger);
        }
      }
    };
  }

  @Override
  public LogicalPlanDeserializer getDeserializer(RelOptCluster cluster, DremioCatalogReader catalogReader, FunctionImplementationRegistry registry, CatalogService catalogService) {
    final TableRetriever tableRetriever = new TableRetriever() {

      @Override
      public DremioPrepareTable getTable(NamespaceKey key) {
         return catalogReader.getTable(key.getPathComponents());
      }

      @Override
      public DremioTranslatableTable getTableSnapshot(NamespaceKey key, TableVersionContext context) {
        return catalogReader.getTableSnapshot(key, context);
      }
    };

    final PluginRetriever pluginRetriever = t -> catalogService.getSource(t);
    final SqlOperatorConverter sqlOperatorConverter = new SqlOperatorConverter(registry);
    return new LogicalPlanDeserializer() {

      @Override
      public RelNode deserialize(byte[] data) {
        try {
          PRelList list = PRelList.parseFrom(data);
          return RelDeserializer.deserialize(serdeRegistry, DremioRelFactories.CALCITE_LOGICAL_BUILDER, tableRetriever, pluginRetriever, registry, list, cluster, sqlOperatorConverter);
        } catch (Exception ex) {
          throw new DeserializationException(ex);
        }
      }

      @Override
      public RelNode deserialize(String data) {
        PRelList.Builder builder = PRelList.newBuilder();
        try {
          JsonFormat.parser().usingTypeRegistry(REGISTRY).merge(data, builder);
          return RelDeserializer.deserialize(serdeRegistry, DremioRelFactories.CALCITE_LOGICAL_BUILDER, tableRetriever, pluginRetriever, registry, builder.build(), cluster, sqlOperatorConverter);
        } catch (Exception ex) {
          throw UserException.validationError(ex).message("Failure deserializing plan.").build(logger);
        }
      }
    };
  }

}
