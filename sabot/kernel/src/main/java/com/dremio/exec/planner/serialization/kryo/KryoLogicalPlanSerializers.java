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
package com.dremio.exec.planner.serialization.kryo;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.planner.serialization.DeserializationException;
import com.dremio.exec.planner.serialization.LogicalPlanDeserializer;
import com.dremio.exec.planner.serialization.LogicalPlanSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A convenience class used to create Kryo based logical plan de/serializers.
 */
public final class KryoLogicalPlanSerializers {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KryoLogicalPlanSerializers.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private KryoLogicalPlanSerializers() { }

  /**
   * Returns a new {@link LogicalPlanSerializer}
   * @param cluster cluster to used during serialization
   */
  public static LogicalPlanSerializer forSerialization(final RelOptCluster cluster) {
    final Kryo kryo = new Kryo();
    // use objenesis for creating mock objects
    kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

    final DremioCatalogReader catalog = kryo.newInstance(DremioCatalogReader.class);
    final KryoRelSerializer serializer = KryoRelSerializer.newBuilder(kryo, cluster, catalog).build();

    return new LogicalPlanSerializer() {

      @Override
      public byte[] serializeToBytes(final RelNode plan) {
        return serializer.serialize(plan);
      }

      @Override
      public String serializeToJson(RelNode plan) {
        ExplainJson jsonObj = new ExplainJson(RelOptUtil.toString(plan, SqlExplainLevel.ALL_ATTRIBUTES));
        try {
          return MAPPER.writeValueAsString(jsonObj);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }

    };
  }

  public static class ExplainJson {
    private final String textPlan;

    public ExplainJson(String textPlan) {
      super();
      this.textPlan = textPlan;
    }

    public String getTextPlan() {
      return textPlan;
    }

  }

  public static byte[] serialize(RelNode node) {
    final LogicalPlanSerializer serializer = KryoLogicalPlanSerializers.forSerialization(node.getCluster());
    return serializer.serializeToBytes(node);
  }

  /**
   * Returns a new {@link LogicalPlanDeserializer}
   * @param cluster cluster to inject during deserialization
   * @param catalog catalog used during deserializing tables
   * @param registry registry used during deserializing storage plugins
   */
  public static LogicalPlanDeserializer forDeserialization(final RelOptCluster cluster, final DremioCatalogReader catalog) {
    final Kryo kryo = new Kryo();
    kryo.getFieldSerializerConfig().setUseAsm(true);
    final KryoRelSerializer serializer = KryoRelSerializer.newBuilder(kryo, cluster, catalog).build();

    return new LogicalPlanDeserializer() {

      @Override
      public RelNode deserialize(final byte[] data) {
        try {
          return serializer.deserialize(data);
        } catch (Throwable e) {
          throw new DeserializationException(e);
        }
      }

      @Override
      public RelNode deserialize(String data) {
        throw UserException.unsupportedError().message("Kryo serializer doesn't support deserialization from JSON.").build(logger);
      }

    };
  }

}
