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
package com.dremio.exec.planner.acceleration;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.planner.logical.serialization.RelSerializer;
import com.esotericsoftware.kryo.Kryo;

/**
 * A convenience class used to create Kryo based logical plan de/serializers.
 */
public final class KryoLogicalPlanSerializers {

  public static class KryoDeserializationException extends RuntimeException {
    public KryoDeserializationException(Throwable cause) {
      super(cause);
    }
  }

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
    final RelSerializer serializer = RelSerializer.newBuilder(kryo, cluster, catalog).build();

    return new LogicalPlanSerializer() {

      @Override
      public byte[] serialize(final RelNode plan) {
        return serializer.serialize(plan);
      }

    };
  }

  public static byte[] serialize(RelNode node) {
    final LogicalPlanSerializer serializer = KryoLogicalPlanSerializers.forSerialization(node.getCluster());
    return serializer.serialize(node);
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
    final RelSerializer serializer = RelSerializer.newBuilder(kryo, cluster, catalog).build();

    return new LogicalPlanDeserializer() {

      @Override
      public RelNode deserialize(final byte[] data) {
        try {
          return serializer.deserialize(data);
        } catch (Throwable e) {
          throw new KryoDeserializationException(e);
        }
      }

    };
  }

}
