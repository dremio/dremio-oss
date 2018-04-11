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
package com.dremio.exec.planner.logical.serialization.serializers;


import java.lang.reflect.Field;
import java.util.Map;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.InterpretableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.poi.ss.formula.functions.T;

import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DistributionTraitDef;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;

public final class RelTraitDefSerializers {

  private RelTraitDefSerializers() { }

  public static void register(final Kryo kryo) {
    kryo.addDefaultSerializer(RelCollationTraitDef.class, SingletonSerializer.of(Suppliers.ofInstance(RelCollationTraitDef.INSTANCE)));
    kryo.addDefaultSerializer(DistributionTraitDef.class, SingletonSerializer.of(Suppliers.ofInstance(DistributionTraitDef.INSTANCE)));
    kryo.addDefaultSerializer(RelDistributionTraitDef.class, SingletonSerializer.of(Suppliers.ofInstance(RelDistributionTraitDef.INSTANCE)));
    kryo.addDefaultSerializer(ConventionTraitDef.class, SingletonSerializer.of(Suppliers.ofInstance(ConventionTraitDef.INSTANCE)));
  }

  private static class SingletonSerializer<T extends RelTraitDef> extends Serializer<T> {

    private final Supplier<T> factory;

    public SingletonSerializer(final Supplier<T> factory) {
      this.factory = factory;
    }

    @Override
    public void write(final Kryo kryo, final Output output, final T object) {}

    @Override
    public T read(final Kryo kryo, final Input input, final Class<T> type) {
      final T result = factory.get();
      kryo.reference(result);
      return result;
    }

    public static  <T extends RelTraitDef> SingletonSerializer<T> of(final Supplier<T> factory) {
      return new SingletonSerializer<>(factory);
    }
  }

}
