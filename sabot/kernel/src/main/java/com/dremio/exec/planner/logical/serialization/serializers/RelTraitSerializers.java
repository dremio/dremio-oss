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
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.InterpretableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;

import com.dremio.exec.planner.physical.DistributionTrait;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public final class RelTraitSerializers {

  private RelTraitSerializers() { }

  public static void register(final Kryo kryo) {
    final EnumSerializer enumSerializer = new EnumSerializer();
    kryo.addDefaultSerializer(BindableConvention.class, enumSerializer);
    kryo.addDefaultSerializer(EnumerableConvention.class, enumSerializer);
    kryo.addDefaultSerializer(InterpretableConvention.class, enumSerializer);
    kryo.addDefaultSerializer(Convention.Impl.class, ConventionSerializer.class);

    kryo.addDefaultSerializer(RelDistributions.SINGLETON.getClass(), RelDistributionSerializer.class);
    kryo.addDefaultSerializer(DistributionTrait.class, DistributionTraitSerializer.class);
    kryo.addDefaultSerializer(RelCollation.class, RelCollationSerializer.class);

    kryo.addDefaultSerializer(RelTraitSet.class, RelTraitSetSerializer.class);
  }

  public static class EnumSerializer<T extends Enum<T>> extends Serializer<T> {
    @Override
    public void write(final Kryo kryo, final Output output, final T object) {
      kryo.writeObject(output, object.ordinal());
    }

    @Override
    public T read(final Kryo kryo, final Input input, final Class<T> type) {
      final int ordinal = kryo.readObject(input, Integer.class);
      return type.getEnumConstants()[ordinal];
    }
  }

  protected static abstract class NonCachingFieldSerializer<T> extends FieldSerializer<T> {
    protected NonCachingFieldSerializer(final Kryo kryo, final Class type) {
      super(kryo, type);
    }

    @Override
    public T read(final Kryo kryo, final Input input, final Class<T> type) {
      final T object = create(kryo, input, type);

      final CachedField[] fields = getFields();
      for (int i = 0, n = fields.length; i < n; i++) {
        fields[i].read(input, object);
      }

      return object;
    }
  }

  public static class ConventionSerializer<T extends Convention> extends NonCachingFieldSerializer<T> {
    public static final String NONE = "NONE";

    public ConventionSerializer(final Kryo kryo, final Class type) {
      super(kryo, type);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final T object) {
      final boolean isNone = NONE.equals(object.getName());
      kryo.writeObject(output, isNone);
      if (isNone) {
        return;
      }
      super.write(kryo, output, object);
    }

    @Override
    public T read(final Kryo kryo, final Input input, final Class<T> type) {
      final boolean isNone = kryo.readObject(input, Boolean.class);
      if (isNone) {
        return (T)Convention.NONE;
      }
      final T result = super.read(kryo, input, type);
      final T normalized = (T) result.getTraitDef().canonize(result);
      kryo.reference(normalized);
      return normalized;
    }
  }

  public static class RelDistributionSerializer<T extends RelDistribution> extends NonCachingFieldSerializer<T> {
    public static final Map<RelDistribution.Type, RelDistribution> distributionMap = ImmutableMap.
        of(
            RelDistributions.ANY.getType(), RelDistributions.ANY,
            RelDistributions.BROADCAST_DISTRIBUTED.getType(), RelDistributions.BROADCAST_DISTRIBUTED,
            RelDistributions.RANDOM_DISTRIBUTED.getType(), RelDistributions.RANDOM_DISTRIBUTED,
            RelDistributions.ROUND_ROBIN_DISTRIBUTED.getType(), RelDistributions.ROUND_ROBIN_DISTRIBUTED,
            RelDistributions.SINGLETON.getType(), RelDistributions.SINGLETON
        );

    public RelDistributionSerializer(final Kryo kryo, final Class type) {
      super(kryo, type);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final T object) {
      final boolean isKnown = distributionMap.containsKey(object.getType());
      kryo.writeObject(output, isKnown);
      if (isKnown) {
        kryo.writeObject(output, object.getType());
        return;
      }

      super.write(kryo, output, object);
    }

    @Override
    public T read(final Kryo kryo, final Input input, final Class<T> type) {
      final boolean isKnown = kryo.readObject(input, Boolean.class);
      final T result;
      if (isKnown) {
        final RelDistribution.Type kind = kryo.readObject(input, RelDistribution.Type.class);
        result = (T)distributionMap.get(kind);
      } else {
        result = super.read(kryo, input, type);
      }

      final T normalized = (T) result.getTraitDef().canonize(result);
      kryo.reference(normalized);
      return normalized;
    }
  }


  public static class DistributionTraitSerializer<T extends DistributionTrait> extends NonCachingFieldSerializer<T> {

    public static final Map<DistributionTrait.DistributionType, DistributionTrait> distributionMap = ImmutableMap.
        of(
            DistributionTrait.SINGLETON.getType(), DistributionTrait.SINGLETON,
            DistributionTrait.ANY.getType(), DistributionTrait.ANY,
            DistributionTrait.BROADCAST.getType(), DistributionTrait.BROADCAST,
            DistributionTrait.ROUND_ROBIN.getType(), DistributionTrait.ROUND_ROBIN
        );

    public DistributionTraitSerializer(final Kryo kryo, final Class type) {
      super(kryo, type);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final T object) {
      final boolean isKnown = distributionMap.containsKey(object.getType());
      kryo.writeObject(output, isKnown);
      if (isKnown) {
        kryo.writeObject(output, object.getType());
        return;
      }

      super.write(kryo, output, object);
    }

    @Override
    public T read(final Kryo kryo, final Input input, final Class<T> type) {
      final boolean isKnown = kryo.readObject(input, Boolean.class);
      final T result;
      if (isKnown) {
        final DistributionTrait.DistributionType kind = kryo.readObject(input, DistributionTrait.DistributionType.class);
        result = (T)distributionMap.get(kind);
      } else {
        result = super.read(kryo, input, type);
      }

      final T normalized = (T) result.getTraitDef().canonize(result);
      kryo.reference(normalized);
      return normalized;
    }
  }


  public static class RelCollationSerializer<T extends RelCollation> extends NonCachingFieldSerializer<T> {
    public static final Set<RelCollation> collationSet = ImmutableSet.
        of(
            RelCollations.EMPTY,
            RelCollations.PRESERVE
        );

    public RelCollationSerializer(final Kryo kryo, final Class type) {
      super(kryo, type);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final T object) {
      final boolean isKnown = collationSet.contains(object);
      kryo.writeObject(output, isKnown);
      if (isKnown) {
        kryo.writeObject(output, object.equals(RelCollations.EMPTY) ? 0 : 1);
        return;
      }

      super.write(kryo, output, object);
    }

    @Override
    public T read(final Kryo kryo, final Input input, final Class<T> type) {
      final boolean isKnown = kryo.readObject(input, Boolean.class);
      final T result;
      if (isKnown) {
        final Integer pos = kryo.readObject(input, Integer.class);
        result = (T) (pos == 0 ? RelCollations.EMPTY:RelCollations.PRESERVE);
      } else {
        result = super.read(kryo, input, type);
      }

      final T normalized = (T) result.getTraitDef().canonize(result);
      kryo.reference(normalized);
      return normalized;
    }
  }


  public static class RelTraitSetSerializer extends Serializer<RelTraitSet> {
    private static final String NAME_CACHE = "cache";
    private static final String NAME_TRAITS = "traits";
    private static final String NAME_STRING = "string";


    @Override
    public void write(final Kryo kryo, final Output output, final RelTraitSet traitSet) {
      final int size = traitSet.size();
      kryo.writeObject(output, size);

      for (int i = 0; i < size; i++) {
        final RelTrait trait = traitSet.getTrait(i);
        kryo.writeClassAndObject(output, trait);
      }
      kryo.writeObject(output, traitSet.toString());

      try {
        final Object cache = getField(NAME_CACHE).get(traitSet);
        kryo.writeClassAndObject(output, cache);
      } catch (final NoSuchFieldException|IllegalAccessException e) {
        throw new RuntimeException("unable to read TraitSet", e);
      }
    }

    @Override
    public RelTraitSet read(final Kryo kryo, final Input input, final Class<RelTraitSet> type) {

      final int size = kryo.readObject(input, Integer.class);
      final RelTrait[] traits = new RelTrait[size];
      for (int i = 0; i < size; i++) {
        final RelTrait trait = (RelTrait) kryo.readClassAndObject(input);
        // normalize trait so that stupid calcite won't complain about == checks.
        traits[i] = trait.getTraitDef().canonize(trait);
      }

      final String digest = kryo.readObject(input, String.class);
      final Object cache = kryo.readClassAndObject(input);

      final RelTraitSet traitSet = kryo.newInstance(type);

      try {
        getField(NAME_CACHE).set(traitSet, cache);
        getField(NAME_TRAITS).set(traitSet, traits);
        getField(NAME_STRING).set(traitSet, digest);
      } catch (final NoSuchFieldException|IllegalAccessException e) {
        throw new RuntimeException("unable to deserialize TraitSet", e);
      }

      kryo.reference(traitSet);
      return traitSet;
    }

    public Field getField(final String fieldName) throws NoSuchFieldException {
      final Field field = RelTraitSet.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field;
    }
  }


}
