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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.util.ImmutableNullableList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;

/**
 * Majority of these are adopted from https://github.com/magro/kryo-serializers
 */
public final class ImmutableCollectionSerializers {
  private ImmutableCollectionSerializers() {
  }

  public static void register(final Kryo kryo) {
    // register list
    ImmutableListSerializer.register(kryo);
    // register set
    ImmutableSetSerializer.register(kryo);
    // register set
    ImmutableMapSerializer.register(kryo);
    // others
    kryo.addDefaultSerializer(FlatLists.AbstractFlatList.class, FieldSerializer.class);
    kryo.addDefaultSerializer(ImmutableNullableList.class, ImmutableNullableListSerializer.class);
  }

  public static final class ImmutableListSerializer extends Serializer<ImmutableList<Object>> {
    private static final boolean ACCEPTS_NULL = false;
    private static final boolean IMMUTABLE = true;

    public ImmutableListSerializer() {
      super(ACCEPTS_NULL, IMMUTABLE);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final ImmutableList<Object> object) {
      output.writeInt(object.size(), true);
      final UnmodifiableIterator iterator = object.iterator();

      while (iterator.hasNext()) {
        final Object value = iterator.next();
        kryo.writeClassAndObject(output, value);
      }
    }

    @Override
    public ImmutableList<Object> read(final Kryo kryo, final Input input, final Class<ImmutableList<Object>> type) {
      final int size = input.readInt(true);
      final Object[] values = new Object[size];

      for (int i = 0; i < size; ++i) {
        values[i] = kryo.readClassAndObject(input);
      }

      final ImmutableList<Object> result = ImmutableList.copyOf(values);
      kryo.reference(result);
      return result;
    }

    public static void register(final Kryo kryo) {
      // register list
      final ImmutableListSerializer serializer = new ImmutableListSerializer();
      kryo.register(ImmutableList.class, serializer);
      kryo.register(ImmutableList.of().getClass(), serializer);
      kryo.register(ImmutableList.of(Integer.valueOf(1)).getClass(), serializer);
      kryo.register(ImmutableList.of(Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(3)).subList(1, 2).getClass(), serializer);
      kryo.register(ImmutableList.of().reverse().getClass(), serializer);
      kryo.register(Lists.charactersOf("dremio").getClass(), serializer);

      final HashBasedTable baseTable = HashBasedTable.create();
      baseTable.put(Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(3));
      baseTable.put(Integer.valueOf(4), Integer.valueOf(5), Integer.valueOf(6));
      ImmutableTable table = ImmutableTable.copyOf(baseTable);
      kryo.register(table.values().getClass(), serializer);
    }
  }


  public static final class ImmutableSetSerializer extends Serializer<ImmutableSet<Object>> {
    private static final boolean ACCEPTS_NULL = false;
    private static final boolean IMMUTABLE = true;

    public ImmutableSetSerializer() {
      super(ACCEPTS_NULL, IMMUTABLE);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final ImmutableSet<Object> object) {
      output.writeInt(object.size(), true);
      final UnmodifiableIterator iterator = object.iterator();

      while (iterator.hasNext()) {
        final Object value = iterator.next();
        kryo.writeClassAndObject(output, value);
      }
    }

    @Override
    public ImmutableSet<Object> read(final Kryo kryo, final Input input, final Class<ImmutableSet<Object>> type) {
      final int size = input.readInt(true);

      final ImmutableSet.Builder builder = ImmutableSet.builder();

      for (int i = 0; i < size; ++i) {
        builder.add(kryo.readClassAndObject(input));
      }

      final ImmutableSet<Object> result = builder.build();
      kryo.reference(result);
      return result;
    }

    public static void register(final Kryo kryo) {
      final ImmutableSetSerializer setSerializer = new ImmutableSetSerializer();
      kryo.register(ImmutableSet.class, setSerializer);
      kryo.register(ImmutableSet.of().getClass(), setSerializer);
      kryo.register(ImmutableSet.of(Integer.valueOf(1)).getClass(), setSerializer);
      kryo.register(ImmutableSet.of(Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(3)).getClass(), setSerializer);
      kryo.register(Sets.immutableEnumSet(SomeEnum.A, new SomeEnum[]{SomeEnum.B, SomeEnum.C}).getClass(), setSerializer);
    }

  }


  public static class ImmutableMapSerializer extends Serializer<ImmutableMap<Object, ? extends Object>> {

    private static final boolean ACCEPTS_NULL = true;
    private static final boolean IMMUTABLE = true;

    public ImmutableMapSerializer() {
      super(ACCEPTS_NULL, IMMUTABLE);
    }

    @Override
    public void write(Kryo kryo, Output output, ImmutableMap<Object, ? extends Object> immutableMap) {
      kryo.writeObject(output, Maps.newHashMap(immutableMap));
    }

    @Override
    public ImmutableMap<Object, Object> read(Kryo kryo, Input input, Class<ImmutableMap<Object, ? extends Object>> type) {
      Map map = kryo.readObject(input, HashMap.class);
      final ImmutableMap<Object, Object> result = ImmutableMap.copyOf(map);
      kryo.reference(result);
      return result;
    }

    /**
     * Creates a new {@link ImmutableMapSerializer} and registers its serializer
     * for the several ImmutableMap related classes.
     *
     * @param kryo the {@link Kryo} instance to set the serializer on
     */
    public static void register(final Kryo kryo) {
      final ImmutableMapSerializer serializer = new ImmutableMapSerializer();
      kryo.register(ImmutableMap.class, serializer);
      kryo.register(ImmutableMap.of().getClass(), serializer);

      final Object o1 = new Object();
      final Object o2 = new Object();

      kryo.register(ImmutableMap.of(o1, o1).getClass(), serializer);
      kryo.register(ImmutableMap.of(o1, o1, o2, o2).getClass(), serializer);

      final Map<SomeEnum,Object> enumMap = new EnumMap<>(SomeEnum.class);
      for (final SomeEnum e : SomeEnum.values()) {
        enumMap.put(e, o1);
      }

      kryo.register(ImmutableMap.copyOf(enumMap).getClass(), serializer);
    }

  }

  /**
   * Need to use List instead of ImmutableNullableList because ImmutableNullableList.copyOf() can return ImmutableList.
   */
  public class ImmutableNullableListSerializer extends Serializer<List> {
    { setImmutable(true); }

    @Override
    public void write(Kryo kryo, Output output, List object) {
      Preconditions.checkArgument(object instanceof ImmutableNullableList);
      output.writeInt(object.size());

      final Iterator iterator = object.iterator();
      while (iterator.hasNext()) {
        kryo.writeClassAndObject(output, iterator.next());
      }
    }

    @Override
    public List read(Kryo kryo, Input input, Class<List> type) {
      final int size = input.readInt();

      Object[] elements = new Object[size];
      for (int i = 0; i < size; ++i) {
        elements[i] = kryo.readClassAndObject(input);
      }
      return ImmutableNullableList.copyOf(elements);
    }
  }

  private enum SomeEnum {
    A, B, C
  }

}
