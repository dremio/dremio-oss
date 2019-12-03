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
package com.dremio.exec.planner.serialization.kryo.serializers;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.expr.fn.hll.HyperLogLog;
import com.dremio.exec.expr.fn.impl.GeoFunctions;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;

/**
 * A serializer for {@link SqlOperator} subclasses.
 *
 * The serialize is capable of operating on two kinds of operator tables:
 *  (i) Calcite's SqlStdOperatorTable
 *  (ii) Dremio's function registry
 *
 * (i) requires special handling as calcite relies on singleton operators.
 * (ii) is handled via regular reflective serialization.
 *
 * For (i) we populate existing SqlOperators, maintain a duplex mapping from their identity to a deterministic ordinal.
 * During serialization we write this ordinal and we do it opposite way for deserialization.
 *
 * @param <T> operator type to serialize
 */
public class SqlOperatorSerializer<T extends SqlOperator> extends FieldSerializer<T> {
  private static final Logger logger = LoggerFactory.getLogger(SqlOperatorSerializer.class);

  public SqlOperatorSerializer(final Kryo kryo, final Class<T> type) {
    super(kryo, type);
  }

  private static final class OperatorPopulator {
    // mapping from operator pointer to its ordinal
    private static final Map<Integer, Integer> FORWARD = Maps.newHashMap();
    // mapping from operator's ordinal to its instance
    private static final Map<Integer, SqlOperator> BACKWARD = Maps.newHashMap();

    static {
      try {
        populate(SqlStdOperatorTable.instance());
      } catch (final Exception ex) {
        logger.error("unable to populate operator table", ex);
      }
    }

    /**
     * Populates and create mappings for all fields of the given operator table.
     *
     * Fields of interest are public, static and subclasses {@link SqlOperator}.
     *
     * @param table operator table instance
     */
    private static void populate(final Object table) {
      final List<Field> operators = FluentIterable
          .from(Arrays.asList(table.getClass().getDeclaredFields()))
          .filter(new Predicate<Field>() {
            @Nullable
            @Override
            public boolean apply(@Nullable final Field field) {
              final int modifiers = field.getModifiers();
              final Class type = field.getType();
              return Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers) && SqlOperator.class.isAssignableFrom(type);
            }
          })
          .toList();

      for (final Field operatorField:operators) {
        try {
          put((SqlOperator) operatorField.get(table));
        } catch (final IllegalAccessException ex) {
          logger.warn("unable to retrieve sql operator from table", ex);
        }
      }

      put(HyperLogLog.HLL);
      put(HyperLogLog.HLL_DECODE);
      put(HyperLogLog.HLL_MERGE);
      put(HyperLogLog.NDV);
      put(GeoFunctions.GEO_DISTANCE);
      put(GeoFunctions.GEO_NEARBY);
      put(GeoFunctions.GEO_BEYOND);

    }

    private static final void put(SqlOperator operator) {

      final int identity = System.identityHashCode(operator);
      final Integer ordinal = FORWARD.get(identity);
      if (ordinal != null) {
        final SqlOperator existing = BACKWARD.get(ordinal);
        throw new IllegalStateException(String.format("there are colliding operators %s <-> %s", existing, operator));
      }

      FORWARD.put(identity, FORWARD.size());
      BACKWARD.put(BACKWARD.size(), operator);

    }

  }

  @Override
  public void write(final Kryo kryo, final Output output, final T operator) {
    final int identity = System.identityHashCode(operator);
    final Integer ordinal = OperatorPopulator.FORWARD.get(identity);
    final boolean isKnown = ordinal != null;
    kryo.writeObject(output, isKnown);
    if (isKnown) {
      kryo.writeObject(output, ordinal);
      return;
    }

    super.write(kryo, output, operator);
  }

  @Override
  public T read(final Kryo kryo, final Input input, final Class<T> type) {
    final boolean isKnown = kryo.readObject(input, Boolean.class);
    if (isKnown) {
      final Integer ordinal = kryo.readObject(input, Integer.class);
      final SqlOperator operator = OperatorPopulator.BACKWARD.get(ordinal);
      if (operator != null) {
        kryo.reference(operator);
        return (T)operator;
      }
      throw new IllegalStateException(String.format("Unable to locate operator with ordinal [%s]", ordinal));
    }

    return super.read(kryo, input, type);
  }

  /**
   * Creates an instance ensuring {@link SqlOperator operators} from {@link SqlStdOperatorTable} are prepopulated.
   */
  public static SqlOperatorSerializer of(final Kryo kryo, final Class<?> type) {
    final SqlOperatorSerializer serializer = new SqlOperatorSerializer(kryo, type);
    Preconditions.checkState(!OperatorPopulator.FORWARD.isEmpty(), "operator table cannot be empty");
    return serializer;
  }
}
