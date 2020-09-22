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
package com.dremio.datastore.generator;

import java.util.List;
import java.util.function.Supplier;

import com.dremio.datastore.generator.supplier.DoubleSupplier;
import com.dremio.datastore.generator.supplier.FloatSupplier;
import com.dremio.datastore.generator.supplier.IntegerSupplier;
import com.dremio.datastore.generator.supplier.LongSupplier;
import com.dremio.datastore.generator.supplier.fixed.FixedLengthStringSupplier;
import com.google.common.collect.ImmutableList;

/**
 * Provides unique values for all types in dummy.proto
 */
public class ProtoGeneratorMixin {
  private final Supplier<String> stringProvider = new FixedLengthStringSupplier("Str1ng-@proto-value#!=V");
  private final Supplier<Long> longProvider = new LongSupplier();
  private final Supplier<Integer> intProvider = new IntegerSupplier();
  private final Supplier<Float> floatProvider = new FloatSupplier();
  private final Supplier<Double> doubleProvider = new DoubleSupplier();

  private boolean flag = true;

  public boolean getBool() {
    final boolean ret = flag;
    flag = !flag;
    return ret;
  }

  public int getInt32() {
    return intProvider.get();
  }

  public long getInt64() {
    return longProvider.get();
  }

  public float getFloat() {
    return floatProvider.get();
  }

  public double getDouble() {
    return doubleProvider.get();
  }

  public String getString() {
    return stringProvider.get();
  }

  public List<Integer> getInt32List() {
    return ImmutableList.of(0, getInt32(), getInt32(), getInt32(), Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  public List<Long> getInt64List() {
    return ImmutableList.of(0L, getInt64(), getInt64(), getInt64(), Long.MIN_VALUE, Long.MAX_VALUE);
  }

  public List<Float> getFloatList() {
    return ImmutableList.of(0F, getFloat(), getFloat(), getFloat(), Float.MIN_VALUE, Float.MAX_VALUE,  Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY);
  }

  public List<Double> getDoubleList() {
    return ImmutableList.of(0D, getDouble(), getDouble(), getDouble(), Double.MIN_VALUE, Double.MAX_VALUE,  Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
  }
}
