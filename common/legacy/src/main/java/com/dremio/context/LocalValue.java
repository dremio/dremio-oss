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
package com.dremio.context;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;

/**
 * Thread local values that are tracked and can be easily transferred to other threads and restored.
 *
 * Values should be immutable since the same instance can exist in multiple threads (transferring them to another
 * thread for example).  All values are stored in one ThreadLocal as an {@link LocalValues}, which can be saved and
 * restored cheaply.
 *
 * @param <T> The type of the value being stored.  The type should be immutable.
 */
public class LocalValue<T> {
  /*
    All values are stored in one ThreadLocal as an {@link LocalValues}.  LocalValues stores each LocalValue in an array,
    which is why for each {@link LocalValue}, an auto incrementing index is generated during creation which represents
    its position in the array.  The position is the same across all threads, and if unset will be null.

    Saving/restoring values becomes as simple as getting/setting a {@link LocalValues}, which holds all the actual values
    This allows saving/restoring to be cheap and code should always optimize for that.
  */
  private static final ThreadLocal<LocalValuesImpl> VALUES = new ThreadLocal<>();
  private static final AtomicInteger SIZE = new AtomicInteger();

  private final int index;

  public LocalValue() {
    index = SIZE.getAndIncrement();
  }

  /**
   * Returns the LocalValue's stored value.
   *
   * @return Optional of T
   */
  @SuppressWarnings("unchecked")
  public Optional<T> get() {
    final LocalValuesImpl values = VALUES.get();

    if (values == null || values.size() <= index) {
      return Optional.empty();
    }

    return Optional.ofNullable((T) values.get(index));
  }

  /**
   * Sets the LocalValue's value.
   *
   * @param value the value to store
   */
  public void set(T value) {
    Preconditions.checkNotNull(value, "LocalValue cannot be set to null");
    doSet(value);
  }

  private void doSet(T value) {
    final LocalValuesImpl values = doSave();
    final LocalValuesImpl newValues = values.set(index, value);
    VALUES.set(newValues);
  }

  /**
   * Clears out the value.
   */
  public void clear() {
    doSet(null);
  }

  /**
   * Returns the currently stored values as a LocalValues.  Used to restore a previous state.
   *
   * @return the stored LocalValues
   */
  public static LocalValues save() {
    return doSave();
  }

  private static LocalValuesImpl doSave() {
    return Optional.ofNullable(VALUES.get()).orElseGet(() -> new LocalValuesImpl(SIZE.get()));
  }

  /**
   * Restores a previously saved LocalValues state.
   *
   * @param localValues saved LocalValues to restore
   */
  public static void restore(LocalValues localValues) {
    Preconditions.checkNotNull(localValues, "Cannot restore a null value");

    VALUES.set((LocalValuesImpl) localValues);
  }

  /**
   * Local Values Implementation
   */
  protected static final class LocalValuesImpl implements LocalValues {
    private final Object[] values;

    private LocalValuesImpl(int size) {
      this.values = new Object[size];
    }

    public LocalValuesImpl(Object[] values) {
      this.values = values;
    }

    public Object get(int index) {
      return values[index];
    }

    public Object[] get() {
      return values;
    }

    public int size() {
      return values.length;
    }

    private LocalValuesImpl set(int index, Object value) {
      final int size = SIZE.get();
      Preconditions.checkArgument(index < size, "index out of bounds");

      if (index < this.values.length && this.values[index] == value) {
        // Fast-path: no copy needed as the references are identical
        return this;
      }

      final Object[] newValues = new Object[size];
      System.arraycopy(values, 0, newValues, 0, values.length);

      newValues[index] = value;

      return new LocalValuesImpl(newValues);
    }
  }
}
