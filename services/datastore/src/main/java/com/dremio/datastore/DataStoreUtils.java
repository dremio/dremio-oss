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
package com.dremio.datastore;

import com.dremio.common.SuppressForbidden;
import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/** Utilities for datastore and other applications */
@SuppressForbidden
public final class DataStoreUtils {

  /**
   * Convert a class name to an instance.
   *
   * @param className The class name to load.
   * @param clazz The expected class type.
   * @param failOnEmpty Whether to fail or return null if no value is given.
   * @return The newly created instance (or null if !failOnEmpty and emptry string used).
   */
  @SuppressWarnings("unchecked")
  public static <T> T getInstance(String className, Class<T> clazz, boolean failOnEmpty) {
    if (className == null || className.isEmpty()) {
      if (failOnEmpty) {
        throw new DatastoreException(
            String.format(
                "Failure trying to resolve class for expected type of %s. The provided class name was either empty or null.",
                clazz.getName()));
      }

      return null;
    } else {
      try {
        final Class<?> outcome = Class.forName(Preconditions.checkNotNull(className));
        Preconditions.checkArgument(clazz.isAssignableFrom(outcome));
        return (T) getInstance(outcome);
      } catch (Exception ex) {
        throw new DatastoreException(
            String.format(
                "Failure while trying to load class named %s which should be a subclass of %s. ",
                className, clazz.getName()));
      }
    }
  }

  private DataStoreUtils() {}

  /**
   * Makes a constructor accessible so classes are instantiable.
   *
   * @param clazz to instantiate
   * @param <T> type to instantiate
   * @return instance
   * @throws DatastoreException if constructor cannot be invoked.
   */
  @SuppressWarnings("unchecked")
  public static <T> T getInstance(Class<T> clazz) throws DatastoreException {
    try {
      Constructor<?> constructor = clazz.getDeclaredConstructor();
      constructor.setAccessible(true);
      return (T) constructor.newInstance();
    } catch (NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw new DatastoreException("Could not get instance of class: " + clazz.getName(), e);
    }
  }
}
