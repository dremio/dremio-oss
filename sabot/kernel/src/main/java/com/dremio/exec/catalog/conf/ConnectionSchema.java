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
package com.dremio.exec.catalog.conf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import com.google.common.base.Throwables;

import io.protostuff.CustomSchema;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeEnv.Instantiator;
import io.protostuff.runtime.RuntimeSchema;

/**
 * A special schema for {@code ConnectionConf}
 *
 * Calls
 *
 * @param <T> the message type
 */
public class ConnectionSchema<T> extends CustomSchema<T> {

  private static final class SchemaInstantiator<T> extends Instantiator<T> {
    private final Schema<T> schema;

    private SchemaInstantiator(Schema<T> schema) {
      this.schema = schema;
    }

    @Override
    public T newInstance() { return schema.newMessage(); }
  }

  private static final class FactoryInstantiator<T> extends Instantiator<T> {
    private final Method method;

    private FactoryInstantiator(Method method) {
      this.method = method;
    }

    @Override
    public T newInstance() {
      try {
        return (T) method.invoke(null);
      } catch (IllegalAccessException e) {
        // Should not happen since we checked method was accessible
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        Throwable cause = e.getCause();
        Throwables.throwIfUnchecked(cause);
        throw new RuntimeException(cause);
      }
    }
  }

  private final Instantiator<T> instantiator;
  private ConnectionSchema(Schema<T> schema, Instantiator<T> instantiator) {
    super(schema);
    this.instantiator = instantiator;
  }

  public static <T> ConnectionSchema<T> getSchema(Class<T> typeClass) {
    final Schema<T> schema = RuntimeSchema.getSchema(typeClass);

    final Instantiator<T> instantiator = findInstantiator(typeClass, schema);
    return new ConnectionSchema<>(schema, instantiator);
  }

  private static <T> Instantiator<T> findInstantiator(Class<T> typeClass, Schema<T> schema) {
    // Look for a factory method
    try {
      final Method method = typeClass.getMethod("newMessage");
      if (!Modifier.isStatic(method.getModifiers())) {
        throw new AssertionError("ConnectionConf#newMessage should be static");
      }
      if (!Modifier.isPublic(method.getModifiers())) {
        throw new AssertionError("ConnectionConf#newMessage should be public");
      }
      if (!typeClass.isAssignableFrom(method.getReturnType())) {
        throw new AssertionError("ConnectionConf#newMessage should return " + typeClass);
      }
      return new FactoryInstantiator<>(method);
    } catch (NoSuchMethodException e) {
      // ignore
    }
    return new SchemaInstantiator<>(schema);
  }
  @Override
  public T newMessage() {
    return instantiator.newInstance();
  }
}
