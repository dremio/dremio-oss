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
package com.dremio.test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.junit.rules.ExternalResource;

import com.dremio.common.SuppressForbidden;

/**
 * The TemporaryEnvironment rule allows editing of system environment variables.
 * Unsafe: the implementation violates the JVM contract that the environment is immutable.
 * Automatically restores original environment state on cleanup.
 */
public class TemporaryEnvironment extends ExternalResource {
  private final static Map<String, String> WRITABLE_ENVIRONMENT = getWritableEnvironment();
  private final static Map<String, String> ORIGINAL_ENVIRONMENT = new HashMap<>(WRITABLE_ENVIRONMENT);

  @SuppressForbidden
  private static Map<String, String> getWritableEnvironment() {
    final Map<String, String> env = System.getenv();
    final Class<?> cl = env.getClass();
    try {
      final Field field = cl.getDeclaredField("m");
      field.setAccessible(true);
      return (Map<String, String>) field.get(env);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException("Failed to get writable environment.", e);
    }
  }

  @Override
  protected void after() {
    WRITABLE_ENVIRONMENT.clear();
    WRITABLE_ENVIRONMENT.putAll(ORIGINAL_ENVIRONMENT);
  }

  public void setEnvironmentVariable(String key, String value) {
    if (value == null) {
      WRITABLE_ENVIRONMENT.remove(key);
    } else {
      WRITABLE_ENVIRONMENT.put(key, value);
    }
  }
}
