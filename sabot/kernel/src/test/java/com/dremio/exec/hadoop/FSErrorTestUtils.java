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
package com.dremio.exec.hadoop;

import com.dremio.common.SuppressForbidden;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.Path;
import com.google.common.base.Throwables;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.hadoop.fs.FSError;

/** Some helper methods to test FSError handling */
@SuppressForbidden
public final class FSErrorTestUtils {
  private FSErrorTestUtils() {}

  private static final MethodHandle FSERROR_NEW_INSTANCE_HANDLE;

  static {
    try {
      Constructor<FSError> constructor = FSError.class.getDeclaredConstructor(Throwable.class);
      constructor.setAccessible(true);
      FSERROR_NEW_INSTANCE_HANDLE = MethodHandles.lookup().unreflectConstructor(constructor);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a new {@code FSError} through reflection
   *
   * @param t
   * @return
   */
  public static FSError newFSError(Throwable t) {
    // Thanks Hadoop for making it so difficult!
    try {
      return (FSError) FSERROR_NEW_INSTANCE_HANDLE.invokeExact(t);
    } catch (Throwable e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Create dummy arguments for invoking the provided method
   *
   * @param method
   * @return
   */
  public static Object[] getDummyArguments(Method method) {
    final Class<?>[] parameterTypes = method.getParameterTypes();
    Object[] params = new Object[parameterTypes.length];
    for (int i = 0; i < params.length; i++) {
      final Class<?> parameterType = parameterTypes[i];
      if (!parameterType.isPrimitive()) {
        if (parameterType == Set.class) {
          params[i] = Collections.emptySet();
        } else if (parameterType == Predicate.class) {
          params[i] = (Predicate<?>) x -> true;
        } else if (parameterType == byte[].class) {
          params[i] = new byte[] {};
        } else if (parameterType == FileAttributes.class) {
          params[i] = new FileStatusWrapper(null);
        } else if (parameterType == Path.class) {
          params[i] = Path.of(".");
        } else if (parameterType == OutputStream.class) {
          params[i] = new ByteArrayOutputStream();
        } else {
          params[i] = null;
        }
        continue;
      }

      if (parameterType == boolean.class) {
        params[i] = false;
      } else if (parameterType == byte.class) {
        params[i] = (byte) 0;
      } else if (parameterType == char.class) {
        params[i] = (char) 0;
      } else if (parameterType == short.class) {
        params[i] = (short) 0;
      } else if (parameterType == int.class) {
        params[i] = 0;
      } else if (parameterType == long.class) {
        params[i] = 0L;
      } else if (parameterType == float.class) {
        params[i] = 0f;
      } else if (parameterType == double.class) {
        params[i] = 0d;
      }
    }

    return params;
  }
}
