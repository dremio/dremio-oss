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
package com.dremio.service;

import static java.lang.reflect.Proxy.newProxyInstance;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class ServiceAutoStart {
  private ServiceAutoStart() {}

  /**
   * This adds support for auto-starting Services that are being invoked before they have been
   * started by the ServiceRegistry.
   */
  @SuppressWarnings("unchecked")
  public static <I extends Service, S extends I> I withAutoStart(Class<I> iface, S service) {
    var classLoader = Thread.currentThread().getContextClassLoader();
    return (I) newProxyInstance(classLoader, new Class<?>[] {iface}, new Handler(service));
  }

  private static final class Handler implements InvocationHandler {
    private static final Method START_METHOD;
    private static final Method CLOSE_METHOD;

    static {
      try {
        START_METHOD = Service.class.getDeclaredMethod("start");
        CLOSE_METHOD = AutoCloseable.class.getDeclaredMethod("close");
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }

    private final Object service;
    private volatile boolean started = false;

    private Handler(Object service) {
      this.service = service;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (START_METHOD.equals(method)) {
        ensureStarted();
        return null;
      } else if (CLOSE_METHOD.equals(method)) {
        if (started) {
          return method.invoke(service);
        }
        return null;
      } else {
        ensureStarted();
        return method.invoke(service, args);
      }
    }

    private void ensureStarted() throws InvocationTargetException, IllegalAccessException {
      if (!started) {
        synchronized (this) {
          if (!started) {
            started = true;
            START_METHOD.invoke(service);
          }
        }
      }
    }
  }
}
