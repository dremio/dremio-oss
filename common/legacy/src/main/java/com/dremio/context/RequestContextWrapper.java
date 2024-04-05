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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.stream.Stream;
import org.apache.commons.lang3.ClassUtils;

/**
 * Wraps the RequestContext against every call on the object. The output objects actions are also
 * wrapped with the context.
 *
 * <p>
 *
 * <p>CAUTIONS:
 *
 * <p>(a) The wrapped calls can incur significant cost. <br>
 * (b) Proxy stacks up in the returned objects. Avoid using it in classes, which return "this"
 * instance.
 *
 * <p>(c) Avoid putting in multiple layers of proxies to the same object. (d) Designed to be used
 * only with simple API interfaces. (e) The proxy applies only the interfaces class types. The
 * sub-object wrapping automatically ignores applying the context if returned object isn't an
 * interface. (f) Collections and arrays aren't wrapped in the response. (g) May not cover all
 * types. Thoroughly test the compatibility before consuming.
 */
public final class RequestContextWrapper<T> implements InvocationHandler {
  private final RequestContext requestContext;
  private final T baseObj;

  private RequestContextWrapper(RequestContext requestContext, T baseObj) {
    this.requestContext = requestContext;
    this.baseObj = baseObj;
  }

  @Override
  public Object invoke(Object proxyObj, Method method, Object[] args) throws Throwable {
    try {
      Class<?> methodReturnType = method.getReturnType();
      Object returnedObj =
          Stream.class.equals(method.getReturnType())
              ? requestContext.callStream(() -> (Stream) method.invoke(baseObj, args))
              : requestContext.call(() -> method.invoke(baseObj, args));

      if (returnedObj != null) {
        Class<?> returnType = returnedObj.getClass();

        // method.getReturnType() is not reliable because the object's return type could be an
        // extended class.
        if (Void.class.equals(returnType)
            || returnType.isPrimitive()
            || returnType.isArray()
            || Collection.class.isAssignableFrom(returnType)
            || proxyObj == returnedObj) {
          return returnedObj;
        }

        returnType =
            ClassUtils.getAllInterfaces(returnType).stream()
                .filter(intfc -> methodReturnType.isAssignableFrom(intfc))
                .findAny()
                .orElse(returnType);

        if (returnType.isInterface()) {
          returnedObj = wrapWithContext(requestContext, returnedObj, returnType);
        }
      }
      return returnedObj;
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T wrapWithContext(
      RequestContext context, T baseObject, Class<?> interfaceType) {
    RequestContextWrapper<T> handler = new RequestContextWrapper(context, baseObject);
    return (T)
        Proxy.newProxyInstance(
            interfaceType.getClassLoader(), new Class<?>[] {interfaceType}, handler);
  }
}
