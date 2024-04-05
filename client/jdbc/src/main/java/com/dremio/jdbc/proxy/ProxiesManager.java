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
package com.dremio.jdbc.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Manager of proxy classes and instances. Managing includes creating proxies and tracking to re-use
 * proxies.
 */
class ProxiesManager {
  private static final class ProxyKey<INTF> {
    private final Class<INTF> declaredType;
    private final INTF originalInstance;

    private ProxyKey(Class<INTF> declaredType, INTF originalInstance) {
      this.declaredType = declaredType;
      this.originalInstance = originalInstance;
    }

    @Override
    public int hashCode() {
      return declaredType.hashCode() + 31 * System.identityHashCode(originalInstance);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ProxyKey)) {
        return false;
      }
      ProxyKey<?> that = (ProxyKey<?>) obj;
      return Objects.equals(this.declaredType, that.declaredType)
          &&
          // Identity comparison instead of equality as it is used for identity hashmap
          this.originalInstance == that.originalInstance;
    }
  }

  private final InvocationReporter reporter;

  /**
   * Map of proxied original objects from proxied JDBC driver to proxy objects to be returned by
   * tracing proxy driver.
   */
  private Map<ProxyKey<?>, Object> proxiedsToProxiesMap = new HashMap<>();

  public ProxiesManager(final InvocationReporter reporter) {
    this.reporter = reporter;
  }

  /**
   * Creates or retrieves proxy instance to be returned for given original instance.
   *
   * @param originalInstance the original object
   * @param declaredType the declared type of source of the original object; interface type
   */
  public <INTF> INTF getProxyInstanceForOriginal(
      final INTF originalInstance, final Class<INTF> declaredType) {
    // Suppressed because checked immediately above.
    ProxyKey<INTF> key = new ProxyKey<>(declaredType, originalInstance);
    @SuppressWarnings("unchecked")
    final INTF proxy = (INTF) proxiedsToProxiesMap.computeIfAbsent(key, this::newProxyInstance);
    return proxy;
  }

  private final <INTF> INTF newProxyInstance(final ProxyKey<INTF> proxyKey) {
    // Create tracing handler instance for this proxy/original pair.
    Class<INTF> declaredType = proxyKey.declaredType;

    final InvocationHandler callHandler =
        new TracingInvocationHandler<>(this, reporter, proxyKey.originalInstance, declaredType);
    try {
      // Suppressed because we know that proxy class implements INTF.
      @SuppressWarnings("unchecked")
      final INTF proxyInstance =
          (INTF)
              Proxy.newProxyInstance(
                  declaredType.getClassLoader(), new Class<?>[] {declaredType}, callHandler);
      return proxyInstance;
    } catch (IllegalArgumentException | SecurityException e) {
      throw new RuntimeException("Error creating proxy for " + declaredType + ": " + e, e);
    }
  }
} // class ProxiesManager
