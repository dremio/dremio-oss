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
package com.dremio.common;

import static com.google.common.base.Throwables.throwIfUnchecked;

import com.dremio.service.Service;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Binding;
import com.google.inject.Scopes;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.ProvisionListener.ProvisionInvocation;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles Guice registered services, allowing to start/stop them in proper order */
public class GuiceServiceModule extends AbstractModule {
  private static final Logger logger = LoggerFactory.getLogger(GuiceServiceModule.class);

  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final Deque<Service> startedServices = new ArrayDeque<>();

  public GuiceServiceModule() {}

  @Override
  protected void configure() {
    binder().bindListener(Matchers.any(), this::onProvisionInstance);
    // only use explicitly annotated constructors
    binder().requireAtInjectOnConstructors();
    // increase required matching accuracy
    binder().requireExactBindingAnnotations();
    // force to design without cycles to keep things simple
    binder().disableCircularProxies();
    // TODO: should modules be required to explicitly mention all types they are injecting?
    // binder().requireExplicitBindings();
  }

  private <T> void onProvisionInstance(ProvisionInvocation<T> provision) {
    final Binding<T> binding = provision.getBinding();
    Class<? super T> targetType = binding.getKey().getTypeLiteral().getRawType();
    if (isClosed.get()) {
      throw new IllegalStateException("Unable to provision after close: " + targetType);
    }
    final T object = provision.provision();
    if (!(Service.class.isInstance(object))) {
      return;
    }
    if (!Scopes.isSingleton(binding)) {
      // TODO: why do we have Factory services that are not singletons? who calls start/close?
      if (targetType.getSimpleName().endsWith("Factory")) {
        logger.debug(
            "Provisioned Service is not a Singleton and will not be started: {} (for {})",
            object,
            targetType);
        return;
      }
      throw new IllegalStateException(
          "Missing Singleton scope for service: " + object + " (for " + targetType + ")");
    }
    Service service = Service.class.cast(object);
    try {
      logger.info("Starting Service Singleton: {} (for {})", service, targetType);
      service.start();
      synchronized (startedServices) {
        startedServices.push(service);
      }
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Closes all registered services
   *
   * @throws Exception
   */
  public void close() throws Exception {
    if (isClosed.compareAndSet(false, true)) {
      synchronized (startedServices) {
        try (DeferredException deferred = new DeferredException()) {
          startedServices.forEach(
              service -> deferred.suppressingClose(() -> closeServiceSingleton(service)));
        }
      }
    }
  }

  private void closeServiceSingleton(Service service) {
    try {
      logger.info("Stopping Service Singleton: {}", service);
      service.close();
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  protected List<Service> getServiceList() {
    return new ArrayList<>(startedServices);
  }
}
