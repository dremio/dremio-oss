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
import com.google.inject.Injector;
import com.google.inject.Scope;
import com.google.inject.internal.SingletonScope;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.DefaultBindingScopingVisitor;
import com.google.inject.spi.ProvisionListener;
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

  private final Deque<Class<?>> serviceList = new ArrayDeque<>();

  public GuiceServiceModule() {}

  @Override
  protected void configure() {
    binder()
        .bindListener(
            Matchers.any(),
            new ProvisionListener() {
              @Override
              public <T> void onProvision(ProvisionInvocation<T> provision) {
                final Binding<T> binding = provision.getBinding();
                logger.debug("provisioning {}", binding.getKey().getTypeLiteral());

                final T provisioned = provision.provision();

                if (provisioned != null && Service.class.isAssignableFrom(provisioned.getClass())) {
                  final AtomicBoolean start = new AtomicBoolean(false);
                  binding.acceptScopingVisitor(
                      new DefaultBindingScopingVisitor<T>() {
                        @Override
                        public T visitEagerSingleton() {
                          start.set(true);
                          return super.visitEagerSingleton();
                        }

                        @Override
                        public T visitScope(Scope scope) {
                          if (scope instanceof SingletonScope) {
                            start.set(true);
                          }
                          return super.visitScope(scope);
                        }
                      });

                  if (start.get()) {
                    serviceList.push(binding.getKey().getTypeLiteral().getRawType());
                    try {
                      logger.debug("starting {}", binding.getKey().getTypeLiteral());
                      ((Service) provisioned).start();
                    } catch (Exception e) {
                      throwIfUnchecked(e);
                      throw new RuntimeException(e);
                    }
                  }
                }
              }
            });
  }

  /**
   * Closes all registered services
   *
   * @throws Exception
   */
  public void close(Injector injector) throws Exception {

    serviceList.forEach(
        (clazz) -> {
          final Object instance = injector.getInstance(clazz);

          if (instance instanceof Service) {
            try {
              logger.debug("stopping {}", instance.getClass().toString());
              ((Service) instance).close();
            } catch (Exception e) {
              throwIfUnchecked(e);
              throw new RuntimeException(e);
            }
          }
        });
  }

  @VisibleForTesting
  protected List<Class> getServiceList() {
    return new ArrayList<>(serviceList);
  }
}
