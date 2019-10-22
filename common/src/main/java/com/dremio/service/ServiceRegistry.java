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

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import javax.annotation.Nullable;

import com.dremio.common.AutoCloseables;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

/**
 * A simple service registry to start and stop services in order.
 */
public class ServiceRegistry implements Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingletonRegistry.class);

  private volatile boolean closed = false;
  private final List<Service> services = new ArrayList<>();
  private final boolean timerEnabled;

  public ServiceRegistry() {
    this(Timer.enabled());
  }

  @VisibleForTesting
  ServiceRegistry(boolean timerEnabled) {
    this.timerEnabled = timerEnabled;
  }

  public <T extends Service> T register(@Nullable T service) {
    if (service != null) {
      services.add(wrapService(service));
    }
    return service;
  }

  public <T extends Service> T replace(@Nullable T service) {
    if (service == null) {
      return null;
    }

    final Service toReplace = wrapService(service);

    for(ListIterator<Service> it = services.listIterator(); it.hasNext(); ) {
      Service s = it.next();
      if (toReplace.equals(s)) {
        it.remove();
        try {
          // Closing service in case some resources are already allocated
          s.close();
        } catch (Exception e) {
          logger.warn("Exception when closing service {}", s, e);
        }
        it.add(toReplace);
        return service;
      }
    }
    throw new IllegalArgumentException("Trying to replace an unregistered service");
  }

  private Service wrapService(Service service) {
    return timerEnabled ? new TimedService(service) : service;
  }

  @Override
  public void start() throws Exception {
    for(Service service: services) {
      service.start();
    }
  }

  @Override
  public synchronized void close() throws Exception {
    if(!closed){
      closed = true;
      AutoCloseables.close(Lists.reverse(services));
    }
  }

  protected List<Service> getServices() {
    return services;
  }

  private static final class TimedService implements Service {

    private final Service delegate;
    private final String name;

    private TimedService(Service delegate) {
      this.delegate = delegate;
      this.name = delegate.getClass().getSimpleName();
    }

    @Override
    public void close() throws Exception {
      try (TimedBlock b = Timer.time(name + ".close")) {
        delegate.close();
      }
    }

    @Override
    public void start() throws Exception {
      try (TimedBlock b = Timer.time(name + ".start")) {
        delegate.start();
      }
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof TimedService)) {
        return false;
      }
      TimedService other = (TimedService) obj;

      return Objects.equal(delegate, other.delegate);
    }
  }
}
