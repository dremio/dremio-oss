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
package com.dremio.service;

import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * A service registry that also supports exposing a lookup or injection provider for each registered service.
 */
public class SingletonRegistry extends BinderImpl implements AutoCloseable {

  private static enum ServiceState {INIT, STARTED, STOPPED};

  private final ServiceRegistry registry = new ServiceRegistry();

  public void start() throws Exception {
    registry.start();
  }

  protected Iterable<Service> getServices() {
    return Iterables.unmodifiableIterable(registry.getServices());
  }

  @SuppressWarnings("unchecked")
  public <S extends Service> S bindSelf(S service) {
    return bind((Class<S>) service.getClass(), service);
  }

  @SuppressWarnings("unchecked")
  public <A extends AutoCloseable> A bindSelf(A autoCloseable) {
    return bind((Class<A>) autoCloseable.getClass(), autoCloseable);
  }

  public <IFACE extends Service, IMPL extends IFACE> IMPL bind(Class<IFACE> iface, IMPL service) {
    ServiceReference<IFACE> ref = wrap(iface, service);
    super.bind(iface, ref);
    registry.register(ref);
    return service;
  }

  public <IFACE extends AutoCloseable, IMPL extends IFACE> IMPL bind(Class<IFACE> iface, IMPL autoCloseable) {
    CloseableReference<?> ref = wrap(iface, new CloseableAsService<IFACE>(autoCloseable));
    super.bind(iface, ref);
    registry.register(ref);
    return autoCloseable;
  }

  @SuppressWarnings("unchecked")
  public <S extends Service> S replace(S service) {
    return replace((Class<S>) service.getClass(), service);
  }

  @SuppressWarnings("unchecked")
  public <A extends AutoCloseable> A replace(A autoCloseable) {
    return replace((Class<A>) autoCloseable.getClass(), autoCloseable);
  }

  public <IFACE extends Service, IMPL extends IFACE> IMPL replace(Class<IFACE> iface, IMPL service) {
    ServiceReference<IFACE> ref = wrap(iface, service);
    super.replace(iface, ref);
    registry.replace(ref);
    return service;
  }

  public <IFACE extends AutoCloseable, IMPL extends IFACE> IMPL replace(Class<IFACE> iface, IMPL autoCloseable) {
    CloseableReference<IFACE> ref = wrap(iface, new CloseableAsService<IFACE>(autoCloseable));
    super.replace(iface, ref);
    registry.replace(ref);
    return autoCloseable;
  }

  @Override
  public void close() throws Exception {
    registry.close();
  }

  private static final class CloseableAsService<A extends AutoCloseable> implements Service {
    private final A c;

    private CloseableAsService(A c){
      this.c = c;
    }
    @Override
    public void close() throws Exception {
      c.close();
    }

    @Override
    public void start() throws Exception {
    }

  }

  private static <S extends Service> ServiceReference<S> wrap(Class<S> iface, S obj){
    return new ServiceReference<>(iface, obj);
  }

  private static <A extends AutoCloseable> CloseableReference<A> wrap(Class<A> iface, CloseableAsService<A> closeable){
    return new CloseableReference<>(iface, closeable);
  }

  private abstract static class AbstractServiceReference<T extends Service> implements Resolver, Service {
    protected final Class<?> iface;
    protected final T service;
    private volatile ServiceState state = ServiceState.INIT;

    public AbstractServiceReference(Class<?> iface, T service) {
      this.iface = iface;
      this.service = service;
    }

    @Override
    public synchronized void close() throws Exception {
      service.close();
      state = ServiceState.STOPPED;
    }

    @Override
    public synchronized void start() throws Exception {
      Preconditions.checkArgument(state == ServiceState.INIT, "Cannot start a service in state %s.", state.name());
      service.start();
      state = ServiceState.STARTED;
    }

    @Override
    public int hashCode() {
      return iface.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof AbstractServiceReference<?>)) {
        return false;
      }
      AbstractServiceReference<?> other = (AbstractServiceReference<?>) obj;
      return Objects.equals(iface, other.iface);
    }
  }

  private static class ServiceReference<S extends Service> extends AbstractServiceReference<S> {

    public ServiceReference(Class<S> iface, S service) {
      super(iface, service);
    }

    @Override
    public ResolverType getType() {
      return ResolverType.SINGLETON;
    }

    @Override
    public Object get(BindingProvider provider) {
      return service;
    }

  }

  private static class CloseableReference<A extends AutoCloseable> extends AbstractServiceReference<CloseableAsService<A>> {

    public CloseableReference(Class<A> iface, CloseableAsService<A> service) {
      super(iface, service);
    }

    @Override
    public ResolverType getType() {
      return ResolverType.SINGLETON;
    }

    @Override
    public Object get(BindingProvider provider) {
      return service.c;
    }

  }
}
