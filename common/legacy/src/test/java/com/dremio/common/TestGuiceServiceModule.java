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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.inject.Provider;

import org.junit.Test;

import com.dremio.service.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import com.google.inject.Singleton;

/**
 * GuiceServiceModule tests
 */
public class TestGuiceServiceModule {
  @Test
  public void testSimpleChain() throws Exception {
    final GuiceServiceModule guiceServiceHandler = new GuiceServiceModule();
    final Injector injector = Guice.createInjector(guiceServiceHandler, new SimpleGraphModule());

    // initially nothing should be created
    assertEquals(0, guiceServiceHandler.getServiceList().size());

    injector.getInstance(A.class);
    assertEquals(0, guiceServiceHandler.getServiceList().size());

    final BImpl bInstance = (BImpl) injector.getInstance(B.class);
    assertEquals(2, guiceServiceHandler.getServiceList().size());
    assertTrue(bInstance.isStarted());
    assertFalse(bInstance.isClosed());

    // singleton so size should not change
    injector.getInstance(B.class);
    assertEquals(2, guiceServiceHandler.getServiceList().size());

    injector.getInstance(D.class);
    assertEquals(3, guiceServiceHandler.getServiceList().size());

    guiceServiceHandler.close(injector);
    assertTrue(bInstance.isStarted());
    assertTrue(bInstance.isClosed());
  }

  @Test
  public void testMultiBind() throws Exception {
    final GuiceServiceModule guiceServiceHandler = new GuiceServiceModule();
    final Injector injector = Guice.createInjector(guiceServiceHandler, new MultiBindModule());

    final MultiImpl bInstance = (MultiImpl) injector.getInstance(B.class);
    final MultiImpl cInstance = (MultiImpl) injector.getInstance(C.class);

    // B and C are bound to the same instance and Guice will only provision MultiImpl once, so it will
    // only be started once.
    assertEquals(bInstance, cInstance);
    assertEquals(1, bInstance.getStarted());

    guiceServiceHandler.close(injector);
    assertEquals(1, bInstance.getClosed());
  }

  @Test(expected = ProvisionException.class)
  public void testStartThrows() throws Exception {
    final GuiceServiceModule guiceServiceHandler = new GuiceServiceModule();
    final Injector injector = Guice.createInjector(guiceServiceHandler, new SimpleGraphModule());

    // will throw a ProvisionException
    injector.getInstance(ThrowsExceptionDuringStart.class);
  }

  private final class SimpleGraphModule extends AbstractModule {
    @Override
    protected void configure() {
    }

    @Provides
    A getA() {
      return new A();
    }

    @Provides
    @Singleton
    B getB(Provider<A> aProvider, Provider<C> cProvider) {
      aProvider.get();
      cProvider.get();
      return new BImpl();
    }

    @Provides
    @Singleton
    C getC(Provider<A> aProvider) {
      aProvider.get();
      return new CImpl();
    }

    @Provides
    @Singleton
    D getD(Provider<B> bProvider, Provider<C> cProvider, Provider<A> aProvider) {
      bProvider.get();
      aProvider.get();
      cProvider.get();
      return new D();
    }

    @Provides
    @Singleton
    ThrowsExceptionDuringStart getE(Provider<B> bProvider, Provider<C> cProvider, Provider<A> aProvider) {
      return new ThrowsExceptionDuringStart();
    }
  }

  private final class MultiBindModule extends AbstractModule {
    @Override
    protected void configure() {
      final MultiImpl multi = new MultiImpl();
      bind(B.class).toInstance(multi);
      bind(C.class).toInstance(multi);
    }
  }

  private static final class A {
  }

  private interface B {
  }

  private interface C extends B {
  }

  private static class Impl implements Service {
    private boolean started = false;
    private boolean closed = false;
    @Override
    public void start() throws Exception {
      started = true;
    }

    @Override
    public void close() throws Exception {
      closed = true;
    }

    public boolean isStarted() {
      return started;
    }

    public boolean isClosed() {
      return closed;
    }
  }

  private static class BImpl extends Impl implements B {
  }

  private static final class CImpl extends Impl implements C {
  }

  private static final class D extends Impl {
  }

  private static final class MultiImpl extends BImpl implements C {
    private int started = 0;
    private int closed = 0;

    @Override
    public void start() throws Exception {
      super.start();

      started++;
    }

    @Override
    public void close() throws Exception {
      super.close();

      closed++;
    }

    public int getStarted() {
      return started;
    }

    public int getClosed() {
      return closed;
    }
  }

  private static final class ThrowsExceptionDuringStart extends Impl {
    @Override
    public void start() throws Exception {
      throw new IllegalArgumentException("evil");
    }
  }
}
