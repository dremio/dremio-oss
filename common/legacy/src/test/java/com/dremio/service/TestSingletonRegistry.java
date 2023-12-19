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

import static com.dremio.service.BinderImpl.ResolverType.SINGLETON;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.dremio.service.SingletonRegistry.CloseableReference;
import com.dremio.service.SingletonRegistry.ServiceReference;

@ExtendWith(MockitoExtension.class)
class TestSingletonRegistry {

  private SingletonRegistry registry;
  private ServiceRegistry delegateRegistry;


  @Nested
  @DisplayName("Registry lifecycle")
  class TestRegistryLifecycle {

    @BeforeEach
    void injectMockServiceRegistry() {
      delegateRegistry = mock(ServiceRegistry.class);
      registry = new SingletonRegistry(delegateRegistry);
    }

    @Test
    void testStart() throws Exception {
      registry.start();
      verify(delegateRegistry).start();
    }

    @Test
    void testClose() throws Exception {
      registry.close();
      verify(delegateRegistry).close();
    }

    @Test
    void testGetServices() throws Exception {
      try (Service service = new TestService()) {
        when(delegateRegistry.getServices()).thenReturn(singletonList(service));
        assertThat(registry.getServices().iterator()).satisfies(i -> {
          assertThat(i).hasNext();
          assertThat(i.next()).isSameAs(service);
          assertThat(i.hasNext()).isFalse();
        });
      }
    }
  }

  @Nested
  @DisplayName("Registry service binding")
  class TestRegistryBinding {

    @BeforeEach
    void injectDependency() {
      delegateRegistry = new ServiceRegistry(false);
      registry = new SingletonRegistry(delegateRegistry);
    }

    @Test
    void testBindSelfAutoCloseable() {
      AutoCloseable closeable = new TestAutoCloseable();
      AutoCloseable rtn = registry.bindSelf(closeable);
      assertThat(rtn).isSameAs(closeable);

      // Check delegate registry
      assertThat(delegateRegistry.getServices()).satisfies(srs -> {
        assertThat(srs.size()).isEqualTo(1);
        assertThat(srs.get(0)).satisfies(r -> {
          assertThat(r).isInstanceOf(CloseableReference.class);
          assertThat((CloseableReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(TestAutoCloseable.class);
            assertThat(sr.get(null)).isSameAs(closeable);
          });
        });
      });

      assertThat(registry.asMap()).satisfies(b -> {
        assertThat(b).hasSize(1);
        assertThat(b.get(TestAutoCloseable.class)).satisfies(r -> {
          assertThat(r).isInstanceOf(CloseableReference.class);
          assertThat((CloseableReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(TestAutoCloseable.class);
            assertThat(sr.get(null)).isSameAs(closeable);
          });
        });
      });
    }

    @Test
    void testBindSelfService() {
      Service service = new TestService();
      Service rtn = registry.bindSelf(service);
      assertThat(rtn).isSameAs(service);

      // Check delegate registry
      assertThat(delegateRegistry.getServices()).satisfies(srs -> {
        assertThat(srs.size()).isEqualTo(1);
        assertThat(srs.get(0)).satisfies(r -> {
          assertThat(r).isInstanceOf(ServiceReference.class);
          assertThat((ServiceReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(TestService.class);
            assertThat(sr.get(null)).isSameAs(service);
          });
        });
      });

      // Check binder
      assertThat(registry.asMap()).satisfies(b -> {
        assertThat(b).hasSize(1);
        assertThat(b.get(TestService.class)).satisfies(r -> {
          assertThat(r).isInstanceOf(ServiceReference.class);
          assertThat((ServiceReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(TestService.class);
            assertThat(sr.get(null)).isSameAs(service);
          });
        });
      });
    }

    @Test
    void testBindAutoCloseable() {
      AutoCloseable closeable = new TestAutoCloseable();
      AutoCloseable rtn = registry.bind(AutoCloseable.class, closeable);
      assertThat(rtn).isSameAs(closeable);

      // Check delegate registry
      assertThat(delegateRegistry.getServices()).satisfies(srs -> {
        assertThat(srs.size()).isEqualTo(1);
        assertThat(srs.get(0)).satisfies(r -> {
          assertThat(r).isInstanceOf(CloseableReference.class);
          assertThat((CloseableReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(AutoCloseable.class);
            assertThat(sr.get(null)).isSameAs(closeable);
          });
        });
      });

      assertThat(registry.asMap()).satisfies(b -> {
        assertThat(b).hasSize(1);
        assertThat(b.get(AutoCloseable.class)).satisfies(r -> {
          assertThat(r).isInstanceOf(CloseableReference.class);
          assertThat((CloseableReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(AutoCloseable.class);
            assertThat(sr.get(null)).isSameAs(closeable);
          });
        });
      });
    }

    @Test
    void testBindService() {
      Service service = new TestService();
      Service rtn = registry.bind(Service.class, service);
      assertThat(rtn).isSameAs(service);

      // Check delegate registry
      assertThat(delegateRegistry.getServices()).satisfies(srs -> {
        assertThat(srs.size()).isEqualTo(1);
        assertThat(srs.get(0)).satisfies(r -> {
          assertThat(r).isInstanceOf(ServiceReference.class);
          assertThat((ServiceReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(Service.class);
            assertThat(sr.get(null)).isSameAs(service);
          });
        });
      });

      // Check binder
      assertThat(registry.asMap()).satisfies(b -> {
        assertThat(b).hasSize(1);
        assertThat(b.get(Service.class)).satisfies(r -> {
          assertThat(r).isInstanceOf(ServiceReference.class);
          assertThat((ServiceReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(Service.class);
            assertThat(sr.get(null)).isSameAs(service);
          });
        });
      });
    }

    @Test
    void testReplaceAutoCloseable() {
      AutoCloseable closeable1 = new TestAutoCloseable();
      registry.bind(AutoCloseable.class, closeable1);

      AutoCloseable closeable2 = new TestAutoCloseable();
      AutoCloseable rtn = registry.replace(AutoCloseable.class, closeable2);
      assertThat(rtn).isSameAs(closeable2);

      // Check delegate registry
      assertThat(delegateRegistry.getServices()).satisfies(srs -> {
        assertThat(srs.size()).isEqualTo(1);
        assertThat(srs.get(0)).satisfies(r -> {
          assertThat(r).isInstanceOf(CloseableReference.class);
          assertThat((CloseableReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(AutoCloseable.class);
            assertThat(sr.get(null)).isSameAs(closeable2);
          });
        });
      });

      assertThat(registry.asMap()).satisfies(b -> {
        assertThat(b).hasSize(1);
        assertThat(b.get(AutoCloseable.class)).satisfies(r -> {
          assertThat(r).isInstanceOf(CloseableReference.class);
          assertThat((CloseableReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(AutoCloseable.class);
            assertThat(sr.get(null)).isSameAs(closeable2);
          });
        });
      });
    }

    @Test
    void testReplaceService() {
      Service service1 = new TestService();
      registry.bind(Service.class, service1);

      Service service2 = new TestService();
      Service rtn = registry.replace(Service.class, service2);

      // Check delegate registry
      assertThat(delegateRegistry.getServices()).satisfies(srs -> {
        assertThat(srs.size()).isEqualTo(1);
        assertThat(srs.get(0)).satisfies(r -> {
          assertThat(r).isInstanceOf(ServiceReference.class);
          assertThat((ServiceReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(Service.class);
            assertThat(sr.get(null)).isSameAs(service2);
          });
        });
      });

      // Check binder
      assertThat(rtn).isSameAs(service2);
      assertThat(registry.asMap()).satisfies(b -> {
        assertThat(b).hasSize(1);
        assertThat(b.get(Service.class)).satisfies(r -> {
          assertThat(r).isInstanceOf(ServiceReference.class);
          assertThat((ServiceReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(Service.class);
            assertThat(sr.get(null)).isSameAs(service2);
          });
        });
      });
    }

    @Test
    void testReplaceSelfAutoCloseable() {
      AutoCloseable closeable1 = new TestAutoCloseable();
      registry.bindSelf(closeable1);

      AutoCloseable closeable2 = new TestAutoCloseable();
      AutoCloseable rtn = registry.replace(closeable2);
      assertThat(rtn).isSameAs(closeable2);

      // Check delegate registry
      assertThat(delegateRegistry.getServices()).satisfies(srs -> {
        assertThat(srs.size()).isEqualTo(1);
        assertThat(srs.get(0)).satisfies(r -> {
          assertThat(r).isInstanceOf(CloseableReference.class);
          assertThat((CloseableReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(TestAutoCloseable.class);
            assertThat(sr.get(null)).isSameAs(closeable2);
          });
        });
      });

      assertThat(registry.asMap()).satisfies(b -> {
        assertThat(b).hasSize(1);
        assertThat(b.get(TestAutoCloseable.class)).satisfies(r -> {
          assertThat(r).isInstanceOf(CloseableReference.class);
          assertThat((CloseableReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(TestAutoCloseable.class);
            assertThat(sr.get(null)).isSameAs(closeable2);
          });
        });
      });
    }

    @Test
    void testReplaceSelfService() {
      Service service1 = new TestService();
      registry.bindSelf(service1);

      Service service2 = new TestService();
      Service rtn = registry.replace(service2);
      assertThat(rtn).isSameAs(service2);

      // Check delegate registry
      assertThat(delegateRegistry.getServices()).satisfies(srs -> {
        assertThat(srs.size()).isEqualTo(1);
        assertThat(srs.get(0)).satisfies(r -> {
          assertThat(r).isInstanceOf(ServiceReference.class);
          assertThat((ServiceReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(TestService.class);
            assertThat(sr.get(null)).isSameAs(service2);
          });
        });
      });

      // Check binder
      assertThat(registry.asMap()).satisfies(b -> {
        assertThat(b).hasSize(1);
        assertThat(b.get(TestService.class)).satisfies(r -> {
          assertThat(r).isInstanceOf(ServiceReference.class);
          assertThat((ServiceReference<?>) r).satisfies(sr -> {
            assertThat(sr.getType()).isEqualTo(SINGLETON);
            assertThat(sr.getIface()).isEqualTo(TestService.class);
            assertThat(sr.get(null)).isSameAs(service2);
          });
        });
      });
    }

  }

  static class TestService implements Service {

    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }
  }

  static class TestAutoCloseable implements AutoCloseable {

    @Override
    public void close() throws Exception {
    }
  }
}
