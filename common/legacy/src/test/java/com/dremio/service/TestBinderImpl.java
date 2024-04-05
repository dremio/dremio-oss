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

import static com.dremio.service.BinderImpl.ResolverType.INSTANCE;
import static com.dremio.service.BinderImpl.ResolverType.PROVIDER;
import static com.dremio.service.BinderImpl.ResolverType.SINGLETON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.service.BinderImpl.Binding;
import com.dremio.service.BinderImpl.DeferredProvider;
import com.dremio.service.BinderImpl.FinalResolver;
import com.dremio.service.BinderImpl.GenericReference;
import com.dremio.service.BinderImpl.InjectableReference;
import com.dremio.service.BinderImpl.InstanceBinding;
import com.dremio.service.BinderImpl.ProviderBinding;
import com.dremio.service.BinderImpl.ProviderReference;
import com.dremio.service.BinderImpl.Resolver;
import com.dremio.service.BinderImpl.SingletonBinding;
import com.google.inject.Injector;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.inject.Inject;
import javax.inject.Provider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TestBinderImpl {

  private final BinderImpl binder = new BinderImpl();

  @Test
  void testIterator() {
    // The iterator is principally used in the InjectableReference to resolve the dependency
    // instances on a call to get()
    binder.bind(String.class, "THIS");
    binder.bind(Foo.class, PublicFooImplementationWithInjectableCtor.class);
    Provider<OutputStream> provider = ByteArrayOutputStream::new;
    binder.bindProvider(OutputStream.class, provider);

    Iterable<Binding<?>> iterable = binder::iterator;
    Map<Class<?>, Binding<?>> bindings =
        StreamSupport.stream(iterable.spliterator(), false)
            .collect(Collectors.toMap(Binding::getIface, Function.identity()));
    assertThat(bindings)
        .satisfies(
            b -> {
              assertThat(b).hasSize(3);
              assertThat(b.get(String.class))
                  .satisfies(
                      sb -> {
                        assertThat(sb.getIface()).isEqualTo(String.class);
                        assertThat(sb.getType()).isEqualTo(SINGLETON);
                        assertThat(sb).isInstanceOf(SingletonBinding.class);
                        assertThat((SingletonBinding<?>) sb)
                            .satisfies(
                                ssb -> {
                                  assertThat(ssb.getSingleton()).isEqualTo("THIS");
                                });
                      });
              assertThat(b.get(Foo.class))
                  .satisfies(
                      sb -> {
                        assertThat(sb.getIface()).isEqualTo(Foo.class);
                        assertThat(sb.getType()).isEqualTo(INSTANCE);
                        assertThat(sb).isInstanceOf(InstanceBinding.class);
                        assertThat((InstanceBinding<?>) sb)
                            .satisfies(
                                isb -> {
                                  assertThat(isb.getInstance())
                                      .isEqualTo(PublicFooImplementationWithInjectableCtor.class);
                                });
                      });
              assertThat(b.get(OutputStream.class))
                  .satisfies(
                      sb -> {
                        assertThat(sb.getIface()).isEqualTo(OutputStream.class);
                        assertThat(sb.getType()).isEqualTo(PROVIDER);
                        assertThat(sb).isInstanceOf(ProviderBinding.class);
                        assertThat((ProviderBinding<?>) sb)
                            .satisfies(
                                isb -> {
                                  assertThat(isb.getProvider()).isEqualTo(provider);
                                });
                      });
            });
  }

  @Nested
  @DisplayName("Validate scopes")
  /*
   * Tests to verify our understanding of scopes. What these show is that the DI is biased towards eager
   * instantiation of singletons.
   */
  class Scopes {

    @Test
    void testBoundInstanceBehavesAsASingleton() {
      binder.bind(String.class, "THIS");
      String value1 = binder.lookup(String.class);
      String value2 = binder.lookup(String.class);
      assertThat(value1).isSameAs(value2);
    }

    @Test
    void testBoundProviderDoesNotBehaveAsASingleton() {
      binder.bindProvider(Object.class, Object::new);
      Object value1 = binder.lookup(Object.class);
      Object value2 = binder.lookup(Object.class);
      assertThat(value1).isNotSameAs(value2);
    }

    @Test
    void testBoundProviderCanBehaveAsASingleton() {
      Object singleton = new Object();
      binder.bindProvider(Object.class, () -> singleton);
      Object value1 = binder.lookup(Object.class);
      Object value2 = binder.lookup(Object.class);
      assertThat(value1).isSameAs(value2);
    }

    /* Naive attempt to have a lazy singleton via a provider. This would be annoying to do in practice. */
    @Test
    void testBoundProviderCanBehaveAsALazySingleton() {
      binder.bindProvider(
          Object.class,
          new Provider<Object>() {
            private volatile Object instance;

            @Override
            public Object get() {
              if (instance == null) {
                synchronized (Object.class) {
                  if (instance == null) {
                    instance = new Object();
                  }
                }
              }
              return instance;
            }
          });
      Object value1 = binder.lookup(Object.class);
      Object value2 = binder.lookup(Object.class);
      assertThat(value1).isSameAs(value2);
    }

    @Test
    void testBoundInjectableDoesNotBehaveAsASingleton() {
      binder.bind(Foo.class, PublicFooImplementationWithInjectableCtor.class);
      // Injected dependency
      binder.bind(String.class, "THIS");
      Foo value1 = binder.lookup(Foo.class);
      Foo value2 = binder.lookup(Foo.class);
      assertThat(value1).isNotSameAs(value2);
    }
  }

  @Nested
  @DisplayName("Graph traversal")
  class GraphTraversal {

    @Nested
    @DisplayName("Final Resolver")
    class TestFinalResolver {

      @Test
      void testGetImplementation() {
        // We expect to see the final resolver to delegate a lookup to
        // the binding provider, causing either an instantiation of the target
        // class, or the return of an existing instance.
        BindingProvider bindingProvider = mock(BindingProvider.class);
        FinalResolver finalResolver = new FinalResolver(String.class);
        finalResolver.getImplementation(bindingProvider);
        verify(bindingProvider).lookup(String.class);
      }
    }

    @Nested
    @DisplayName("Injectable reference")
    class TestInjectableReference {

      @Test
      void testGet() {
        // In an injectable reference, it creates a final resolver instance for each injectable
        // argument of the ctor. When we call get, this triggers a getImplementation call on each
        // final resolver, forcing a cascade of dependency instantiation and resolution.
        InjectableReference reference =
            new InjectableReference(PublicFooImplementationWithInjectableCtor.class);

        BindingProvider bindingProvider = mock(BindingProvider.class);
        reference.get(bindingProvider);
        // Subsequent lookup of injectable ctor arg
        verify(bindingProvider).lookup(String.class);
      }
    }
  }

  @Nested
  @DisplayName("Binding Resolution")
  class TestResolution {

    @Nested
    @DisplayName("Resolver resolution")
    class TestResolverResolution {
      @Test
      void testInstanceResolver() {
        binder.bind(String.class, "THIS");
        assertThat(binder.getResolver(String.class))
            .satisfies(
                r -> {
                  assertThat(r).isInstanceOf(GenericReference.class);
                  assertThat(r.getType()).isEqualTo(SINGLETON);
                  assertThat(r.get(null)).isEqualTo("THIS");
                });
      }

      @Test
      void testProviderResolver() {
        Provider<String> provider = mock(Provider.class);
        when(provider.get()).thenReturn("THIS");
        binder.bindProvider(String.class, provider);
        assertThat(binder.getResolver(String.class))
            .satisfies(
                r -> {
                  verify(provider, never()).get();
                  assertThat(r).isInstanceOf(ProviderReference.class);
                  assertThat(r.getType()).isEqualTo(PROVIDER);
                  assertThat(r.get(null)).isEqualTo("THIS");
                });
      }

      @Test
      void testInjectableResolver() {
        binder.bind(Foo.class, PublicFooImplementationWithInjectableCtor.class);
        assertThat(binder.getResolver(Foo.class))
            .satisfies(
                r -> {
                  assertThat(r).isInstanceOf(InjectableReference.class);
                  assertThat(r.getType()).isEqualTo(INSTANCE);
                  // Inject dependency
                  binder.bind(String.class, "DEPENDENCY");
                  BindingProvider bindingProvider = binder;
                  assertThat(r.get(bindingProvider))
                      .isInstanceOf(PublicFooImplementationWithInjectableCtor.class);
                });
      }

      @Test
      void testInjectableResolverCheckNoEagerCinit() {
        // If Injectable is eagerly created, this will fail
        binder.bind(Foo.class, UninstantiablePublicFooImplementationWithInjectableCtor.class);
        assertThat(binder.getResolver(Foo.class))
            .satisfies(
                r -> {
                  assertThat(r).isInstanceOf(InjectableReference.class);
                  assertThat(r.getType()).isEqualTo(INSTANCE);
                });
      }

      @Test
      void testNonExistentResolver() {
        assertThat(binder.getResolver(String.class)).isNull();
      }

      @Test
      void testParentResolver() {
        binder.bind(String.class, "THIS");
        BinderImpl child = new BinderImpl(binder);
        assertThat(child.asMap()).isEmpty();
        assertThat(child.getResolver(String.class))
            .satisfies(
                r -> {
                  assertThat(r).isInstanceOf(GenericReference.class);
                  assertThat(r.getType()).isEqualTo(SINGLETON);
                  assertThat(r.get(null)).isEqualTo("THIS");
                });
      }
    }

    @Nested
    @DisplayName("Reference Resolution")
    class TestReferenceResolution {

      @Nested
      @DisplayName("Direct Reference resolution")
      class TestLookup {

        @Test
        void testLookupInstanceReference() {
          binder.bind(String.class, "THIS");
          assertThat(binder.lookup(String.class)).isEqualTo("THIS");
        }

        @Test
        void testLookupProviderReference() {
          Provider<String> provider = mock(Provider.class);
          when(provider.get()).thenReturn("THIS");
          binder.bindProvider(String.class, provider);
          verify(provider, never()).get();
          assertThat(binder.lookup(String.class)).isEqualTo("THIS");
        }

        @Test
        void testLookupInjectableReference() {
          binder.bind(Foo.class, PublicFooImplementationWithInjectableCtor.class);
          // Inject dependency
          binder.bind(String.class, "DEPENDENCY");
          assertThat(binder.lookup(Foo.class))
              .satisfies(
                  v -> {
                    assertThat(v).isNotNull();
                    assertThat(v).isInstanceOf(PublicFooImplementationWithInjectableCtor.class);
                  });
        }

        @Test
        void testLookupNonExistentReference() {
          assertThatThrownBy(() -> binder.lookup(String.class))
              .isInstanceOf(NullPointerException.class);
        }

        @Test
        void testLookupGuiceInstanceReference() {
          Injector guiceInjector = mock(Injector.class);
          when(guiceInjector.getInstance(String.class)).thenReturn("THIS");
          binder.registerGuiceInjector(guiceInjector);
          assertThat(binder.asMap()).isEmpty();
          assertThat(binder.lookup(String.class))
              .satisfies(
                  v -> {
                    assertThat(v).isEqualTo("THIS");
                    verify(guiceInjector).getInstance(String.class);
                  });
        }

        @Test
        void testLookupParentInstanceReference() {
          binder.bind(String.class, "THIS");
          BinderImpl child = new BinderImpl(binder);
          assertThat(child.asMap()).isEmpty();
          assertThat(child.lookup(String.class)).isEqualTo("THIS");
        }
      }

      @Nested
      @DisplayName("Deferred Reference resolution")
      class TestProvider {
        @Test
        void testProviderForInstanceReference() {
          binder.bind(String.class, "THIS");
          assertThat(binder.provider(String.class).get()).isEqualTo("THIS");
        }

        @Test
        void testProviderForProviderReference() {
          Provider<String> provider = mock(Provider.class);
          when(provider.get()).thenReturn("THIS");
          binder.bindProvider(String.class, provider);
          assertThat(binder.provider(String.class))
              .satisfies(
                  p -> {
                    assertThat(p).isNotEqualTo(provider);
                    assertThat(p).isInstanceOf(DeferredProvider.class);
                    // It's a deferred provider, so we should expect it to defer the get
                    verify(provider, never()).get();
                    assertThat(p.get()).isSameAs(provider.get());
                  });
        }

        @Test
        void testProviderForInjectableReference() {
          binder.bind(Foo.class, PublicFooImplementationWithInjectableCtor.class);
          assertThat(binder.provider(Foo.class))
              .satisfies(
                  p -> {
                    assertThat(p).isInstanceOf(DeferredProvider.class);
                    // Inject dependency
                    binder.bind(String.class, "DEPENDENCY");
                    assertThat(p.get())
                        .satisfies(
                            v -> {
                              assertThat(v).isNotNull();
                              assertThat(v)
                                  .isInstanceOf(PublicFooImplementationWithInjectableCtor.class);
                            });
                  });
        }

        @Test
        void testProviderForInjectableReferenceCheckNoEagerCinit() {
          // If Injectable is eagerly created, this will fail
          binder.bind(Foo.class, UninstantiablePublicFooImplementationWithInjectableCtor.class);
          assertThat(binder.provider(Foo.class)).isInstanceOf(DeferredProvider.class);
        }

        @Test
        void testProviderForNonExistentReference() {
          assertThat(binder.provider(String.class))
              .satisfies(
                  p -> {
                    assertThat(p).isNotNull();
                    assertThatThrownBy(p::get).isInstanceOf(NullPointerException.class);
                  });
        }

        @Test
        void testProviderForGuiceInstanceReference() {
          Injector guiceInjector = mock(Injector.class);
          when(guiceInjector.getInstance(String.class)).thenReturn("THIS");
          binder.registerGuiceInjector(guiceInjector);
          assertThat(binder.asMap()).isEmpty();
          assertThat(binder.provider(String.class))
              .satisfies(
                  p -> {
                    assertThat(p).isInstanceOf(DeferredProvider.class);
                    // It's a deferred provider, so we should expect it to defer the get
                    verify(guiceInjector, never()).getInstance((Class<?>) any());
                    assertThat(p.get()).isEqualTo("THIS");
                    verify(guiceInjector).getInstance(String.class);
                  });
        }

        @Test
        void testProviderForParentInstanceReference() {
          binder.bind(String.class, "THIS");
          BinderImpl child = new BinderImpl(binder);
          assertThat(child.asMap()).isEmpty();
          assertThat(child.provider(String.class).get()).isEqualTo("THIS");
        }
      }
    }
  }

  @Nested
  @DisplayName("Wrap functions")
  class TestWrapFunction {

    @Test
    void testWrapResolver() {
      Resolver resolver = mock(Resolver.class);
      assertThat(BinderImpl.wrap(resolver)).isEqualTo(resolver);
      verify(resolver, never()).get(any(BindingProvider.class));
    }

    @Test
    void testWrapProvider() {
      Provider<?> provider = mock(Provider.class);
      assertThat(BinderImpl.wrap(provider)).isEqualTo(new ProviderReference(provider));
      verify(provider, never()).get();
    }

    @Test
    void testWrapNonInjectableClass() {
      assertThatThrownBy(() -> BinderImpl.wrap(Foo.class))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testWrapInjectableClass() {
      assertThat(BinderImpl.wrap(UninstantiablePublicFooImplementationWithInjectableCtor.class))
          .isEqualTo(
              new InjectableReference(
                  UninstantiablePublicFooImplementationWithInjectableCtor.class));
    }

    @Test
    void testWrapObject() {
      assertThat(BinderImpl.wrap("THIS")).isEqualTo(new GenericReference("THIS"));
    }
  }

  @Nested
  @DisplayName("Binding functions")
  class TestBindingFunctions {

    @Test
    void testBindInstance() {
      binder.bind(String.class, "THIS");
      assertThat(binder.asMap())
          .satisfies(
              b -> {
                assertThat(b).hasSize(1);
                assertThat(b).containsEntry(String.class, new GenericReference("THIS"));
              });
    }

    @Test
    void testBindInstanceNull() {
      binder.bind(String.class, (String) null);
      assertThat(binder.asMap())
          .satisfies(
              b -> {
                assertThat(b).hasSize(1);
                assertThat(b).containsEntry(String.class, new GenericReference(null));
              });
    }

    @Test
    void testBindSelfInstance() {
      binder.bindSelf("THIS");
      assertThat(binder.asMap())
          .satisfies(
              b -> {
                assertThat(b).hasSize(1);
                assertThat(b).containsEntry(String.class, new GenericReference("THIS"));
              });
    }

    @Test
    void testBindPublicImplementationClassWithInjectableCtor() {
      binder.bind(Foo.class, UninstantiablePublicFooImplementationWithInjectableCtor.class);
      assertThat(binder.asMap())
          .satisfies(
              b -> {
                assertThat(b).hasSize(1);
                assertThat(b)
                    .containsEntry(
                        Foo.class,
                        new InjectableReference(
                            UninstantiablePublicFooImplementationWithInjectableCtor.class));
              });
    }

    @Test
    void testBindPrivateImplementationClassWithInjectableCtor() {
      assertThatThrownBy(
              () ->
                  binder.bind(
                      Foo.class, UninstantiablePrivateFooImplementationWithInjectableCtor.class))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBindImplementationClassWithoutInjectableCtor() {
      assertThatThrownBy(() -> binder.bind(CharSequence.class, String.class))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBindSelfPublicImplementationClassWithInjectableCtor() {
      binder.bindSelf(UninstantiablePublicFooImplementationWithInjectableCtor.class);
      assertThat(binder.asMap())
          .satisfies(
              b -> {
                assertThat(b).hasSize(1);
                assertThat(b)
                    .containsEntry(
                        UninstantiablePublicFooImplementationWithInjectableCtor.class,
                        new InjectableReference(
                            UninstantiablePublicFooImplementationWithInjectableCtor.class));
              });
    }

    @Test
    void testBindProvider() {
      Provider<String> provider = mock(Provider.class);
      when(provider.get()).thenReturn("THIS");
      verify(provider, never()).get();
      binder.bindProvider(CharSequence.class, provider);
      assertThat(binder.asMap())
          .satisfies(
              b -> {
                assertThat(b).hasSize(1);
                assertThat(b).containsEntry(CharSequence.class, new ProviderReference(provider));
              });
    }

    @Nested
    @DisplayName("Conditional Binding functions")
    class TestConditionalBindingFunctions {

      @Test
      void testNewInstanceBinding() {
        assertThat(binder.bindIfUnbound(String.class, "THIS")).isTrue();
        assertThat(binder.asMap())
            .satisfies(
                b -> {
                  assertThat(b).hasSize(1);
                  assertThat(b).containsEntry(String.class, new GenericReference("THIS"));
                });
      }

      @Test
      void testExistingInstanceBinding() {
        binder.bind(String.class, "THIS");
        assertThat(binder.bindIfUnbound(String.class, "THAT")).isFalse();
        assertThat(binder.asMap())
            .satisfies(
                b -> {
                  assertThat(b).hasSize(1);
                  assertThat(b).containsEntry(String.class, new GenericReference("THIS"));
                });
      }
    }

    @Nested
    @DisplayName("Remove binding functions")
    class TestRemoveFunctions {
      @Test
      void testRemoveReference() {
        binder.bind(String.class, "THIS");
        binder.remove(String.class);
        assertThat(binder.asMap()).isEmpty();
      }

      @Test
      void testRemoveNonExistentReference() {
        assertThatThrownBy(() -> binder.remove(String.class))
            .isInstanceOf(IllegalStateException.class);
      }
    }

    @Nested
    @DisplayName("Replace binding functions")
    class TestReplaceFunctions {

      @Test
      void testReplaceInstance() {
        binder.bind(String.class, "THIS");
        binder.replace(String.class, "THAT");
        assertThat(binder.asMap())
            .satisfies(
                b -> {
                  assertThat(b).hasSize(1);
                  assertThat(b).containsEntry(String.class, new GenericReference("THAT"));
                });
      }

      @Test
      void testReplaceNonExistentInstance() {
        assertThatThrownBy(() -> binder.replace(String.class, "THAT"))
            .isInstanceOf(IllegalStateException.class);
      }

      @Test
      void testReplaceProvider() {
        binder.bindProvider(String.class, mock(Provider.class));
        Provider<String> replacement = mock(Provider.class);
        binder.replaceProvider(String.class, replacement);
        verify(replacement, never()).get();
        assertThat(binder.asMap())
            .satisfies(
                b -> {
                  assertThat(b).hasSize(1);
                  assertThat(b).containsEntry(String.class, new ProviderReference(replacement));
                });
      }

      /* This is not consistent with other replace methods */
      @Test
      void testReplaceNonExistentProvider() {
        Provider<String> replacement = mock(Provider.class);
        binder.replaceProvider(String.class, replacement);
        verify(replacement, never()).get();
        assertThat(binder.asMap())
            .satisfies(
                b -> {
                  assertThat(b).hasSize(1);
                  assertThat(b).containsEntry(String.class, new ProviderReference(replacement));
                });
      }

      @Test
      void testReplaceInjectable() {
        binder.bind(Foo.class, UninstantiablePublicFooImplementationWithInjectableCtor.class);
        binder.replace(Foo.class, UninstantiablePublicFooImplementationWithInjectableCtor2.class);
        assertThat(binder.asMap())
            .satisfies(
                b -> {
                  assertThat(b).hasSize(1);
                  assertThat(b)
                      .containsEntry(
                          Foo.class,
                          new InjectableReference(
                              UninstantiablePublicFooImplementationWithInjectableCtor2.class));
                });
      }

      @Test
      void testReplaceNonExistentInjectable() {
        assertThatThrownBy(
                () ->
                    binder.replace(
                        Foo.class, UninstantiablePublicFooImplementationWithInjectableCtor.class))
            .isInstanceOf(IllegalStateException.class);
      }

      @Test
      void testReplaceResolver() {
        binder.bind(String.class, mock(Resolver.class));
        Resolver replacement = mock(Resolver.class);
        binder.replace(String.class, replacement);
        verify(replacement, never()).get(any(BindingProvider.class));
        assertThat(binder.asMap())
            .satisfies(
                b -> {
                  assertThat(b).hasSize(1);
                  assertThat(b).containsEntry(String.class, replacement);
                });
      }

      @Test
      void testReplaceNonExistentResolver() {
        Resolver replacement = mock(Resolver.class);
        assertThatThrownBy(() -> binder.replace(Foo.class, replacement))
            .isInstanceOf(IllegalStateException.class);
        verify(replacement, never()).get(any(BindingProvider.class));
      }
    }
  }

  interface Foo {}

  /** An injectable that can be bound and instantiated. */
  static class PublicFooImplementationWithInjectableCtor implements Foo {

    @Inject
    public PublicFooImplementationWithInjectableCtor(String bar) {}
  }

  /** An injectable that can't be instantiated. */
  static class UninstantiablePublicFooImplementationWithInjectableCtor implements Foo {

    @Inject
    public UninstantiablePublicFooImplementationWithInjectableCtor(String bar) {
      throw new IllegalStateException("Unexpected eager cinit()");
    }
  }

  /** An injectable that can't be instantiated. */
  static class UninstantiablePublicFooImplementationWithInjectableCtor2 implements Foo {

    @Inject
    public UninstantiablePublicFooImplementationWithInjectableCtor2(String bar) {
      throw new IllegalStateException("Unexpected eager cinit()");
    }
  }

  /** An injectable that can't be bound because it is private, can cannot be instantiated. */
  static class UninstantiablePrivateFooImplementationWithInjectableCtor implements Foo {

    @Inject
    UninstantiablePrivateFooImplementationWithInjectableCtor(String bar) {
      throw new IllegalStateException("Unexpected eager cinit()");
    }
  }
}
