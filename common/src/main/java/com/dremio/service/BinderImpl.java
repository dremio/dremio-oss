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

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.inject.ConfigurationException;
import com.google.inject.Injector;

/**
 * A very basic injection provider implementation.
 *
 * TODO: replace with real injection framework.
 */
public class BinderImpl implements Binder {

  private Injector injector;

  public static enum ResolverType {SINGLETON, INSTANCE, PROVIDER}

  private final BinderImpl parent;
  private volatile Map<Class<?>, Resolver> lookups = ImmutableMap.of();

  public BinderImpl() {
    this.parent = null;
  }

  public BinderImpl(BinderImpl parent) {
    this.parent = parent;
  }

  public void copyBindings(BinderImpl target){
    synchronized(target){
      ImmutableMap.Builder<Class<?>, Resolver> newMap = ImmutableMap.<Class<?>, Resolver>builder();
      newMap.putAll(target.lookups);
      newMap.putAll(lookups);
      target.lookups = newMap.build();
    }
  }

  @Override
  public <T> Provider<T> provider(final Class<T> iface) {
    return new DeferredProvider<>(iface);
  }

  private class DeferredProvider<T> implements Provider<T> {

    private final Class<T> iface;

    public DeferredProvider(Class<T> iface){
      this.iface = iface;
    }

    @Override
    public T get() {
      return lookup(iface);
    }

  }

  private Resolver getResolver(Class<?> iface){
    Resolver r = lookups.get(iface);

    if(r != null){
      return r;
    }

    if(parent != null){
      return parent.getResolver(iface);
    }

    return null;
  };

  @Override
  @SuppressWarnings("unchecked")
  public <T> T lookup(Class<T> iface) {
    Preconditions.checkNotNull(iface, "Must provided desired interface or class.");
    Resolver lookup = getResolver(iface);

    if (lookup == null && injector != null) {
      try {
        return injector.getInstance(iface);
      } catch (ConfigurationException ignored) {
      }
    }

    Preconditions.checkNotNull(lookup, "Unable to find injectable based on %s", iface.getName());
    // safe since the signatures above guarantee this.
    return (T) lookup.get(this);
  }

  @Override
  public <IFACE> void bindSelf(Class<IFACE> iface) {
    bind(iface, new InjectableReference(iface));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <IFACE> IFACE bindSelf(IFACE impl) {
    return bindInternal((Class<IFACE>) impl.getClass(), impl);
  }

  @Override
  public <IFACE> void bind(Class<IFACE> iface, Class<? extends IFACE> impl) {
    bind(iface, new InjectableReference(impl));
  }

  @Override
  public synchronized <IFACE> void replace(Class<IFACE> iface, Class<? extends IFACE> impl) {
    remove(iface);
    bind(iface, impl);
  }

  @Override
  public <IFACE> IFACE bind(Class<IFACE> iface, IFACE impl) {
    bind(iface, new GenericReference(impl));
    return impl;
  }

  @Override
  public synchronized <IFACE> boolean bindIfUnbound(Class<IFACE> iface, IFACE impl){
    if(!lookups.containsKey(iface)){
      bind(iface, impl);
      return true;
    } else {
      return false;
    }
  }

  private <IFACE> IFACE bindInternal(Class<IFACE> iface, @Nullable IFACE impl) {
    bind(iface, wrap(impl));
    return impl;
  }

  protected synchronized <IFACE> void bind(Class<IFACE> iface, Resolver reference) {
    Preconditions.checkNotNull(iface);
    final ImmutableMap.Builder<Class<?>, Resolver> newLookups = ImmutableMap.builder();
    newLookups.putAll(lookups);
    newLookups.put(iface, reference);
    lookups = newLookups.build();
  }

  @Override
  public synchronized <IFACE> void bindProvider(Class<IFACE> iface, Provider<? extends IFACE> provider) {
    Preconditions.checkNotNull(iface);
    final ImmutableMap.Builder<Class<?>, Resolver> newLookups = ImmutableMap.builder();
    newLookups.putAll(lookups);
    newLookups.put(iface, wrap(provider));
    lookups = newLookups.build();
  }

  @Override
  public synchronized <IFACE> void replaceProvider(Class<IFACE> iface, Provider<? extends IFACE> provider) {
    Preconditions.checkNotNull(iface);
    if (lookups.containsKey(iface)) {
      remove(iface);
    }
    bindProvider(iface, provider);
  }

  private synchronized void remove(final Class<?> iface){
    final ImmutableMap.Builder<Class<?>, Resolver> newLookups = ImmutableMap.builder();
    final Pointer<Resolver> removed = new Pointer<>();
    newLookups.putAll(Maps.filterEntries(lookups, new Predicate<Map.Entry<Class<?>, Resolver>>(){

      @Override
      public boolean apply(Entry<Class<?>, Resolver> input) {
        boolean matches = input.getKey().equals(iface);
        if(matches){
          removed.value = input.getValue();
        }
        return !matches;
      }}));

    if(removed.value == null){
      throw new IllegalStateException("Trying to remove an unbound value. No singleton is bound to " + iface.getName());
    }
    lookups = newLookups.build();
  }

  @Override
  public synchronized <IFACE> void replace(Class<IFACE> iface, IFACE impl){
    remove(iface);
    bindInternal(iface, impl);
  }

  protected synchronized <IFACE> void replace(Class<IFACE> iface, Resolver reference) {
    remove(iface);
    bind(iface, reference);
  }

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public Iterator<Binding<?>> iterator() {
    return Iterators.transform(lookups.entrySet().iterator(), new Function<Entry<Class<?>, Resolver>, Binding<?>>(){
      @Override
      public Binding apply(Entry<Class<?>, Resolver> input) {
        switch(input.getValue().getType()){
        case INSTANCE:
          return new InstanceBinding(input.getKey(), ((InjectableReference)input.getValue()).clazz);
        case SINGLETON:
          return new SingletonBinding(input.getKey(), input.getValue().get(null));
        case PROVIDER:
          return new ProviderBinding(input.getKey(), ((ProviderReference)input.getValue()).getProvider());
        default:
          throw new IllegalStateException();
        }

      }});
  }

  /**
   * Description of a specific singleton binding.
   */
  public abstract static class Binding<T> {
    private final Class<T> iface;

    private Binding(Class<T> iface) {
      super();
      this.iface = iface;
    }

    public Class<?> getIface() {
      return iface;
    }

    public abstract ResolverType getType();
  }

  public static class InstanceBinding<T> extends Binding<T> {
    private final Class<? extends T> instanceClass;

    private InstanceBinding(Class<T> iface, Class<? extends T> instanceClass) {
      super(iface);
      this.instanceClass = instanceClass;
    }

    public Class<? extends T> getInstance() {
      return instanceClass;
    }

    @Override
    public ResolverType getType() {
      return ResolverType.INSTANCE;
    }

  }

  public static class SingletonBinding<T> extends Binding<T> {
    private final T singleton;

    private SingletonBinding(Class<T> iface, T singleton) {
      super(iface);
      this.singleton = singleton;
    }

    public Object getSingleton() {
      return singleton;
    }

    @Override
    public ResolverType getType() {
      return ResolverType.SINGLETON;
    }
  }

  public static class ProviderBinding<T> extends Binding<T> {
    private final Provider<T> value;

    private ProviderBinding(Class<T> iface, Provider<T> value) {
      super(iface);
      this.value = value;
    }

    public Provider<T> getProvider() {
      return value;
    }

    @Override
    public ResolverType getType() {
      return ResolverType.PROVIDER;
    }
  }

  public BindingCreator getBindingCreator(){
    return new BindingCreatorImpl();
  }

  private class BindingCreatorImpl implements BindingCreator {

    @Override
    public <IFACE> IFACE bindSelf(IFACE impl) {
      return BinderImpl.this.bindSelf(impl);
    }

    @Override
    public <IFACE> IFACE bind(Class<IFACE> iface, IFACE impl) {
      return BinderImpl.this.bindInternal(iface, impl);
    }

    @Override
    public <IFACE> boolean bindIfUnbound(Class<IFACE> iface, IFACE impl) {
      return BinderImpl.this.bindIfUnbound(iface, impl);
    }

    @Override
    public <IFACE> void replace(Class<IFACE> iface, IFACE impl) {
      BinderImpl.this.replace(iface, impl);
    }

    @Override
    public <IFACE> void bindSelf(Class<IFACE> iface) {
      BinderImpl.this.bindSelf(iface);
    }

    @Override
    public <IFACE> void bind(Class<IFACE> iface, Class<? extends IFACE> impl) {
      BinderImpl.this.bind(iface, impl);
    }

    @Override
    public <IFACE> void replace(Class<IFACE> iface, Class<? extends IFACE> impl) {
      BinderImpl.this.replace(iface, impl);
    }

    @Override
    public <IFACE> void bindProvider(Class<IFACE> iface, Provider<? extends IFACE> provider) {
      BinderImpl.this.bindProvider(iface, provider);
    }

    @Override
    public <IFACE> void replaceProvider(Class<IFACE> iface, Provider<? extends IFACE> provider) {
      BinderImpl.this.replaceProvider(iface, provider);
    }
  }

  public BindingProvider getBindingProvider(){
    return new BindingProviderImpl();
  }

  /**
   * Masked impl.
   */
  private class BindingProviderImpl implements BindingProvider {

    @Override
    public <T> Provider<T> provider(Class<T> iface) {
      return BinderImpl.this.provider(iface);
    }

    @Override
    public <T> T lookup(Class<T> iface) {
      return BinderImpl.this.lookup(iface);
    }

    @Override
    public Iterator<Binding<?>> iterator() {
      return BinderImpl.this.iterator();
    }

    @Override
    public Binder newChild() {
      return BinderImpl.this.newChild();
    }
  }

  /**
   * A reference that ensures that we've initialized a service before returning this reference.
   */
  public interface Resolver {
    Object get(BindingProvider provider);
    ResolverType getType();
  }

  private Resolver wrap(Object obj) {
    if (obj instanceof Resolver) {
      return (Resolver) obj;
    } else if (obj instanceof Provider) {
      return new ProviderReference((Provider) obj);
    } else if (obj instanceof Class) {
      return new InjectableReference((Class<?>) obj);
    } else {
      return new GenericReference(obj);
    }
  }

  public static class GenericReference implements Resolver {

    private Object inner;

    public GenericReference(Object inner) {
      this.inner = inner;
    }

    @Override
    public Object get(BindingProvider provider) {
      return inner;
    }

    @Override
    public ResolverType getType() {
      return ResolverType.SINGLETON;
    }

  }

  private static class FinalResolver {
    private final Class<?> iface;

    public FinalResolver(Class<?> iface) {
      this.iface = iface;
    }

    public Object getImplementation(BindingProvider provider) {
      return provider.lookup(iface);
    }
  }

  public static class InjectableReference implements Resolver {

    private final Class<?> clazz;
    private final Constructor<?> constructor;
    private final List<FinalResolver> providers;

    public InjectableReference(Class<?> clazz){
      this.clazz = clazz;
      Collection<Constructor<?>> annotated = Collections2.filter(Arrays.asList(clazz.getConstructors()), new Predicate<Constructor<?>>(){
        @Override
        public boolean apply(Constructor<?> input) {
          return input.getAnnotation(Inject.class) != null;
        }});

      Preconditions.checkArgument(annotated.size() == 1, "Unable to treat class %s as injectable. It should have one and only one "
          + "constructor marked with @Inject annotation. It has %s constructors with this annotation.", clazz.getName(), annotated.size());

      this.constructor = annotated.iterator().next();
      this.providers = Arrays.stream(constructor.getParameterTypes())
        .map(FinalResolver::new)
        .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Object get(final BindingProvider provider) {
      try{
        return constructor.newInstance(FluentIterable.from(providers).transform(new Function<FinalResolver, Object>(){
          @Override
          public Object apply(FinalResolver input) {
            return input.getImplementation(provider);
          }}).toArray(Object.class));
      }catch(Exception ex){
        throw new RuntimeException(String.format("Failure while attempting to create %s.", clazz.getName()), ex);
      }
    }

    @Override
    public ResolverType getType() {
      return ResolverType.INSTANCE;
    }

  }

  public static class ProviderReference implements Resolver {
    private final Provider<?> provider;

    public ProviderReference(Provider<?> provider) {
      this.provider = provider;
    }

    @Override
    public Object get(BindingProvider bindingProvider) {
      return provider.get();
    }

    public Provider<?> getProvider() {
      return provider;
    }

    @Override
    public ResolverType getType() {
      return ResolverType.PROVIDER;
    }
  }

  @Override
  public Binder newChild() {
    return new BinderImpl(this);
  }

  @Override
  public void registerGuiceInjector(Injector injector) {
    this.injector = injector;
  }
}
