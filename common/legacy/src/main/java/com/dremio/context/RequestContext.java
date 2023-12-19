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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents the context for a request.  The context stores key/value pairs and is immutable.  Provides ways to
 * apply the context to Runnables/Callables.
 *
 * Example usage:
 *
 * RequestContext.current()
 *   .with(key, value)
 *   .run(() -> {
 *     RequestContext.current().get(key);
 *   });
 */
@SuppressWarnings("checkstyle:FinalClass")
public class RequestContext {
  private static final LocalValue<RequestContext> CURRENT = new LocalValue<>();
  private static final RequestContext EMPTY = new RequestContext(Collections.emptyMap());

  private final Map<Key<?>, Object> values;

  private RequestContext(Map<Key<?>, Object> values) {
    this.values = values;
  }

  /**
   * Assigns a value to a key for the context.
   *
   * @param key key of type T
   * @param value value of type T
   * @param <T> type of the value being stored
   * @return a new RequestContext with the key/value set
   */
  public <T> RequestContext with(Key<T> key, T value) {
    final Map<Key<?>, Object> newValues = new HashMap<>(this.values);
    newValues.put(key, value);

    return new RequestContext(newValues);
  }

  /**
   *  Adds a map of key/values to the context.
   *
   * @param map map of key/values to add to the context
   * @return a new RequestContext with the keys/values set
   */
  public RequestContext with(Map<Key<?>, Object> map) {
    final Map<Key<?>, Object> newValues = new HashMap<>(this.values);
    newValues.putAll(map);

    return new RequestContext(newValues);
  }

  /**
   * Removes the value for the given key from the context.
   */
  public <T> RequestContext without(Key key) {
    final Map<Key<?>, Object> newValues = new HashMap<>(this.values);
    newValues.remove(key);

    return new RequestContext(newValues);
  }

  /**
   * Returns the value for the specified key.
   *
   * @param key key of type T
   * @param <T> type of the value being stored
   * @return the value assigned to the key
   */
  @SuppressWarnings("unchecked")
  public <T> T get(Key<T> key) {
    return (T) values.get(key);
  }

  /**
   * Runs the runnable in the current context.
   *
   * @param runnable runnable to run with the context
   */
  public void run(Runnable runnable) {
    final RequestContext saved = current();
    CURRENT.set(this);

    try {
      runnable.run();
    } finally {
      CURRENT.set(saved);
    }
  }

  /**
   * Runs the callable in the current context.
   *
   * @param callable callable to call with the context
   * @param <V> return type of the callable
   * @return the result of the callable
   */
  public <V> V call(Callable<V> callable) throws Exception {
    final RequestContext saved = current();
    CURRENT.set(this);

    try {
      return callable.call();
    } finally {
      CURRENT.set(saved);
    }
  }

  /**
   * Runs the callable in the current context and transforms any Exceptions to RuntimeExceptions
   */
  public <V> V callUnchecked(Callable<V> callable) {
    try {
      return call(callable);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * <p>
   * Builds the Stream via the callable under this context and makes sure that later on,
   * the lazy-evaluation of elements in the Stream also happens under this context.
   * Note: if different pipeline steps need a different RequestContext this has to be taken
   * care of manually by capturing the RequestContext in a local variable outside of the lambda.
   * </p>
   * <p>
   * For example this is needed when the Stream pipeline uses a context aware method:
   * </p>
   * <code>return buildStream().filter(x -> x > 2).map(x -> contextMethod(x, otherParam));</code>
   */
  public <E> Stream<E> callStream(Callable<Stream<E>> callable) throws Exception {
    final RequestContext saved = current();
    CURRENT.set(this);

    try {
      Stream<E> stream = callable.call();
      Spliterator<E> wrapper = new RequestContextAwareSplitIterator<E>(this, stream.spliterator());
      return StreamSupport.stream(wrapper, false);
    } finally {
      CURRENT.set(saved);
    }
  }

  /**
   * Returns the currently set RequestContext.
   *
   * @return the current RequestContext
   */
  public static RequestContext current() {
    return CURRENT.get().orElse(EMPTY);
  }

  /**
   * Returns an empty RequestContext.
   *
   * @return empty RequestContext
   */
  public static RequestContext empty() {
    return EMPTY;
  }

  /**
   * Creates a key.
   *
   * @param name The key name.
   * @param <T> The type of value to be stored with the key.
   * @return a new key
   */
  public static <T> Key<T> newKey(String name) {
    return new KeyImpl<>(name);
  }

  /**
   * Interface for RequestContext keys.
   *
   * @param <T> the type of data stored with the key
   */
  public interface Key<T> {
    String getName();
  }

  /**
   * Key impl
   *
   * @param <T> the type of data stored with the key
   */
  private static class KeyImpl<T> implements Key<T> {
    private final String name;

    public KeyImpl(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }
  }
}
