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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Utilities for AutoCloseable classes.
 */
public final class AutoCloseables {
  // Utility class. Should not be instantiated
  private AutoCloseables() {
  }

  public static AutoCloseable all(final Collection<? extends AutoCloseable> autoCloseables) {
    return new AutoCloseable() {
      @Override
      public void close() throws Exception {
        AutoCloseables.close(autoCloseables);
      }
    };
  }

  /**
   * Closes all autoCloseables if not null and suppresses exceptions by adding them to t
   *
   * @param t              the throwable to add suppressed exception to
   * @param autoCloseables the closeables to close
   */
  public static void close(Throwable t, AutoCloseable... autoCloseables) {
    close(t, Arrays.asList(autoCloseables));
  }

  /**
   * Closes all autoCloseables if not null and suppresses exceptions by adding them to t
   *
   * @param t              the throwable to add suppressed exception to
   * @param autoCloseables the closeables to close
   */
  public static void close(Throwable t, Iterable<? extends AutoCloseable> autoCloseables) {
    try {
      close(autoCloseables);
    } catch (Exception e) {
      t.addSuppressed(e);
    }
  }

  /**
   * Closes all autoCloseables if not null and suppresses subsequent exceptions if more than one
   *
   * @param autoCloseables the closeables to close
   */
  public static void close(AutoCloseable... autoCloseables) throws Exception {
    close(Arrays.asList(autoCloseables));
  }

  /**
   * Close with an expected exception class. This method wraps any checked exception to an expected type.
   * The exception class should have a constructor that takes Exception object as a parameter.
   *
   * @param exceptionClazz
   * @param autoCloseables
   * @param <E>
   * @throws E
   */
  public static <E extends Throwable> void close(Class<E> exceptionClazz, AutoCloseable... autoCloseables) throws E {
    try {
      close(autoCloseables);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      if (exceptionClazz.isInstance(e) || RuntimeException.class.isInstance(e)) {
        throw (E) e;
      }

      try {
        Constructor<E> constructor = exceptionClazz.getDeclaredConstructor(new Class[]{Throwable.class});
        throw constructor.newInstance(new Object[]{e});
      } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Closes all autoCloseables if not null and suppresses subsequent exceptions if more than one
   *
   * @param ac the closeables to close
   */
  public static void close(Iterable<? extends AutoCloseable> ac) throws Exception {
    // this method can be called on a single object if it implements Iterable<AutoCloseable> like for example VectorContainer
    // make sure we handle that properly
    if (ac == null) {
      return;
    } else if (ac instanceof AutoCloseable) {
      ((AutoCloseable) ac).close();
      return;
    }

    Exception topLevelException = null;
    for (AutoCloseable closeable : ac) {
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (Exception e) {
        if (topLevelException == null) {
          topLevelException = e;
        } else if (e != topLevelException) {
          topLevelException.addSuppressed(e);
        }
      }
    }
    if (topLevelException != null) {
      throw topLevelException;
    }
  }

  @SafeVarargs
  public static void close(Iterable<? extends AutoCloseable>... closeables) throws Exception {
    close(Iterables.concat(closeables));
  }

  public static Iterable<AutoCloseable> iter(AutoCloseable... ac) {
    if (ac.length == 0) {
      return Collections.emptyList();
    } else {
      final List<AutoCloseable> nonNullAc = Lists.newArrayList();
      for (AutoCloseable autoCloseable : ac) {
        if (autoCloseable != null) {
          nonNullAc.add(autoCloseable);
        }
      }
      return nonNullAc;
    }
  }

  /**
   * A closeable wrapper that will close the underlying closeables if a commit does not occur.
   */
  public static class RollbackCloseable implements AutoCloseable {

    private boolean commit = false;
    private final boolean reverse;
    private List<AutoCloseable> closeables;

    public RollbackCloseable(boolean reverse, AutoCloseable... closeables) {
      this.closeables = new ArrayList<>(Arrays.asList(closeables));
      this.reverse = reverse;
    }

    public RollbackCloseable(AutoCloseable... closeables) {
      this(false, closeables);
    }


    public <T extends AutoCloseable> T add(T t) {
      closeables.add(t);
      return t;
    }

    public void addAll(AutoCloseable... list) {
      closeables.addAll(Arrays.asList(list));
    }

    public void addAll(Iterable<? extends AutoCloseable> list) {
      for (AutoCloseable ac : list) {
        closeables.add(ac);
      }
    }

    public List<AutoCloseable> getCloseables() {
      return Collections.unmodifiableList(closeables);
    }

    public void commit() {
      commit = true;
    }

    @Override
    public void close() throws Exception {
      if (!commit) {
        if (reverse) {
          Collections.reverse(closeables);
        }
        AutoCloseables.close(closeables);
      }
    }

  }

  public static RollbackCloseable rollbackable(AutoCloseable... closeables) {
    return new RollbackCloseable(closeables);
  }

  /**
   * close() an {@see java.lang.AutoCloseable} without throwing a (checked)
   * {@see java.lang.Exception}. This wraps the close() call with a
   * try-catch that will rethrow an Exception wrapped with a
   * {@see java.lang.RuntimeException}, providing a way to call close()
   * without having to do the try-catch everywhere or propagate the Exception.
   *
   * @param autoCloseable the AutoCloseable to close; may be null
   * @throws RuntimeException if an Exception occurs; the Exception is
   *                          wrapped by the RuntimeException
   */
  public static void closeNoChecked(final AutoCloseable autoCloseable) {
    if (autoCloseable != null) {
      try {
        autoCloseable.close();
      } catch (final Exception e) {
        throw new RuntimeException("Exception while closing: " + e.getMessage(), e);
      }
    }
  }

  private static final AutoCloseable noOpAutocloseable = new AutoCloseable() {
    @Override
    public void close() {
    }
  };

  /**
   * @return A do-nothing autocloseable
   */
  public static AutoCloseable noop() {
    return noOpAutocloseable;
  }
}
