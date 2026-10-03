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
package com.dremio.plugins.icebergcatalog.store;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;

// Based on com.google.common.base.Suppliers.ExpiringMemoizingSupplier
public class ExpiringCatalogCache implements Supplier<Catalog>, Closeable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ExpiringCatalogCache.class);
  private final Object lock = new Object();
  private final Supplier<Catalog> catalogSupplier;
  private final long durationNanos;
  private volatile Catalog catalog;
  private volatile long expirationNanos;

  /** Set by {@link #close()}; a client handed to {@link #replace} afterwards is closed at once. */
  private boolean closed;

  ExpiringCatalogCache(Supplier<Catalog> catalogSupplier, long duration, TimeUnit unit) {
    this(catalogSupplier, unit.toNanos(duration));
  }

  ExpiringCatalogCache(Supplier<Catalog> catalogSupplier, long durationNanos) {
    this.catalogSupplier = catalogSupplier;
    this.durationNanos = durationNanos;
    this.catalog = null;
    this.expirationNanos = 0;
  }

  public void invalidate() {
    synchronized (lock) {
      expirationNanos = 0;
      if (catalog != null) {
        closeQuietly(catalog);
        catalog = null;
      }
    }
  }

  /** Closes a catalog client that is no longer used; a failure only matters for debugging. */
  static void closeQuietly(Catalog toClose) {
    if (toClose instanceof Closeable) {
      try {
        ((Closeable) toClose).close();
      } catch (Exception e) {
        logger.debug("Failed to close an Iceberg REST catalog client.", e);
      }
    }
  }

  /**
   * Returns the cached catalog if one has been built and has not expired yet, without building a
   * new one (and therefore without contacting the catalog service).
   */
  @Nullable
  public Catalog getIfPresent() {
    long nanos = expirationNanos;
    Catalog current = catalog;
    if (current == null || nanos == 0 || System.nanoTime() - nanos >= 0) {
      return null;
    }
    return current;
  }

  /**
   * Replaces the cached catalog with an already initialized one (closing the previous one) and
   * restarts the expiration period. Used when a newly built client works while the cached one does
   * not, e.g. after the catalog service restarted and the cached session can no longer
   * authenticate. If the cache was closed in the meantime, the replacement is closed instead.
   */
  public void replace(Catalog replacement) {
    Preconditions.checkArgument(
        replacement instanceof RESTCatalog, "RESTCatalog instance expected");
    Catalog toClose;
    synchronized (lock) {
      if (closed) {
        toClose = replacement;
      } else {
        // Swap before closing: a concurrent get() on its fast path must never see a null catalog.
        toClose = catalog == replacement ? null : catalog;
        catalog = replacement;
        expirationNanos = System.nanoTime() + durationNanos + 1;
      }
    }
    if (toClose != null) {
      closeQuietly(toClose);
    }
  }

  private Supplier<Catalog> getCatalogSupplier() {
    return catalogSupplier;
  }

  @Override
  public Catalog get() {
    // double-checked locking
    long nanos = expirationNanos;
    long now = System.nanoTime();
    if (expirationNanos == 0 || now - expirationNanos >= 0) {
      synchronized (lock) {
        if (nanos == expirationNanos) {
          invalidate();
          catalog = getCatalogSupplier().get();
          Preconditions.checkArgument(
              catalog instanceof RESTCatalog, "RESTCatalog instance expected");
          expirationNanos = now + durationNanos + 1;
        }
      }
    }
    return catalog;
  }

  @Override
  public void close() {
    synchronized (lock) {
      closed = true;
      invalidate();
    }
  }
}
