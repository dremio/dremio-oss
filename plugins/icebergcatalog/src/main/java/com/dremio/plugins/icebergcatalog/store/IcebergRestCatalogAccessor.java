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

import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS;

import com.dremio.options.OptionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.RESTCatalog;

@Deprecated
public class IcebergRestCatalogAccessor extends AbstractRestCatalogAccessor {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(IcebergRestCatalogAccessor.class);

  private final Supplier<Catalog> catalogSupplier;
  private final ExpiringCatalogCache catalogCache;

  public IcebergRestCatalogAccessor(
      Supplier<Catalog> catalogSupplier,
      OptionManager optionsManager,
      List<String> allowedNamespaces,
      boolean isRecursiveAllowedNamespaces) {
    this(
        catalogSupplier,
        optionsManager,
        allowedNamespaces,
        isRecursiveAllowedNamespaces,
        UnaryOperator.identity());
  }

  /**
   * @param redactor masks the source's secret values in server-provided text (see {@link
   *     AbstractRestCatalogAccessor})
   */
  public IcebergRestCatalogAccessor(
      Supplier<Catalog> catalogSupplier,
      OptionManager optionsManager,
      List<String> allowedNamespaces,
      boolean isRecursiveAllowedNamespaces,
      UnaryOperator<String> redactor) {
    this(
        catalogSupplier,
        new ExpiringCatalogCache(
            catalogSupplier,
            optionsManager.getOption(RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS),
            TimeUnit.SECONDS),
        optionsManager,
        allowedNamespaces,
        isRecursiveAllowedNamespaces,
        redactor);
  }

  @VisibleForTesting
  IcebergRestCatalogAccessor(
      Supplier<Catalog> catalogSupplier,
      ExpiringCatalogCache catalogCache,
      OptionManager optionsManager,
      List<String> allowedNamespaces,
      boolean isRecursiveAllowedNamespaces,
      UnaryOperator<String> redactor) {
    super(catalogCache, optionsManager, allowedNamespaces, isRecursiveAllowedNamespaces, redactor);
    this.catalogSupplier = catalogSupplier;
    this.catalogCache = catalogCache;
  }

  /**
   * Thrown by {@link #checkState()} when the catalog authenticated the client but denied listing
   * the top-level namespaces (HTTP 403). The connection itself works; whether this matters depends
   * on the namespaces the source discovers.
   */
  public static final class NamespaceListingForbiddenException extends RuntimeException {
    NamespaceListingForbiddenException(ForbiddenException cause) {
      super(cause.getMessage(), cause);
    }
  }

  /**
   * Checks that the catalog is reachable and accepts the configured credentials.
   *
   * <p>The check reuses the cached catalog client (and its OAuth2 session, which the Iceberg client
   * refreshes on its own) and makes one cheap authenticated request: listing the top-level
   * namespaces. A new client (configuration request plus a new token) is built only when no client
   * is cached yet, or when the catalog rejects the cached client's session with HTTP 401 (for
   * example after the catalog restarted with new signing keys): then a new client tells a stale
   * session apart from a configuration problem, and replaces the cached client if it works. Every
   * other failure (network failures, timeouts, HTTP 5xx, 429, ...) is reported right away and keeps
   * the cached client: a new client would not fix it, and replacing a working client closes it
   * under the queries that use it.
   */
  @Override
  protected void checkStateInternal() throws Exception {
    Catalog cached = catalogCache.getIfPresent();
    if (cached == null) {
      // Builds (and caches) the client the same way queries do; failures propagate.
      probe(catalogCache.get());
      return;
    }

    try {
      probe(cached);
      return;
    } catch (NotAuthorizedException e) {
      logger.debug(
          "The Iceberg REST catalog rejected the session of the cached client (HTTP 401);"
              + " retrying the health check with a new client.");
    }

    Catalog fresh = catalogSupplier.get();
    try {
      probe(fresh);
    } catch (NamespaceListingForbiddenException e) {
      // The new client authenticated: it replaces the stale one either way.
      replaceCachedClient(fresh);
      throw e;
    } catch (RuntimeException e) {
      ExpiringCatalogCache.closeQuietly(fresh);
      throw e;
    }
    replaceCachedClient(fresh);
  }

  private void replaceCachedClient(Catalog fresh) {
    catalogCache.replace(fresh);
    // The cached tables and views hold table operations bound to the replaced (closed) client.
    invalidateCachedTablesAndViews();
    logger.info(
        "Replaced the cached Iceberg REST catalog client after it failed a health check that a new"
            + " client passed.");
  }

  private static void probe(Catalog catalog) {
    if (!(catalog instanceof SupportsNamespaces)) {
      return;
    }
    try {
      ((SupportsNamespaces) catalog).listNamespaces(Namespace.empty());
    } catch (ForbiddenException e) {
      throw new NamespaceListingForbiddenException(e);
    }
  }

  @Override
  protected boolean invalidatesCachesOnFailure(Exception failure) {
    // A denied root listing says nothing about the cached tables and views. With allowed
    // namespaces the source reports it as healthy, and it then occurs on every state check.
    return !(failure instanceof NamespaceListingForbiddenException);
  }

  @Override
  public String getDefaultBaseLocation() {
    // The cached client: building a new one here would fetch a token and leak the client.
    Catalog catalog = getCatalog();
    Preconditions.checkState(
        catalog instanceof RESTCatalog, "Catalog is not an instance of RESTCatalog");
    Map<String, String> props = ((RESTCatalog) catalog).properties();
    return props.get(DEFAULT_BASE_LOCATION);
  }

  @Override
  public Transaction createTableTransactionForNewTable(
      TableIdentifier tableIdentifier, Schema schema) {
    return getCatalog().newCreateTableTransaction(tableIdentifier, schema);
  }
}
