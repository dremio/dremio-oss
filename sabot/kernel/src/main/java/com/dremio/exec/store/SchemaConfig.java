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
package com.dremio.exec.store;

import org.apache.calcite.schema.SchemaPlus;

import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

/**
 * Contains information needed by Catalog implementations
 */
public class SchemaConfig {
  private final AuthorizationContext authContext;
  private final boolean exposeInternalSources;
  private final NamespaceKey defaultSchema;
  private final OptionManager optionManager;
  private final ViewExpansionContext viewExpansionContext;
  private final Predicate<DatasetConfig> validityChecker;

  private SchemaConfig(
      final AuthorizationContext authContext,
      final NamespaceKey defaultSchema,
      final OptionManager optionManager,
      final ViewExpansionContext viewExpansionContext,
      boolean exposeInternalSources,
      Predicate<DatasetConfig> validityChecker) {
    this.authContext = authContext;
    this.viewExpansionContext = viewExpansionContext;
    this.defaultSchema = defaultSchema;
    this.optionManager = optionManager;
    this.exposeInternalSources = exposeInternalSources;
    this.validityChecker = validityChecker;
  }

  /**
   * Create new builder.
   * @param subject Subject accessing the storage sources.
   */
  public static Builder newBuilder(final CatalogIdentity subject) {
    return new Builder(subject);
  }

  public static class Builder {
    private final CatalogIdentity subject;
    private boolean ignoreAuthErrors;
    private boolean exposeInternalSources = false;
    private NamespaceKey defaultSchema;
    private OptionManager optionManager;
    private ViewExpansionContext viewExpansionContext;
    private Predicate<DatasetConfig> validityChecker = Predicates.alwaysTrue();

    private Builder(final CatalogIdentity subject) {
      this.subject = Preconditions.checkNotNull(subject);
    }

    public Builder setViewExpansionContext(ViewExpansionContext viewExpansionContext) {
      this.viewExpansionContext = viewExpansionContext;
      return this;
    }
    public Builder setIgnoreAuthErrors(boolean ignoreAuthErrors) {
      this.ignoreAuthErrors = ignoreAuthErrors;
      return this;
    }

    public Builder optionManager(OptionManager optionManager) {
      this.optionManager = optionManager;
      return this;
    }

    public Builder exposeInternalSources(boolean exposeInternalSources) {
      this.exposeInternalSources = exposeInternalSources;
      return this;
    }

    public Builder defaultSchema(NamespaceKey defaultSchema) {
      this.defaultSchema = defaultSchema;
      return this;
    }

    /**
     * Predicate to apply to check if a dataset is valid.
     *
     * @param validityChecker predicate
     * @return this builder
     */
    public Builder setDatasetValidityChecker(Predicate<DatasetConfig> validityChecker) {
      Preconditions.checkNotNull(validityChecker, "dataset validity checker must be set");
      this.validityChecker = validityChecker;
      return this;
    }

    public SchemaConfig build() {
      return new SchemaConfig(
          new AuthorizationContext(subject, ignoreAuthErrors),
          defaultSchema,
          optionManager,
          viewExpansionContext,
          exposeInternalSources,
          validityChecker);
    }
  }

  public AuthorizationContext getAuthContext() {
    return authContext;
  }

  /**
   * @return User whom to impersonate as while creating {@link SchemaPlus} instances
   * interact with the underlying storage.
   *
   * TODO: move to passing subject around vs the username.
   */
  public String getUserName() {
    return authContext.getSubject().getName();
  }

  public NamespaceKey getDefaultSchema() {
    return defaultSchema;
  }

  /**
   * @return Should ignore if authorization errors are reported while {@link SchemaPlus}
   * instances interact with the underlying storage.
   */
  public boolean getIgnoreAuthErrors() {
    return authContext.getIgnoreAuthErrors();
  }

  public boolean exposeInternalSources() {
    return exposeInternalSources;
  }

  public OptionManager getOptions() {
    return optionManager;
  }

  public boolean isSystemUser() {
    return getUserName().equals(SystemUser.SYSTEM_USERNAME);
  }

  public OptionValue getOption(String optionKey) {
    return optionManager.getOption(optionKey);
  }

  public ViewExpansionContext getViewExpansionContext() {
    return viewExpansionContext;
  }

  public Predicate<DatasetConfig> getDatasetValidityChecker() {
    return validityChecker;
  }

}
