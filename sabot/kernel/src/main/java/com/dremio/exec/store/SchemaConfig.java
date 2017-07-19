/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.server.options.OptionValue;
import com.google.common.base.Preconditions;

/**
 * Contains information needed by {@link com.dremio.exec.store.AbstractSchema} implementations.
 */
public class SchemaConfig {
  private transient final SchemaInfoProvider provider;
  private final AuthorizationContext authContext;
  private final boolean exposeSubSchemasAsTopLevelSchemas;

  private SchemaConfig(final SchemaInfoProvider provider, final AuthorizationContext authContext,
      final boolean exposeSubSchemasAsTopLevelSchemas) {
    this.provider = provider;
    this.authContext = authContext;
    this.exposeSubSchemasAsTopLevelSchemas = exposeSubSchemasAsTopLevelSchemas;
  }

  /**
   * Create new builder.
   * @param username Name of the user accessing the storage sources.
   */
  public static Builder newBuilder(final String username) {
    return new Builder(username);
  }

  public static class Builder {
    final String username;
    SchemaInfoProvider provider;
    boolean ignoreAuthErrors;
    boolean exposeSubSchemasAsTopLevelSchemas;

    private Builder(final String username) {
      this.username = Preconditions.checkNotNull(username);
    }

    public Builder setProvider(final SchemaInfoProvider provider) {
      this.provider = provider;
      return this;
    }

    public Builder setIgnoreAuthErrors(boolean ignoreAuthErrors) {
      this.ignoreAuthErrors = ignoreAuthErrors;
      return this;
    }

    /**
     * Whether to expose the nested subschemas as top level schemas by adding a short cut to subschema at top level.
     * Useful in cases where a client can not understand more than one level of schemas such as Tableau.
     * @param exposeSubSchemasAsTopLevelSchemas
     * @return
     */
    public Builder exposeSubSchemasAsTopLevelSchemas(boolean exposeSubSchemasAsTopLevelSchemas) {
      this.exposeSubSchemasAsTopLevelSchemas = exposeSubSchemasAsTopLevelSchemas;
      return this;
    }

    public SchemaConfig build() {
      return new SchemaConfig(provider, new AuthorizationContext(username, ignoreAuthErrors), exposeSubSchemasAsTopLevelSchemas);
    }
  }

  public AuthorizationContext getAuthContext() {
    return authContext;
  }

  /**
   * @return User whom to impersonate as while creating {@link SchemaPlus} instances
   * interact with the underlying storage.
   */
  public String getUserName() {
    return authContext.getUsername();
  }

  /**
   * @return Should ignore if authorization errors are reported while {@link SchemaPlus}
   * instances interact with the underlying storage.
   */
  public boolean getIgnoreAuthErrors() {
    return authContext.getIgnoreAuthErrors();
  }

  /**
   * Whether to expose the nested subschemas as top level schemas by adding a short cut to subschema at top level.
   * Useful in cases where a client can not understand more than one level of schemas such as Tableau.
   * @return
   */
  public boolean exposeSubSchemasAsTopLevelSchemas() {
    return exposeSubSchemasAsTopLevelSchemas;
  }

  public OptionValue getOption(String optionKey) {
    return provider.getOption(optionKey);
  }

  public ViewExpansionContext getViewExpansionContext() {
    return provider.getViewExpansionContext();
  }

  /**
   * Interface to implement to provide required info for {@link com.dremio.exec.store.SchemaConfig}
   */
  public interface SchemaInfoProvider {
    ViewExpansionContext getViewExpansionContext();

    OptionValue getOption(String optionKey);
  }
}
