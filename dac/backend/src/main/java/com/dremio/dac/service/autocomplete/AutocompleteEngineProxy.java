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
package com.dremio.dac.service.autocomplete;

import java.util.List;

import javax.ws.rs.core.SecurityContext;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.EagerCachingOptionManager;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.exec.server.options.QueryOptionManager;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.options.OptionManager;
import com.dremio.service.autocomplete.AutocompleteEngine;
import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.catalog.AutocompleteSchemaProvider;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.nessie.NessieElementReader;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.base.Supplier;

/**
 * Factory for creating Autocomplete engines.
 */
public final class AutocompleteEngineProxy {
  public static Completions getCompletions(
    SecurityContext securityContext,
    SabotContext sabotContext,
    ProjectOptionManager projectOptionManager,
    List<String> context,
    String corpus,
    int cursorPosition) {
    String username = securityContext.getUserPrincipal().getName();
    ViewExpansionContext viewExpansionContext = new ViewExpansionContext(new CatalogUser(username));
    OptionManager optionManager = OptionManagerWrapper.Builder.newBuilder()
      .withOptionManager(new DefaultOptionManager(sabotContext.getOptionValidatorListing()))
      .withOptionManager(new EagerCachingOptionManager(projectOptionManager))
      .withOptionManager(new QueryOptionManager(sabotContext.getOptionValidatorListing()))
      .build();
    NamespaceKey defaultSchemaPath = context.size() == 0 ? null : new NamespaceKey(context);
    SchemaConfig newSchemaConfig = SchemaConfig.newBuilder(CatalogUser.from(username))
      .defaultSchema(defaultSchemaPath)
      .optionManager(optionManager)
      .setViewExpansionContext(viewExpansionContext)
      .build();

    Catalog catalog = sabotContext
      .getCatalogService()
      .getCatalog(MetadataRequestOptions.of(newSchemaConfig));

    FunctionImplementationRegistry functionImplementationRegistry;
    if (optionManager.getOption(PlannerSettings.ENABLE_DECIMAL_V2_KEY).getBoolVal()) {
      functionImplementationRegistry = sabotContext.getDecimalFunctionImplementationRegistry();
    } else {
      functionImplementationRegistry = sabotContext.getFunctionImplementationRegistry();
    }

    OperatorTable opTable = new OperatorTable(functionImplementationRegistry);
    NamespaceService namespaceService = sabotContext.getNamespaceServiceFactory().get(username);

    AutocompleteSchemaProvider autocompleteSchemaProvider = new AutocompleteSchemaProvider(
      username,
      namespaceService,
      sabotContext.getConnectionReaderProvider().get(),
      catalog,
      context);
    Supplier<NessieElementReader> nessieElementReaderSupplier = () -> new NessieElementReaderImpl(sabotContext.getNessieClientProvider().get());

    AutocompleteEngineContext autocompleteEngineContext = new AutocompleteEngineContext(
      autocompleteSchemaProvider,
      nessieElementReaderSupplier,
      opTable,
      catalog);
    return AutocompleteEngine.generateCompletions(
      autocompleteEngineContext,
      corpus,
      cursorPosition);
  }
}
