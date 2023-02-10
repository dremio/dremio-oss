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
package com.dremio.dac.resource;

import static java.util.stream.Collectors.groupingBy;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.ws.rs.core.SecurityContext;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.EagerCachingOptionManager;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.exec.server.options.QueryOptionManager;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.options.OptionManager;
import com.dremio.service.functions.FunctionListDictionary;
import com.dremio.service.functions.model.Function;
import com.dremio.service.functions.model.FunctionCategory;
import com.dremio.service.functions.model.FunctionSignature;
import com.dremio.service.functions.model.Parameter;
import com.dremio.service.functions.model.SampleCode;
import com.dremio.service.namespace.NamespaceKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * Resource for exposing the Function List Service located in dremio-services-functions.
 *
 * Exposes metadata information about all functions needed for features like:
 *
 * 1) Autocomplete Function Completion
 * 2) Public Docs
 * 3) UI Functions Side Panel
 */

public final class FunctionsListService {
  private final SecurityContext securityContext;
  private final SabotContext sabotContext;
  private final ProjectOptionManager projectOptionManager;

  public FunctionsListService(
    SabotContext sabotContext,
    SecurityContext securityContext,
    ProjectOptionManager projectOptionManager) {

    this.securityContext = securityContext;
    this.sabotContext = sabotContext;
    this.projectOptionManager = projectOptionManager;
  }

  public Response getFunctions() {
    FunctionSource functionSource = getFunctionSource(
      securityContext,
      sabotContext,
      projectOptionManager);
    List<Function> functions = getFunctions(functionSource)
      .stream()
      .map(FunctionsListService::removeDefaults)
      .collect(Collectors.toList());

    return new FunctionsListService.Response(functions);
  }

  private static FunctionSource getFunctionSource(
    SecurityContext securityContext,
    SabotContext sabotContext,
    ProjectOptionManager projectOptionManager) {
    String username = securityContext.getUserPrincipal().getName();
    ViewExpansionContext viewExpansionContext = new ViewExpansionContext(new CatalogUser(username));
    OptionManager optionManager = OptionManagerWrapper.Builder.newBuilder()
      .withOptionManager(new DefaultOptionManager(sabotContext.getOptionValidatorListing()))
      .withOptionManager(new EagerCachingOptionManager(projectOptionManager))
      .withOptionManager(new QueryOptionManager(sabotContext.getOptionValidatorListing()))
      .build();
    NamespaceKey defaultSchemaPath = null;
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

    OperatorTable operatorTable = new OperatorTable(functionImplementationRegistry);

    return new FunctionSource(catalog, operatorTable);
  }

  private static List<Function> getFunctions(FunctionSource functionSource) {
    DremioCatalogReader catalogReader = new DremioCatalogReader(
      functionSource.getCatalog(),
      JavaTypeFactoryImpl.INSTANCE);
    SqlOperatorTable chainedOperatorTable = ChainedSqlOperatorTable.of(
      functionSource.getSqlOperatorTable(),
      catalogReader);

    Map<String, List<SqlFunction>> functionsGroupedByName =  chainedOperatorTable
      .getOperatorList()
      .stream()
      .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
      .map(sqlOperator -> (SqlFunction) sqlOperator)
      .collect(groupingBy(sqlFunction -> sqlFunction.getName().toUpperCase()));

    List<Function> convertedFunctions = new ArrayList<>();
    for (String functionName : functionsGroupedByName.keySet()) {
      Optional<Function> optionalFunction = FunctionListDictionary.tryGetFunction(functionName);
      if (optionalFunction.isPresent()) {
        convertedFunctions.add(optionalFunction.get());
      }
    }

    return convertedFunctions
      .stream()
      .sorted(Comparator.comparing(function -> function.getName()))
      .collect(Collectors.toList());
  }

  private static Function removeDefaults(Function function) {
    String description = valueOrNullIfDefault(function.getDescription());
    String dremioVersion = valueOrNullIfDefault(function.getDremioVersion());
    List<FunctionCategory> functionCategories = function.getFunctionCategories();
    if ((functionCategories != null) && functionCategories.isEmpty()) {
      functionCategories = null;
    }

    return Function.builder()
      .name(function.getName())
      .addAllSignatures(function
        .getSignatures()
        .stream()
        .map(FunctionsListService::removeDefaults)
        .collect(Collectors.toList()))
      .description(description)
      .dremioVersion(dremioVersion)
      .functionCategories(functionCategories)
      .build();
  }

  private static FunctionSignature removeDefaults(FunctionSignature functionSignature) {
    String description = valueOrNullIfDefault(functionSignature.getDescription());
    List<SampleCode> sampleCodes = functionSignature.getSampleCodes();
    if (sampleCodes != null) {
      sampleCodes = sampleCodes
        .stream()
        .filter(sampleCode -> !isDefaultSampleCode(sampleCode))
        .collect(Collectors.toList());
      if (sampleCodes.isEmpty()) {
        sampleCodes = null;
      }
    }

    return FunctionSignature.builder()
      .returnType(functionSignature.getReturnType())
      .addAllParameters(functionSignature
        .getParameters()
        .stream()
        .map(FunctionsListService::removeDefaults)
        .collect(Collectors.toList()))
      .description(description)
      .sampleCodes(sampleCodes)
      .snippetOverride(functionSignature.getSnippetOverride())
      .build();
  }

  private static boolean isDefaultSampleCode(SampleCode sampleCode) {
    return isStringDefault(sampleCode.getCall()) || isStringDefault(sampleCode.getResult());
  }

  private static Parameter removeDefaults(Parameter parameter) {
    String name = valueOrNullIfDefault(parameter.getName());
    String description = valueOrNullIfDefault(parameter.getDescription());
    String format = valueOrNullIfDefault(parameter.getFormat());

    return Parameter.builder()
      .kind(parameter.getKind())
      .type(parameter.getType())
      .name(name)
      .description(description)
      .format(format)
      .build();
  }

  private static String valueOrNullIfDefault(String value) {
    return isStringDefault(value) ? null : value;
  }

  private static boolean isStringDefault(String value) {
    return (value != null) && value.startsWith("<") && value.endsWith(">");
  }

  public static final class Response {
    private final List<Function> functions;

    @JsonCreator
    private Response(
      @JsonProperty("functions") List<Function> functions) {
      Preconditions.checkNotNull(functions);
      this.functions = functions;
    }

    public List<Function> getFunctions() {
      return functions;
    }
  }

  private static final class FunctionSource {
    private final SimpleCatalog<?> catalog;
    private final SqlOperatorTable sqlOperatorTable;

    private FunctionSource(SimpleCatalog<?> catalog, SqlOperatorTable sqlOperatorTable) {
      Preconditions.checkNotNull(catalog);
      Preconditions.checkNotNull(sqlOperatorTable);

      this.catalog = catalog;
      this.sqlOperatorTable = sqlOperatorTable;
    }

    public SimpleCatalog<?> getCatalog() {
      return catalog;
    }

    public SqlOperatorTable getSqlOperatorTable() {
      return sqlOperatorTable;
    }
  }
}
