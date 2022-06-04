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

import static com.dremio.common.utils.SqlUtils.quoteIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.DremioEmptyScope;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.api.CatalogEntity;
import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.api.Dataset;
import com.dremio.dac.api.File;
import com.dremio.dac.api.Folder;
import com.dremio.dac.api.Home;
import com.dremio.dac.api.Source;
import com.dremio.dac.api.Space;
import com.dremio.dac.explore.model.AnalyzeRequest;
import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.explore.model.SuggestionResponse;
import com.dremio.dac.explore.model.ValidationResponse;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataWrapper;
import com.dremio.dac.model.job.QueryError;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.util.JobRequestUtil;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.DremioSqlConformance;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.planner.sql.SQLAnalyzer;
import com.dremio.exec.planner.sql.SQLAnalyzerFactory;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.EagerCachingOptionManager;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.exec.server.options.QueryOptionManager;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.options.OptionManager;
import com.dremio.service.autocomplete.AutocompleteEngine;
import com.dremio.service.autocomplete.AutocompleteRequest;
import com.dremio.service.autocomplete.AutocompleteResponse;
import com.dremio.service.autocomplete.CompletionItem;
import com.dremio.service.autocomplete.ImmutableAutocompleteResponse;
import com.dremio.service.autocomplete.catalog.CatalogNode;
import com.dremio.service.autocomplete.catalog.CatalogNodeReaderImpl;
import com.dremio.service.autocomplete.catalog.CatalogNodeResolver;
import com.dremio.service.autocomplete.catalog.DatasetCatalogNode;
import com.dremio.service.autocomplete.catalog.FileCatalogNode;
import com.dremio.service.autocomplete.catalog.FolderCatalogNode;
import com.dremio.service.autocomplete.catalog.HomeCatalogNode;
import com.dremio.service.autocomplete.catalog.SourceCatalogNode;
import com.dremio.service.autocomplete.catalog.SpaceCatalogNode;
import com.dremio.service.autocomplete.columns.ColumnReader;
import com.dremio.service.autocomplete.columns.ColumnReaderImpl;
import com.dremio.service.autocomplete.columns.ColumnResolver;
import com.dremio.service.autocomplete.columns.DremioQueryParserImpl;
import com.dremio.service.autocomplete.functions.ParameterResolver;
import com.dremio.service.autocomplete.functions.SqlFunctionDictionary;
import com.dremio.service.autocomplete.tokens.ResourceBackedMarkovChainFactory;
import com.dremio.service.autocomplete.tokens.SqlTokenKindMarkovChain;
import com.dremio.service.autocomplete.tokens.TokenResolver;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.CompletionListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

/**
 * run external sql
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/sql")
public class SQLResource extends BaseResourceWithAllocator {
  private static SqlTokenKindMarkovChain MARKOV_CHAIN = ResourceBackedMarkovChainFactory.create("markov_chain_queries.sql");

  private final JobsService jobs;
  private final SecurityContext securityContext;
  private final SabotContext sabotContext;
  private final ProjectOptionManager projectOptionManager;
  private final CatalogServiceHelper catalogServiceHelper;

  @Inject
  public SQLResource(
    SabotContext sabotContext,
    JobsService jobs,
    SecurityContext securityContext,
    BufferAllocatorFactory allocatorFactory,
    ProjectOptionManager projectOptionManager,
    CatalogServiceHelper catalogServiceHelper) {
    super(allocatorFactory);
    this.jobs = jobs;
    this.securityContext = securityContext;
    this.sabotContext = sabotContext;
    this.projectOptionManager = projectOptionManager;
    this.catalogServiceHelper = catalogServiceHelper;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public JobDataFragment query(CreateFromSQL sql) {
    final SqlQuery query = JobRequestUtil.createSqlQuery(sql.getSql(), sql.getContext(), securityContext.getUserPrincipal().getName());
    // Pagination is not supported in this API, so we need to truncate the results to 500 records
    final CompletionListener listener = new CompletionListener();
    final JobId jobId = jobs.submitJob(SubmitJobRequest.newBuilder().setSqlQuery(query).setQueryType(QueryType.REST).build(), listener);
    listener.awaitUnchecked();
    return new JobDataWrapper(jobs, jobId, securityContext.getUserPrincipal().getName()).truncate(getOrCreateAllocator("query"), 500);
  }

  @POST
  @Path("/analyze/suggest")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public SuggestionResponse suggestSQL(AnalyzeRequest analyzeRequest) {
    final String sql = analyzeRequest.getSql();
    final List<String> context = analyzeRequest.getContext();
    final int cursorPosition = analyzeRequest.getCursorPosition();

    // Setup dependencies and execute suggestion acquisition
    SQLAnalyzer SQLAnalyzer =
      SQLAnalyzerFactory.createSQLAnalyzer(
        securityContext.getUserPrincipal().getName(), sabotContext, context, true, projectOptionManager);

    List<SqlMoniker> sqlEditorHints = SQLAnalyzer.suggest(sql, cursorPosition);

    // Build response object and return
    return buildSuggestionResponse(sqlEditorHints);
  }

  @POST
  @Path("/analyze/validate")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public ValidationResponse validateSQL(AnalyzeRequest analyzeRequest) {

    final String sql = analyzeRequest.getSql();
    final List<String> context = analyzeRequest.getContext();

    // Setup dependencies and execute validation
    SQLAnalyzer SQLAnalyzer =
      SQLAnalyzerFactory.createSQLAnalyzer(
        securityContext.getUserPrincipal().getName(), sabotContext, context, false, projectOptionManager);

    List<SqlAdvisor.ValidateErrorInfo> validationErrors = SQLAnalyzer.validate(sql);

    // Build response object and return
    return buildValidationResponse(validationErrors);
  }

  @POST
  @Path("/autocomplete")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public AutocompleteResponse getCompletions(AutocompleteRequest request) throws NamespaceException {
    Preconditions.checkNotNull(request);
    AutocompleteEngine autocompleteEngine = createAutocompleteEngine();
    List<CompletionItem> completionItems = autocompleteEngine.generateCompletions(
      request.getQuery(),
      request.getCursor());

    return ImmutableAutocompleteResponse.builder()
      .addAllItems(completionItems)
      .build();
  }

  /**
   * Builds the response object for query suggestions.
   *
   * @param suggestionList The suggestion list returned from the SqlAdvisor.
   * @return The built SuggestionResponse object or null if there are no suggestions.
   */
  public SuggestionResponse buildSuggestionResponse(List<SqlMoniker> suggestionList) {

    // Return empty response in REST request
    if (suggestionList == null || suggestionList.isEmpty()) {
      return null;
    }

    // Create and populate suggestion response list
    List<SuggestionResponse.Suggestion> suggestions = new ArrayList<>();
    for (SqlMoniker hint : suggestionList) {

      // Quote the identifiers if they are not keywords or functions,
      // and are required to be quoted.
      List<String> qualifiedNames = hint.getFullyQualifiedNames();
      if ((hint.getType() != SqlMonikerType.KEYWORD) && (hint.getType() != SqlMonikerType.FUNCTION)) {
        qualifiedNames = qualifiedNames.stream().map(name -> quoteIdentifier(name)).collect(Collectors.toList());
      }

      suggestions.add(
        new SuggestionResponse.Suggestion(Joiner.on(".").join(qualifiedNames), hint.getType().name()));
    }

    SuggestionResponse response = new SuggestionResponse(suggestions);
    return response;
  }

  /**
   * Builds the response object for query validation.
   *
   * @param errorList The list of query errors returned from the SqlAdvisor.
   * @return The built ValidationResponse object or null if there are no available validation errors.
   */
  protected ValidationResponse buildValidationResponse(List<SqlAdvisor.ValidateErrorInfo> errorList) {

    // Return empty response in REST request
    if (errorList == null || errorList.isEmpty()) {
      return null;
    }

    // Create and populate error response list
    List<QueryError> sqlErrors = new ArrayList<>();
    for (SqlAdvisor.ValidateErrorInfo error : errorList) {
      sqlErrors.add(
        new QueryError(error.getMessage(),
          new QueryError.Range(error.getStartLineNum(),
            error.getStartColumnNum(),
            error.getEndLineNum() + 1,
            error.getEndColumnNum() + 1)));
    }

    ValidationResponse response = new ValidationResponse(sqlErrors);
    return response;
  }

  private AutocompleteEngine createAutocompleteEngine() throws NamespaceException {
    String username = securityContext.getUserPrincipal().getName();
    ViewExpansionContext viewExpansionContext = new ViewExpansionContext(new CatalogUser(username));
    OptionManager optionManager = OptionManagerWrapper.Builder.newBuilder()
      .withOptionManager(new DefaultOptionManager(sabotContext.getOptionValidatorListing()))
      .withOptionManager(new EagerCachingOptionManager(projectOptionManager))
      .withOptionManager(new QueryOptionManager(sabotContext.getOptionValidatorListing()))
      .build();
    SchemaConfig newSchemaConfig = SchemaConfig.newBuilder(CatalogUser.from(username))
      .defaultSchema(null)
      .optionManager(optionManager)
      .setViewExpansionContext(viewExpansionContext)
      .build();

    Catalog catalog = sabotContext
      .getCatalogService()
      .getCatalog(MetadataRequestOptions.of(newSchemaConfig));
    DremioCatalogReader catalogReader = new DremioCatalogReader(catalog, JavaTypeFactoryImpl.INSTANCE);
    ColumnReader columnReader = new ColumnReaderImpl(catalogReader);

    FunctionImplementationRegistry functionImplementationRegistry;
    if (optionManager.getOption(PlannerSettings.ENABLE_DECIMAL_V2_KEY).getBoolVal()) {
      functionImplementationRegistry = sabotContext.getDecimalFunctionImplementationRegistry();
    } else {
      functionImplementationRegistry = sabotContext.getFunctionImplementationRegistry();
    }

    DremioQueryParserImpl dremioQueryParser = new DremioQueryParserImpl(
      functionImplementationRegistry,
      catalog,
      username);
    ColumnResolver columnResolver = new ColumnResolver(
      columnReader,
      dremioQueryParser);

    TokenResolver tokenResolver = new TokenResolver(MARKOV_CHAIN);

    List<CatalogItem> rootItems = catalogServiceHelper.getTopLevelCatalogItems(Collections.EMPTY_LIST);
    HomeCatalogNode homeCatalogNode = new HomeCatalogNode(
      "TOP LEVEL CATALOG ITEMS",
      createFromCatalogItems(rootItems, catalogServiceHelper));
    CatalogNodeResolver catalogNodeResolver = new CatalogNodeResolver(new CatalogNodeReaderImpl(homeCatalogNode));

    OperatorTable opTable = new OperatorTable(functionImplementationRegistry);
    SqlOperatorTable chainedOpTable = new ChainedSqlOperatorTable(ImmutableList.<SqlOperatorTable>of(opTable, catalogReader));
    List<SqlFunction> sqlFunctions = new ArrayList<>();
    for (SqlOperator sqlOperator : chainedOpTable.getOperatorList()) {
      if (sqlOperator instanceof SqlFunction) {
        SqlFunction sqlFunction = (SqlFunction) sqlOperator;
        sqlFunctions.add(sqlFunction);
      }
    }

    SqlFunctionDictionary sqlFunctionDictionary = new SqlFunctionDictionary(sqlFunctions);

    SqlValidatorImpl validator = (SqlValidatorImpl) SqlValidatorUtil.newValidator(
      chainedOpTable,
      catalogReader,
      JavaTypeFactoryImpl.INSTANCE,
      SqlAdvisorValidator.Config.DEFAULT.withSqlConformance(DremioSqlConformance.INSTANCE));
    SqlValidatorScope sqlValidatorScope = new DremioEmptyScope(validator);

    ParameterResolver parameterResolver = new ParameterResolver(
      sqlFunctionDictionary,
      validator,
      sqlValidatorScope,
      dremioQueryParser);

    AutocompleteEngine autocompleteEngine = new AutocompleteEngine(
      tokenResolver,
      columnResolver,
      catalogNodeResolver,
      parameterResolver);

    return autocompleteEngine;
  }

  private static CatalogNode createFromCatalogEntity(
    CatalogEntity catalogEntity,
    CatalogServiceHelper catalogServiceHelper) {
    try {
      CatalogNode node;
      if (catalogEntity instanceof Home) {
        Home home = (Home) catalogEntity;
        node = new HomeCatalogNode(
          home.getName(),
          createFromCatalogItems(
            home.getChildren(),
            catalogServiceHelper));
      } else if (catalogEntity instanceof Space) {
        Space space = (Space) catalogEntity;
        node = new SpaceCatalogNode(
          space.getName(),
          createFromCatalogItems(
            space.getChildren(),
            catalogServiceHelper));
      } else if (catalogEntity instanceof Folder) {
        Folder folder = (Folder) catalogEntity;
        node = new FolderCatalogNode(
          folder.getId(),
          createFromCatalogItems(
            folder.getChildren(),
            catalogServiceHelper));
      } else if (catalogEntity instanceof Source) {
        Source source = (Source) catalogEntity;
        node = new SourceCatalogNode(
          source.getName(),
          createFromCatalogItems(
            source.getChildren(),
            catalogServiceHelper));
      } else if (catalogEntity instanceof Dataset) {
        Dataset dataset = (Dataset) catalogEntity;
        node = new DatasetCatalogNode(
          dataset.getPath().get(dataset.getPath().size() - 1),
          dataset.getType() == Dataset.DatasetType.PHYSICAL_DATASET ? DatasetCatalogNode.Type.Physical : DatasetCatalogNode.Type.Virtual);
      } else if (catalogEntity instanceof File) {
        File file = (File) catalogEntity;
        node = new FileCatalogNode(file.getId());
      } else {
        throw new NotSupportedException("Can't convert catalog entity: " + catalogEntity.getId());
      }

      return node;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static ImmutableList<CatalogNode> createFromCatalogItems(
    List<CatalogItem> catalogEntityChildren,
    CatalogServiceHelper catalogServiceHelper) throws NamespaceException {
    ImmutableList.Builder<CatalogNode> builder = new ImmutableList.Builder<>();
    for (CatalogItem catalogItem : catalogEntityChildren) {
      CatalogEntity childCatalogEntity;
      if (catalogItem.getContainerType() == CatalogItem.ContainerSubType.SOURCE) {
        // For some reason we get a null pointer exception when we try to read a SOURCE.
        continue;
      }

      try {
        childCatalogEntity = (CatalogEntity) catalogServiceHelper
          .getCatalogEntityById(
            catalogItem.getId(),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST)
          .get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      CatalogNode childCatalogNode = createFromCatalogEntity(
        childCatalogEntity,
        catalogServiceHelper);
      builder.add(childCatalogNode);
    }

    return builder.build();
  }
}

