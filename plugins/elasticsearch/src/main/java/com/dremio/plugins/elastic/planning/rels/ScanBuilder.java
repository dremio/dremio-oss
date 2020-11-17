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
package com.dremio.plugins.elastic.planning.rels;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptTable;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticTableXattr;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.store.SplitWork;
import com.dremio.plugins.elastic.ElasticsearchConf;
import com.dremio.plugins.elastic.planning.ElasticsearchGroupScan;
import com.dremio.plugins.elastic.planning.ElasticsearchScanSpec;
import com.dremio.plugins.elastic.planning.rules.ExpressionNotAnalyzableException;
import com.dremio.plugins.elastic.planning.rules.PredicateAnalyzer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


public class ScanBuilder {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanBuilder.class);

  private final static ImmutableSet<Class<?>> CONSUMEABLE_RELS = ImmutableSet.<Class<?>>of(ElasticsearchSample.class, ElasticsearchLimit.class, ElasticsearchFilter.class, ElasticIntermediateScanPrel.class);

  private ElasticsearchScanSpec spec;
  private ElasticIntermediateScanPrel scan;

  public GroupScan<SplitWork> toGroupScan(OpProps props, long estimatedRowCount){
    return new ElasticsearchGroupScan(props, spec, scan.getTableMetadata(), scan.getProjectedColumns(), estimatedRowCount);
  }

  public String getResource(){
    return spec.getResource();
  }

  public String getQuery(){
    return spec.getQuery();
  }

  public List<SchemaPath> getColumns(){
    return scan.getProjectedColumns();
  }

  public RelOptTable getTable() {
    return scan.getTable();
  }

  protected ElasticsearchScanSpec getSpec() {
    return spec;
  }

  protected void setSpec(ElasticsearchScanSpec spec) {
    this.spec = spec;
  }

  protected ElasticIntermediateScanPrel getScan() {
    return scan;
  }

  protected void setScan(ElasticIntermediateScanPrel scan) {
    this.scan = scan;
  }

  /**
   * Check the stack is valid for transformation. The following are
   *
   *   The stack must have a leaf that is an ElasticsearchScan
   *   The stack must not include a ElasitcsearchProject (this should have been removed in rel finalization prior to invoking ScanBuilder.
   *   The stack can only include the following rels (and only one each):
   *     ElasticsearchScanPrel, ElasticsearchFilter, ElasticsearchSample, ElasticsearchLimit
   *
   *   The order must be
   *   ElasticsearchSample (or ElasticsearchLimit) (optional)
   *       \
   *     ElasticsearchFilter (optional)
   *         \
   *       ElasticsearchScanPrel
   *
   * @param stack
   */
  private  Map<Class<?>, ElasticsearchPrel> validate(List<ElasticsearchPrel> stack){
    final Map<Class<?>, ElasticsearchPrel> map = new HashMap<>();
    for(int i =0; i < stack.size(); i++){
      ElasticsearchPrel prel = stack.get(i);
      if(!CONSUMEABLE_RELS.contains(prel.getClass())){
        throw new IllegalStateException(String.format("ScanBuilder can't consume a %s.", prel.getClass().getName()));
      }
      if(map.containsKey(prel.getClass())){
        throw new IllegalStateException(String.format("ScanBuilder found more than one %s.", prel.getClass().getName()));
      }

      map.put(prel.getClass(), prel);
    }

    switch(stack.size()){
    case 0:
      throw new IllegalStateException("Stacks must include a scan.");
    case 1:
      Preconditions.checkArgument(stack.get(0) instanceof ElasticIntermediateScanPrel);
      break;
    case 2:
      Preconditions.checkArgument(stack.get(0) instanceof ElasticsearchSample || stack.get(0) instanceof ElasticsearchFilter || stack.get(0) instanceof ElasticsearchLimit);
      Preconditions.checkArgument(stack.get(1) instanceof ElasticIntermediateScanPrel);
      break;
    case 3:
      Preconditions.checkArgument(stack.get(0) instanceof ElasticsearchSample || stack.get(0) instanceof ElasticsearchLimit);
      Preconditions.checkArgument(stack.get(1) instanceof ElasticsearchFilter);
      Preconditions.checkArgument(stack.get(2) instanceof ElasticIntermediateScanPrel);
      break;
    default:
      throw new IllegalStateException(String.format("Stack should 1..3 in size, was %d in size.", stack.size()));
    }
    return ImmutableMap.copyOf(map);
  }

  protected void applyFilter(SearchRequestBuilder searchRequest, ElasticIntermediateScanPrel scan, ElasticsearchFilter filter, ElasticTableXattr tableAttributes) throws ExpressionNotAnalyzableException {

    QueryBuilder b = null;
    if (tableAttributes.hasAliasFilter()) {
      b = new WrapperQueryBuilder(tableAttributes.getAliasFilter());
    }

    if (filter != null) {
      QueryBuilder filterQuery = PredicateAnalyzer.analyze(scan, filter.getCondition(), tableAttributes.getVariationDetected());
      if (b != null) {
        b = QueryBuilders.boolQuery().must(filterQuery).must(b);
      } else {
        b = filterQuery;
      }
    }
    if (b != null) {
      searchRequest.setQuery(b);
    } else {
      searchRequest.setQuery(QueryBuilders.matchAllQuery());

    }

  }

  protected int applyFetch(SearchRequestBuilder searchRequest, ElasticsearchConf config, ElasticsearchLimit limit, ElasticsearchFilter filter, ElasticsearchSample sample){
    final int configuredFetchSize = config.getScrollSize();
    int fetch = configuredFetchSize;
    // If there is a limit or sample, add it to the search builder.
    if (limit != null) {
      fetch = limit.getFetchSize();
    } else if (sample != null) {
      fetch = sample.getFetchSize();
    }

    // make sure that limit 100000 doesn't create a fetch size beyond the configured fetch size.
    fetch = Math.min(fetch, configuredFetchSize);

    searchRequest.setFrom(0).setSize(fetch);
    return fetch;
  }

  protected void applyEdgeProjection(SearchRequestBuilder searchRequest, ElasticIntermediateScanPrel scan) {
    boolean edgeProject = PrelUtil.getPlannerSettings(scan.getCluster()).getOptions().getOption(ExecConstants.ELASTIC_RULES_EDGE_PROJECT);
    if(!edgeProject){
      return;
    }


    final String[] includesOrderedByOriginalTable;
    if (scan.getProjectedColumns().isEmpty()) {
      includesOrderedByOriginalTable = new String[0];
    } else {
      includesOrderedByOriginalTable =
          CalciteArrowHelper.wrap(scan.getBatchSchema().mask(scan.getProjectedColumns(), false))
            .toCalciteRecordType(scan.getCluster().getTypeFactory(), PrelUtil.getPlannerSettings(scan.getCluster()).isFullNestedSchemaSupport()).getFieldNames().toArray(new String[0]);
    }

    // canonicalize includes order so we don't get test variability.
    Arrays.sort(includesOrderedByOriginalTable);
    searchRequest.setFetchSource(includesOrderedByOriginalTable, null);
  }

  public void setup(List<ElasticsearchPrel> stack, FunctionLookupContext functionLookupContext) {

    validate(stack);

    try {
      final SearchRequestBuilder searchRequest = buildRequestBuilder();


      final Map<Class<?>, ElasticsearchPrel> map = validate(stack);

      final ElasticIntermediateScanPrel scan = (ElasticIntermediateScanPrel) map.get(ElasticIntermediateScanPrel.class);
      final ElasticTableXattr tableAttributes = scan.getExtendedAttributes();
      final ElasticsearchFilter filter = (ElasticsearchFilter) map.get(ElasticsearchFilter.class);
      final ElasticsearchSample sample = (ElasticsearchSample) map.get(ElasticsearchSample.class);
      final ElasticsearchLimit limit = (ElasticsearchLimit) map.get(ElasticsearchLimit.class);

      applyEdgeProjection(searchRequest, scan);
      applyFilter(searchRequest, scan, filter, tableAttributes);
      final int fetch = applyFetch(searchRequest,
        ElasticsearchConf.createElasticsearchConf(scan.getPluginId().getConnectionConf()),
        limit, filter, sample);

      ElasticsearchScanSpec scanSpec = new ElasticsearchScanSpec(
          tableAttributes.getResource(),
          searchRequest.toString(),
          fetch,
          filter != null || sample != null || limit != null);

      this.spec = scanSpec;
      this.scan = scan;
    } catch (ExpressionNotAnalyzableException e) {
      throw UserException.dataReadError(e).message("Elastic pushdown failed. Too late to recover query.").build(logger);
    }
  }

  protected SearchRequestBuilder buildRequestBuilder() {
    SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(new ElasticsearchClient() {
      @Override
      public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(
              Action<Request, Response, RequestBuilder> action, Request request) {
        return null;
      }

      @Override
      public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(
              Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> actionListener) {
      }

      @Override
      public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(
              Action<Request, Response, RequestBuilder> action) {
        return null;
      }

      @Override
      public ThreadPool threadPool() {
        return null;
      }
    }, SearchAction.INSTANCE);

    return searchRequestBuilder;
  }

}
