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
package com.dremio.plugins.elastic.execution;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.core.Response;

import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.impl.SingleStructReaderImpl;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.calcite.util.Pair;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticSplitXattr;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticTableXattr;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.easy.json.JsonProcessor;
import com.dremio.exec.store.easy.json.reader.BaseJsonProcessor;
import com.dremio.exec.vector.complex.fn.JsonWriter;
import com.dremio.plugins.elastic.ElasticActions.DeleteScroll;
import com.dremio.plugins.elastic.ElasticActions.Search;
import com.dremio.plugins.elastic.ElasticActions.SearchBytes;
import com.dremio.plugins.elastic.ElasticActions.SearchScroll;
import com.dremio.plugins.elastic.ElasticConnectionPool.ElasticConnection;
import com.dremio.plugins.elastic.ElasticVersionBehaviorProvider;
import com.dremio.plugins.elastic.ElasticsearchConf;
import com.dremio.plugins.elastic.ElasticsearchConstants;
import com.dremio.plugins.elastic.ElasticsearchStoragePlugin;
import com.dremio.plugins.elastic.planning.ElasticsearchScanSpec;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Record reader for Elasticsearch.
 */
public class ElasticsearchRecordReader extends AbstractRecordReader {

  private static final boolean PRINT_OUTPUT = false;
  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchRecordReader.class);
  public static final String MATCH_ALL_QUERY = QueryBuilders.matchAllQuery().buildAsBytes().utf8ToString();
  public static final String MATCH_ALL_REQUEST = String.format("{\"query\": %s }", MATCH_ALL_QUERY);
  private static final int STREAM_COUNT_BREAK_MULTIPLIER = 3;
  private static final String TIMED_OUT = "\"timed_out\": true";

  enum State {INIT, READ, DEPLETED, CLOSED};


  private final String query;

  private final ElasticConnection connection;
  private final OperatorStats stats;
  private final String resource;
  private final ElasticsearchScanSpec spec;
  private final ElasticsearchConf config;
  private final ElasticSplitXattr splitAttributes;
  private final boolean usingElasticProjection;
  private final FieldReadDefinition readDefinition;
  private final ElasticTableXattr tableAttributes;
  private final List<String> tableSchemaPath;

  private final boolean metaUIDSelected;
  private final boolean metaIDSelected;
  private final boolean metaIndexSelected;
  private final boolean metaTypeSelected;
  private final ElasticsearchStoragePlugin plugin;

  private long totalSize;
  private long totalCount;
  private String scrollId;
  private VectorContainerWriter complexWriter;
  private BaseJsonProcessor jsonReader;
  private State state = State.INIT;
  private final ElasticVersionBehaviorProvider elasticVersionBehaviorProvider;

  public ElasticsearchRecordReader(
    ElasticsearchStoragePlugin plugin,
    List<String> tableSchemaPath,
    ElasticTableXattr tableAttributes,
    OperatorContext context,
    ElasticsearchScanSpec spec,
    boolean useElasticProjection,
    SplitAndPartitionInfo split,
    ElasticConnection connection,
    List<SchemaPath> columns,
    FieldReadDefinition readDefinition,
    ElasticsearchConf config) throws InvalidProtocolBufferException {
    super(context, columns);
    this.plugin = plugin;
    this.tableAttributes = tableAttributes;
    this.tableSchemaPath = tableSchemaPath;
    this.spec = spec;
    this.stats = context == null ? null : context.getStats();
    this.readDefinition = readDefinition;
    this.connection = connection;
    String query = spec.getQuery();
    this.query = query != null && query.length() > 0 ? query : MATCH_ALL_REQUEST;
    this.usingElasticProjection = useElasticProjection;
    this.config = config;
    this.splitAttributes = split == null ? null : ElasticSplitXattr.parseFrom(split.getDatasetSplitInfo().getExtendedProperty());
    this.resource = split == null ? spec.getResource() : splitAttributes.getResource();
    this.elasticVersionBehaviorProvider = new ElasticVersionBehaviorProvider(this.connection.getESVersionInCluster());
    if(elasticVersionBehaviorProvider.isEnable7vFeatures()) {
      this.metaUIDSelected = false;
      this.metaIDSelected = getColumns().contains(SchemaPath.getSimplePath(ElasticsearchConstants.ID)) || isStarQuery();
    } else {
      this.metaUIDSelected = getColumns().contains(SchemaPath.getSimplePath(ElasticsearchConstants.UID)) || isStarQuery();
      this.metaIDSelected = config.isShowIdColumn() && (getColumns().contains(SchemaPath.getSimplePath(ElasticsearchConstants.ID)) || isStarQuery());
    }
    this.metaTypeSelected = getColumns().contains(SchemaPath.getSimplePath(ElasticsearchConstants.TYPE)) || isStarQuery();
    this.metaIndexSelected = getColumns().contains(SchemaPath.getSimplePath(ElasticsearchConstants.INDEX)) || isStarQuery();

    if (spec.getFetch() > 0) {
      this.numRowsPerBatch = Math.min(this.numRowsPerBatch, spec.getFetch());
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    complexWriter = new VectorContainerWriter(output);
    if (getColumns().isEmpty()) {
      jsonReader = elasticVersionBehaviorProvider.createCountingElasticSearchReader(context.getManagedBuffer(),
        ImmutableList.copyOf(getColumns()),
        resource,
        readDefinition,
        usingElasticProjection,
        metaUIDSelected,
        metaIDSelected,
        metaTypeSelected,
        metaIndexSelected);
    } else {
      jsonReader = elasticVersionBehaviorProvider.createElasticSearchReader(context.getManagedBuffer(),
        ImmutableList.copyOf(getColumns()),
        resource,
        readDefinition,
        usingElasticProjection,
        metaUIDSelected,
        metaIDSelected,
        metaTypeSelected,
        metaIndexSelected);
    }
  }

  @Override
  protected boolean supportsSkipAllQuery() {
    return true;
  }

  private void getFirstPage() {
    assert state == State.INIT;
    int searchSize = config.getScrollSize();
    int fetch = spec.getFetch();
    if (fetch >= 0 && fetch < searchSize) {
      searchSize = fetch;
    }

    final Search<byte[]> search;
    final String newQuery;
    newQuery = elasticVersionBehaviorProvider.processElasticSearchQuery(query);
    search = new SearchBytes()
      .setQuery(newQuery)
      .setResource(resource)
      .setParameter("scroll", config.getScrollTimeoutFormatted())
      .setParameter("size", Integer.toString(searchSize));

    if (splitAttributes != null) {
      search.setParameter("preference", "_shards:" + splitAttributes.getShard());
    }

    if (this.usingElasticProjection) {
      search.setParameter(ElasticsearchConstants.SOURCE, "false");
    }

    final byte[] bytes;
    try {
      bytes = elasticVersionBehaviorProvider.getSearchBytes(connection, search);
    } catch (UserException e) {
      if (e.getErrorType() == ErrorType.INVALID_DATASET_METADATA) {
        logger.trace("failed with invalid metadata, ", e);
        throw UserException.invalidMetadataError()
          .setAdditionalExceptionContext(
            new InvalidMetadataErrorContext(Collections.singletonList(tableSchemaPath)))
          .build(logger);
      }

      throw e;
    }

    try {
      jsonReader.setSource(bytes);

      Pair<String, Long> scrollIdAndTotalSize;
      scrollIdAndTotalSize = jsonReader.getScrollAndTotalSizeThenSeekToHits();

      scrollId = scrollIdAndTotalSize.getKey();
      totalSize = scrollIdAndTotalSize.getValue();
    } catch (IOException e) {
      String bestEffortMessage = bestEffortMessageForUnknownException(e.getCause());
      if (bestEffortMessage != null) {
        throw UserException.ioExceptionError().message(bestEffortMessage).buildSilently();
      }

      throw UserException.dataReadError(e)
        .message("Failure when initiating Elastic query.")
        .addContext("Resource", resource)
        .addContext("Shard", splitAttributes == null ? "all" : splitAttributes.getShard())
        .addContext("Query", query)
        .build(logger);
    }

    state = State.READ;
  }

  private byte[] getNextPage() throws IOException {
    try {
      if (stats != null) {
        stats.startWait();
      }
      SearchScroll searchScroll = new SearchScroll()
        .setScrollId(scrollId)
        .setScrollTimeout(config.getScrollTimeoutFormatted());
      return connection.execute(searchScroll);
    } finally {
      if (stats != null) {
        stats.stopWait();
      }
    }
  }


  @Override
  public int next() {
    if (state == State.DEPLETED || state == State.CLOSED) {
      return 0;
    }

    if (state == State.INIT) {
      getFirstPage();
    }

    assert state == State.READ;

    complexWriter.allocate();
    complexWriter.reset();

    int pageCount = 0;
    int count = 0;
    try {
      while (count < numRowsPerBatch) {
        if (state == State.DEPLETED) {
          break;
        }

        complexWriter.setPosition(count);

        JsonProcessor.ReadState readState = jsonReader.write(complexWriter);
        if (readState == JsonProcessor.ReadState.WRITE_SUCCEED) {
          count++;
          totalCount++;
          continue;
        }

        // if we receive the records we were told we'd receive, we will should stop reading.
        if (totalCount == totalSize) {
          state = State.DEPLETED;
          break;
        }

        final byte[] bytes = getNextPage();
        pageCount++;

        // if we're calling an ES server many times and isn't getting us the number of messages we expect, we should terminate the query to avoid a DOS attack
        boolean badStreamBreak = pageCount > STREAM_COUNT_BREAK_MULTIPLIER * numRowsPerBatch / (1.0 * spec.getFetch()) && pageCount > 5;

        if (!badStreamBreak) {
          jsonReader.setSource(bytes);
          scrollId = jsonReader.getScrollAndTotalSizeThenSeekToHits().getKey();
          continue;
        }

        // we didn't get the records we expected within a reasonable amount of time.
        final String latest = new String(bytes, Charsets.UTF_8);
        final boolean timedOut = latest.contains(TIMED_OUT);

        if (!timedOut && config.isWarnOnRowCountMismatch()) {
          logger.warn("Dremio didn't receive as many results from Elasticsearch as expected. Expected {}. Received: {}", totalSize, totalCount);
          state = State.DEPLETED;
          break;
        }

        final UserException.Builder builder = UserException.dataReadError();
        if (timedOut) {
          builder.message("Elastic failed with scroll timed out.");
        } else {
          builder.message("Elastic query terminated as Dremio didn't receive as many results as expected.");
          builder.addContext("Expected record count", totalSize);
          builder.addContext("Records received", totalCount);
        }

        builder.addContext("Resource", this.resource);
        builder.addContext("Query", this.query);
        builder.addContext("Final Response", latest);
        throw builder.build(logger);

      }

      if (count > 0) {
        print(complexWriter.getStructVector(), count);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    jsonReader.ensureAtLeastOneField(complexWriter);
    complexWriter.setValueCount(count);
    try {
      print(complexWriter.getStructVector(), count);
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
    return count;
  }

  private void print(NonNullableStructVector structVector, int count) throws JsonGenerationException, IOException {
    if (PRINT_OUTPUT) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      JsonWriter jsonWriter = new JsonWriter(baos, true, false);
      FieldReader reader = new SingleStructReaderImpl(structVector);
      for (int index = 0; index < count; index++) {
        reader.setPosition(index);
        jsonWriter.write(reader);
      }

      System.out.println(baos.toString());
    }
  }

  @Override
  public synchronized void close() throws Exception {
    if (state == State.CLOSED) {
      return;
    }

    if (state == State.INIT) {
      state = State.CLOSED;
      return; // scroll id is not yet set
    }

    // TODO(DX-10051): fix rare race condition: above block assumes scrollId is not set, but the fragment thread
    // could be in #getFirstPage, right before setting scrollId. In this case, the scroll will never be deleted.

    try {
      final DeleteScroll delete = new DeleteScroll(scrollId);
      final CountDownLatch countDownLatch = new CountDownLatch(1);
      delete.delete(connection.getTarget(), new InvocationCallback<Response>() {
        @Override
        public void completed(Response response) {
          countDownLatch.countDown();
        }

        @Override
        public void failed(Throwable throwable) {
          logger.warn("Exception while deleting scroll", throwable);
          countDownLatch.countDown();
        }
      });
      try {
        countDownLatch.await(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger.warn("Interrupted while deleting scroll", e);
      }
    } catch (Exception e) {
      logger.warn("Failure while closing Elasticsearch scroll: " + scrollId);
    } finally {
      state = State.CLOSED;
    }
  }
}
