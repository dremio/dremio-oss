/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic.execution;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.core.Response;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.SingleMapReaderImpl;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.calcite.util.Pair;
import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticSplitXattr;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticTableXattr;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.DefaultSchemaMutator;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.easy.json.JsonProcessor;
import com.dremio.exec.vector.complex.fn.JsonWriter;
import com.dremio.exec.vector.complex.fn.WorkingBuffer;
import com.dremio.plugins.elastic.ElasticActions.DeleteScroll;
import com.dremio.plugins.elastic.ElasticActions.Search;
import com.dremio.plugins.elastic.ElasticActions.SearchScroll;
import com.dremio.plugins.elastic.ElasticConnectionPool.ElasticConnection;
import com.dremio.plugins.elastic.ElasticsearchConstants;
import com.dremio.plugins.elastic.ElasticsearchStoragePlugin2;
import com.dremio.plugins.elastic.ElasticsearchStoragePluginConfig;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet.ElasticMapping;
import com.dremio.plugins.elastic.mapping.SchemaMerger;
import com.dremio.plugins.elastic.mapping.SchemaMerger.MergeResult;
import com.dremio.plugins.elastic.planning.ElasticsearchScanSpec;
import com.dremio.sabot.driver.SchemaChangeMutator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

import io.protostuff.ByteString;

/**
 * Record reader for Elasticsearch.
 */
public class ElasticsearchRecordReader extends AbstractRecordReader {

  private static final boolean PRINT_OUTPUT = false;
  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchRecordReader.class);
  public static final String MATCH_ALL_QUERY = QueryBuilders.matchAllQuery().buildAsBytes().toUtf8();
  public static final String MATCH_ALL_REQUEST = String.format("{\"query\": %s }", MATCH_ALL_QUERY);
  private static final int STREAM_COUNT_BREAK_MULTIPLIER = 3;
  private static final String TERMINATED_EARLY = "\"terminated_early\": true";
  private static final String TIMED_OUT = "\"timed_out\": true";

  enum State {INIT, READ, DEPLETED, CLOSED};


  private final String query;
  private final DatasetSplit split;

  private final ElasticConnection connection;
  private final OperatorStats stats;
  private final String resource;
  private final ElasticsearchScanSpec spec;
  private final ElasticsearchStoragePluginConfig config;
  private final ElasticSplitXattr splitAttributes;
  private final boolean usingElasticProjection;
  private final BatchSchema schema;
  private final WorkingBuffer buffer;
  private final FieldReadDefinition readDefinition;
  private final ElasticTableXattr tableAttributes;
  private final List<String> tableSchemaPath;

  private final boolean metaUIDSelected;
  private final boolean metaIDSelected;
  private final boolean metaIndexSelected;
  private final boolean metaTypeSelected;
  private final ElasticsearchStoragePlugin2 plugin;

  private long totalSize;
  private long totalCount;
  private String scrollId;
  private VectorContainerWriter complexWriter;
  private ElasticsearchJsonReader jsonReader;
  private State state = State.INIT;

  public ElasticsearchRecordReader(
      ElasticsearchStoragePlugin2 plugin,
      List<String> tableSchemaPath,
      ElasticTableXattr tableAttributes,
      OperatorContext context,
      ElasticsearchScanSpec spec,
      boolean useElasticProjection,
      DatasetSplit split,
      ElasticConnection connection,
      List<SchemaPath> columns,
      FieldReadDefinition readDefinition,
      ElasticsearchStoragePluginConfig config,
      WorkingBuffer buffer,
      BatchSchema schema) throws InvalidProtocolBufferException {
    super(context, columns);
    this.plugin = plugin;
    this.tableAttributes = tableAttributes;
    this.tableSchemaPath = tableSchemaPath;
    this.spec = spec;
    this.stats = context == null ? null : context.getStats();
    this.split = split;
    this.readDefinition = readDefinition;
    this.connection = connection;
    String query = spec.getQuery();
    this.query = query != null && query.length() > 0 ? query : MATCH_ALL_REQUEST;
    this.usingElasticProjection = useElasticProjection;
    this.config = config;
    this.schema = schema;
    this.buffer = buffer;
    this.splitAttributes = split == null ? null : ElasticSplitXattr.parseFrom(split.getExtendedProperty().toByteArray());
    this.resource = split == null ? spec.getResource() : splitAttributes.getResource();
    this.metaUIDSelected = getColumns().contains(SchemaPath.getSimplePath(ElasticsearchConstants.UID)) || isStarQuery();
    this.metaIDSelected = config.isIdColumnEnabled() && (getColumns().contains(SchemaPath.getSimplePath(ElasticsearchConstants.ID)) || isStarQuery());
    this.metaTypeSelected = getColumns().contains(SchemaPath.getSimplePath(ElasticsearchConstants.TYPE)) || isStarQuery();
    this.metaIndexSelected = getColumns().contains(SchemaPath.getSimplePath(ElasticsearchConstants.INDEX)) || isStarQuery();
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    complexWriter = new VectorContainerWriter(output);
    jsonReader = new ElasticsearchJsonReader(
        context.getManagedBuffer(),
        ImmutableList.copyOf(getColumns()),
        resource,
        readDefinition,
        usingElasticProjection,
        metaUIDSelected,
        metaIDSelected,
        metaTypeSelected,
        metaIndexSelected
        );
  }


  @Override
  public SchemaChangeMutator getSchemaChangeMutator() {
    return new SchemaUpdateMerger();
  }

  private final class SchemaUpdateMerger implements SchemaChangeMutator {

    @Override
    public DatasetConfig updateForSchemaChange(DatasetConfig oldConfig, BatchSchema expectedSchema, BatchSchema newlyObservedSchema) {
      Preconditions.checkNotNull(oldConfig);
      Preconditions.checkNotNull(newlyObservedSchema);

      // its possible that the mapping has changed. If so, we need to re-sample the data. fail the query and retry.
      // If not, make sure we update the schema using the elastic schema merger rather than a general merge behavior.
      int mappingHash = tableAttributes.getMappingHash();
      NamespaceKey key = new NamespaceKey(tableSchemaPath);
      ElasticMapping mapping = plugin.getMapping(key);
      if(mapping == null){
        throw UserException.dataReadError().message("Unable to find schema information for %s after observing schema change.", key).build(logger);
      }

      int latestMappingHash = mapping.hashCode();
      if(mappingHash != latestMappingHash){
        throw UserException.dataReadError().message("Mapping updated since last metadata refresh. Please run \"ALTER TABLE %s REFRESH METADATA\" before rerunning query.", new NamespaceKey(tableSchemaPath).toString()).build(logger);
      }

      SchemaMerger merger = new SchemaMerger();
      // Since the newlyObserved schema could be partial due to projections, we need to merge it with the original.
      DatasetConfig newConfig = DefaultSchemaMutator.clone(oldConfig);

      BatchSchema preMergedSchema = expectedSchema.merge(newlyObservedSchema);
      MergeResult result = merger.merge(mapping, preMergedSchema);


      try {
        // update the annotations.
        ElasticTableXattr xattr = ElasticTableXattr.parseFrom(newConfig.getReadDefinition().getExtendedProperty().toByteArray());
        newConfig.getReadDefinition().setExtendedProperty(ByteString.copyFrom(xattr.toBuilder().clearAnnotation().addAllAnnotation(result.getAnnotations()).build().toByteArray()));
        newConfig.setRecordSchema(ByteString.copyFrom(result.getSchema().serialize()));
        return newConfig;
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private void getFirstPage() {
    assert state == State.INIT;
    int searchSize = config.getBatchSize();
    int fetch = spec.getFetch();
    if (fetch >= 0 &&  fetch < searchSize) {
      searchSize = fetch;
    }

    final Search search = new Search()
        .setQuery(query)
        .setResource(resource)
        .setParameter("scroll", config.getScrollTimeoutFormatted())
        .setParameter("size", Integer.toString(searchSize));

    if (splitAttributes != null) {
      search.setParameter("preference", "_shards:" + splitAttributes.getShard());
    }

    if (this.usingElasticProjection) {
      search.setParameter(ElasticsearchConstants.SOURCE, "false");
    }

    try {
      jsonReader.setSource(connection.execute(search));
      Pair<String, Long> scrollIdAndTotalSize = jsonReader.getScrollAndTotalSizeThenSeekToHits();
      scrollId = scrollIdAndTotalSize.getKey();
      totalSize = scrollIdAndTotalSize.getValue();
    } catch (IOException e) {
      throw UserException.dataReadError(e)
        .message("Failure when initating Elastic query.")
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

    if(state == State.INIT){
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
        boolean badStreamBreak = pageCount > STREAM_COUNT_BREAK_MULTIPLIER * numRowsPerBatch/(1.0*spec.getFetch()) && pageCount > 5;

        if(!badStreamBreak){
          jsonReader.setSource(bytes);
          scrollId = jsonReader.getScrollAndTotalSizeThenSeekToHits().getKey();
          continue;
        }

        // we didn't get the records we expected within a reasonable amount of time.
        final String latest = new String(bytes, Charsets.UTF_8);
        final boolean timedOut = latest.contains(TIMED_OUT);
        final boolean terminatedEarly = latest.contains(TERMINATED_EARLY);
        final UserException.Builder builder = UserException.dataReadError();
        if (timedOut) {
          builder.message("Elastic failed with scroll timed out.");

        } else if(terminatedEarly) {
          builder.message("Elastic failed with early termination.");
        } else {
          builder.message("Elastic query terminated as Dremio didn't receive as many results as expected.");
        }

        builder.addContext("Resource", this.resource);
        builder.addContext("Query", this.query);
        builder.addContext("Final Response", latest);
        throw builder.build(logger);

      }

      if(count > 0){
        print(complexWriter.getMapVector(), count);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    jsonReader.ensureAtLeastOneField(complexWriter);
    complexWriter.setValueCount(count);
    try{
      print(complexWriter.getMapVector(), count);
    }catch(Exception ex){
      throw Throwables.propagate(ex);
    }
    return count;
  }

  private void print(MapVector mapVector, int count) throws JsonGenerationException, IOException{
    if(PRINT_OUTPUT){
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      JsonWriter jsonWriter = new JsonWriter(baos, true, false);
      FieldReader reader = new SingleMapReaderImpl(mapVector);
      for(int index = 0; index < count; index++){
        reader.setPosition(index);
        jsonWriter.write(reader);
      }

      System.out.println(baos.toString());
    }
  }

  @Override
  public synchronized void close() throws Exception {
    if(state == State.CLOSED){
      return;
    }

    DeleteScroll delete = new DeleteScroll(scrollId);
    try {
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
