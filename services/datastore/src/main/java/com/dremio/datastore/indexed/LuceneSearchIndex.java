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
package com.dremio.datastore.indexed;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;

import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.WarningTimer;
import com.dremio.datastore.indexed.CommitWrapper.CommitCloser;
import com.dremio.telemetry.api.metrics.Metrics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

/**
 * Local search index based on lucene.
 */
public class LuceneSearchIndex implements AutoCloseable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LuceneSearchIndex.class);


  /**
   * Property name for configuring the amount of RAM (in MB) that may be used for buffering added documents
   *  and deletions before they are flushed during normal processing
   *
   *  Default is 32MB
   */
  public static final String RAM_BUFFER_SIZE_MB_PROPERTY = "dremio.lucene.ram_buffer_size_mb";

  /**
   * Property name for configuring the amount of RAM (in MB) that may be used for buffering added documents
   *  and deletions before they are flushed during reindexing
   *
   *  Default is half the amount of JVM memory
   */
  public static final String REINDEX_RAM_BUFFER_SIZE_MB_PROPERTY = "dremio.lucene.reindex.ram_buffer_size_mb";

  /**
   * Property name for configuring the ratio between the reindex ram buffer and the total JVM
   *
   *  Default is 2
   */
  public static final String REINDEX_RAM_BUFFER_SIZE_AUTO_RATIO_PROPERTY = "dremio.lucene.reindex.ram_buffer_size_auto_ratio";

  /**
   * Property name for the frequency (period in millis) between two commits
   *
   * Default is 1minute
   */
  public static final String COMMIT_FREQUENCY_MILLIS_PROPERTY = "dremio.lucene.commit_frequency";

  /**
   * Spinning disks override property
   *
   * Set to true if spinning disk, false if ssd, and do not set if auto-detect (only works on linux)
   */
  public static final String OVERRIDE_SPINS_PROPERTY = "dremio.lucene.override_spins";


  private static final String METRIC_PREFIX = "kvstore.lucene";


  //delay between end of a commit and next commit
  private static final long COMMIT_FREQUENCY = Integer.getInteger(COMMIT_FREQUENCY_MILLIS_PROPERTY, 60_000);

  // Amount of RAM that may be used for buffering added documents and deletions before they are flushed
  // during normal processing
  private static final int RAM_BUFFER_SIZE_MB = Integer.getInteger(RAM_BUFFER_SIZE_MB_PROPERTY, 32);

  // Ratio to apply to JVM total memory for buffering added documents and deletions during reindexing
  // if not set
  private static final int REINDEX_RAM_BUFFER_SIZE_AUTO_RATIO = Integer.getInteger(REINDEX_RAM_BUFFER_SIZE_AUTO_RATIO_PROPERTY, 2);

  // Amount of RAM that may be used for buffering added documents and deletions before they are flushed
  // during reindexing
  private static final int REINDEX_RAM_BUFFER_SIZE_MB = Integer.getInteger(REINDEX_RAM_BUFFER_SIZE_MB_PROPERTY,
      (int) (Runtime.getRuntime().totalMemory() / (1024 * 1024) / REINDEX_RAM_BUFFER_SIZE_AUTO_RATIO));


  /**
   * Starts a thread that will commit the writer every 60s (by default), if any exception is thrown during commit it will
   * be recorded and calling throwExceptionIfAny() will throw it back
   */
  private final class CommitterThread implements AutoCloseable {
    private volatile Throwable commitException;
    private final Thread commitThread;
    private volatile boolean closed;

    CommitterThread() {
      commitThread = new Thread(new Runnable() {
        @Override
        public void run() {
          commitLoop();
        }
      });

      commitThread.setName(format("LuceneSearchIndex:committer %s", name));
      commitThread.start();
    }

    void throwExceptionIfAny() {
      if (commitException != null) {
        Throwables.propagate(commitException);
      }
    }

    private void commitLoop() {
      while (!closed) {

        try {
          Thread.sleep(COMMIT_FREQUENCY);
        } catch (InterruptedException e) {
          // thread interrupted, exit immediately
          return;
        }

        // Do not commit while reindexing
        if (reindexing) {
          continue;
        }

        try (WarningTimer watch = new WarningTimer("LuceneSearchIndex commit", 5000)) {
          try {
            commit();
          } catch (Throwable e) {
            commitException = e;
            return; // stop commit thread, next call to any other method will throw an exception
          }
        }
      }
    }

    @Override
    public void close() {
      closed = true;
      commitThread.interrupt();

      while (true) {
        try {
          commitThread.join();
          break;
        } catch (InterruptedException e) {
          // we really don't want to be interrupted here
        }
      }
    }
  }

  private final CommitterThread committerThread;
  private final CommitWrapper commitWrapper;

  private final IndexWriter writer;
  private final BaseDirectory directory;
  private final SearcherManager searcherManager;
  private final String name;
  private final String liveRecordsMetricName;
  private final String deletedRecordsMetricsName;

  private volatile boolean reindexing = false;

  public LuceneSearchIndex(
      final File localStorageDir,
      final String name,
      final boolean inMemory,
      final CommitWrapper commitWrapper
  ) {
    this.name = name;
    this.commitWrapper = commitWrapper;

    final ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    String overrideSpins = System.getProperty(OVERRIDE_SPINS_PROPERTY);
    if (overrideSpins != null) {
      cms.setDefaultMaxMergesAndThreads(Boolean.parseBoolean(overrideSpins));
    }
    final IndexWriterConfig writerConfig = new IndexWriterConfig(new KeywordAnalyzer())
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
        .setRAMBufferSizeMB(RAM_BUFFER_SIZE_MB)
        .setMergeScheduler(cms);

    try {

      if(!inMemory){
        final File rootDir = new File(localStorageDir, name);
        if (rootDir.exists()) {
          if (!rootDir.isDirectory()) {
            throw new RuntimeException(String.format("Invalid path %s for local index, not a directory.",
              rootDir.getAbsolutePath()));
          }
        } else {
          if (!rootDir.mkdirs()) {
            throw new RuntimeException(String.format("Failed to create directory %s for local index.",
              rootDir.getAbsolutePath()));
          }
        }
        directory = MMapDirectory.open(new File(rootDir, "core").toPath());
      }else{
        directory = new RAMDirectory();
      }

      writer = new IndexWriter(directory, writerConfig);
      commit();
      searcherManager = new SearcherManager(writer, true, true, null);

      committerThread = new CommitterThread();
    } catch(IOException ex){
      throw Throwables.propagate(ex);
    }

    liveRecordsMetricName = Metrics.join(METRIC_PREFIX, name, "live-records");
    deletedRecordsMetricsName = Metrics.join(METRIC_PREFIX, name, "deleted-records");
    Metrics.newGauge(liveRecordsMetricName, this::getLiveRecords);
    Metrics.newGauge(deletedRecordsMetricsName, this::getDeletedRecords);
  }

  private void checkIfChanged() {
    try{
      if (!searcherManager.isSearcherCurrent()) {
        searcherManager.maybeRefreshBlocking();
      }
    }catch(IOException ex){
      throw Throwables.propagate(ex);
    }
  }

  private void commit() throws IOException {
    try (CommitCloser committer = commitWrapper.open(name)) {
      writer.commit();
      committer.succeeded();
    }
  }

  public void add(Document document) {
    committerThread.throwExceptionIfAny();
    Preconditions.checkNotNull(document.getField(IndexedStore.ID_FIELD_NAME));
    try{
      writer.addDocument(document);
    } catch(IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  public void addMany(Document... documents) {
    committerThread.throwExceptionIfAny();
    try{
      for(Document d : documents){
        writer.addDocument(d);
      }
    } catch(IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  public void update(Term term, Document document) {
    committerThread.throwExceptionIfAny();
    try {
      writer.updateDocument(term, document);
    } catch(IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  public int count(final Query query){
    committerThread.throwExceptionIfAny();
    checkIfChanged();
    try(Searcher searcher = acquireSearcher()) {
      return searcher.count(query);
    }
  }

  public List<Integer> count(final List<Query> queries) {
    committerThread.throwExceptionIfAny();
    checkIfChanged();
    List<Integer> integers = new ArrayList<>(queries.size());

    try(Searcher searcher = acquireSearcher()) {
      for(Query q : queries){
        integers.add(searcher.count(q));
      }
      return integers;
    }
  }

  private Searcher acquireSearcher() {
    try {
      IndexSearcher searcher = searcherManager.acquire();
      return new Searcher(searcher);
    } catch(IOException ex){
      throw Throwables.propagate(ex);
    }
  }

  private List<Doc> toDocs(ScoreDoc[] hits, Searcher searcher) throws IOException{
    List<Doc> documentList = new ArrayList<>();
    for (int i = 0; i < hits.length; ++i) {
      ScoreDoc scoreDoc = hits[i];
      Document doc = searcher.doc(scoreDoc.doc);
      IndexableField idField = doc.getField("_id");
      if(idField == null){
        // deleted between index hit and retrieval.
        continue;
      }
      final BytesRef ref = idField.binaryValue();
      final byte[] bytes = new byte[ref.length];
      System.arraycopy(ref.bytes, ref.offset, bytes, 0, ref.length);
      Doc outputDoc = new Doc(scoreDoc, bytes, 0);
      documentList.add(outputDoc);
    }
    return documentList;
  }

  public List<Doc> searchAfter(final Query query, int pageSize, Sort sort, Doc doc) throws IOException {
    committerThread.throwExceptionIfAny();
    checkIfChanged();
    try(Searcher searcher = acquireSearcher()) {
      TopDocs fieldDocs = searcher.searchAfter(doc.doc, query, pageSize, sort);
      if(fieldDocs == null) {
        return ImmutableList.of();
      }
      return toDocs(fieldDocs.scoreDocs, searcher);
    }
  }

  @Deprecated
  public List<Document> searchForDocuments(final Query query, int pageSize, Sort sort) throws IOException {
    committerThread.throwExceptionIfAny();
    checkIfChanged();
    try (Searcher searcher = acquireSearcher()){
      final List<Document> documents = new ArrayList<>();
      TopDocs fieldDocs = searcher.search(query, pageSize, sort);
      for(ScoreDoc d : fieldDocs.scoreDocs){
        documents.add(searcher.doc(d.doc));
      }
      return documents;
    }

  }

  public List<Doc> search(final Query query, int pageSize, Sort sort, int skip) throws IOException {
    committerThread.throwExceptionIfAny();
    checkIfChanged();
    Preconditions.checkArgument(skip > -1, "Skip must be zero or greater. Was %d.", skip);

    try (Searcher searcher = acquireSearcher()){

      if(skip == 0){
        // don't skip anything.
        final TopDocs fieldDocs = searcher.search(query, pageSize, sort);
        return toDocs(fieldDocs.scoreDocs, searcher);
      }

      // we'll skip docs without resolving external ids.
      final TopDocs skipDocs = searcher.search(query, skip, sort);
      if(skipDocs.scoreDocs.length == skip){
        // we found at least as many results as were skipped, we'll submit another query.
        TopDocs fieldDocs = searcher.searchAfter(skipDocs.scoreDocs[skip-1], query, pageSize, sort);
        if(fieldDocs == null) {
          return ImmutableList.of();
        }
        return toDocs(fieldDocs.scoreDocs, searcher);
      }else{
        // there are no more results once we did the skip.
        return Collections.emptyList();
      }

    }
  }

  @Override
  public void close() throws IOException {
    committerThread.close();
    Metrics.unregister(deletedRecordsMetricsName);
    Metrics.unregister(liveRecordsMetricName);
    // commit will fail if writer is closed
    if (writer.isOpen()) {
      // flush first
      writer.flush();
      commit();
      writer.close();
    }
    searcherManager.close();
  }

  public int getLiveRecords() {
    checkIfChanged();
    try(Searcher searcher = acquireSearcher()) {
      DirectoryReader reader = (DirectoryReader) searcher.searcher.getIndexReader();
      return reader.numDocs();
    }
  }

  public int getDeletedRecords() {
    checkIfChanged();
    try(Searcher searcher = acquireSearcher()) {
      DirectoryReader reader = (DirectoryReader) searcher.searcher.getIndexReader();
      return reader.numDeletedDocs();
    }
  }

  public void deleteDocuments(Term key) {
    committerThread.throwExceptionIfAny();
    try {
      writer.deleteDocuments(key);
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  public void delete() {
    committerThread.throwExceptionIfAny();
    try {
      writer.deleteAll();
      commit();
      // Forcing refresh of index so that open files are freed and deleted from disk
      checkIfChanged();
    } catch(Exception ex){
      throw Throwables.propagate(ex);
    }
  }

  public void forReindexing(Runnable r) {
    final LiveIndexWriterConfig config = writer.getConfig();
    final double maxRAMBufferSizeMB = config.getRAMBufferSizeMB();
    final boolean useCompoundFile = config.getUseCompoundFile();

    try {
      reindexing = true;
      // Adjust index writer settings to be better suited for reindexing
      config.setRAMBufferSizeMB(REINDEX_RAM_BUFFER_SIZE_MB);
      config.setUseCompoundFile(false);
      r.run();
      commit();
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    } finally {
      config.setRAMBufferSizeMB(maxRAMBufferSizeMB);
      config.setUseCompoundFile(useCompoundFile);
      reindexing = false;
    }
  }
  /**
   * Class that describes the relevant information to map index items to the KVStore.
   */
  public static final class Doc {
    private final ScoreDoc doc;
    private final byte[] key;
    private final long version;

    public Doc(ScoreDoc doc, byte[] key, long version) {
      super();
      this.doc = doc;
      this.key = key;
      this.version = version;
    }

    public byte[] getKey() {
      return key;
    }

    public long getVersion() {
      return version;
    }

  }

  /**
   * Facade on top of IndexSearcher that propagates IOExceptions as
   * RuntimeException and is AutoCloseable for managing opening/closing of
   * IndexSearchers.
   */
  private class Searcher implements AutoCloseable {

    private final IndexSearcher searcher;

    public Searcher(IndexSearcher searcher) {
      super();
      this.searcher = searcher;
    }

    public TopDocs searchAfter(final ScoreDoc after, Query query, int numHits, Sort order) {
      try {
        return searcher.searchAfter(after, query, numHits, order);
      } catch(IllegalArgumentException ex) {
        // we got to end of index.
        return null;
      } catch(IOException ex){
        throw Throwables.propagate(ex);
      }
    }

    public TopDocs search(Query query, int numHits, Sort order) {
      try {
        return searcher.search(query, numHits, order);
      } catch(IOException ex){
        throw Throwables.propagate(ex);
      }
    }

    public Document doc(int docId){
      try {
        return searcher.doc(docId);
      } catch(IOException ex){
        throw Throwables.propagate(ex);
      }
    }

    public int count(Query q) {
      try {
        return searcher.count(q);
      } catch(IOException ex){
        throw Throwables.propagate(ex);
      }
    }

    @Override
    public void close() {
      try {
        searcherManager.release(searcher);
      } catch(IOException ex) {
        throw Throwables.propagate(ex);
      }
    }
  }

  @VisibleForTesting
  public void deleteEverything() throws IOException{
    committerThread.throwExceptionIfAny();
    writer.deleteAll();
    commit();
  }
}
