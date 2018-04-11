/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.SearchQueryUtils;
import com.google.common.collect.Maps;

/**
 * Indexing test
 */
public class TestLuceneIndexer {

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testSearchIndex() throws Exception {
    try (LuceneSearchIndex index = new LuceneSearchIndex("", "test", true)) {

      final Document doc1 = new Document();
      doc1.add(new StringField(IndexedStore.ID_FIELD_NAME, new BytesRef("1".getBytes()), Store.YES));
      doc1.add(new StringField("ds", "space1.ds1", Field.Store.NO));
      doc1.add(new StringField("job", "job1", Field.Store.YES));
      // since we want to sort on version add docvalues
      doc1.add(new StringField("version", "v1", Field.Store.NO));
      doc1.add(new SortedDocValuesField("version", new BytesRef("v1")));
      doc1.add(new StringField("foo", "bar1", Store.NO));
      doc1.add(new SortedDocValuesField("foo", new BytesRef("bar1")));

      final Document doc2 = new Document();
      doc2.add(new StringField(IndexedStore.ID_FIELD_NAME, new BytesRef("2".getBytes()), Store.YES));
      doc2.add(new StringField("ds", "space1.ds1", Field.Store.NO));
      doc2.add(new StringField("job", "job3", Field.Store.YES));
      doc2.add(new StringField("version", "v2", Field.Store.NO));
      doc2.add(new SortedDocValuesField("version", new BytesRef("v2")));
      doc2.add(new StringField("foo", "bar2", Store.NO));
      doc2.add(new SortedDocValuesField("foo", new BytesRef("bar2")));

      final Document doc3 = new Document();
      doc3.add(new StringField(IndexedStore.ID_FIELD_NAME, new BytesRef("3".getBytes()), Store.YES));
      doc3.add(new StringField("ds", "space2.ds2", Field.Store.NO));
      doc3.add(new StringField("job", "job2", Field.Store.YES));
      doc3.add(new StringField("version", "v1", Field.Store.NO));
      doc3.add(new SortedDocValuesField("version", new BytesRef("v1")));

      index.add(doc1);

      assertEquals(1, index.count(new TermQuery(new Term("ds", "space1.ds1"))));
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.add(new BooleanClause(new TermQuery(new Term("ds", "space1.ds1")), BooleanClause.Occur.MUST));
      builder.add(new BooleanClause(new TermQuery(new Term("version", "v1")), BooleanClause.Occur.MUST));
      assertEquals(1, index.count(builder.build()));

      assertEquals(0, index.count(new TermQuery(new Term("ds", "space2.ds2"))));
      assertEquals(0, index.count(new TermQuery(new Term("version", "v2"))));

      builder = new BooleanQuery.Builder();
      builder.add(new BooleanClause(new TermQuery(new Term("ds", "space1.ds1")), BooleanClause.Occur.MUST));
      builder.add(new BooleanClause(new TermQuery(new Term("version", "v2")), BooleanClause.Occur.MUST));
      assertEquals(0, index.count(builder.build()));
      assertEquals(1, index.count(new TermsQuery(new Term("ds", "space1.ds1"), new Term("version1", "v2"))));

      index.add(doc2);
      index.add(doc3);

      assertEquals(2, index.count(new TermQuery(new Term("ds", "space1.ds1"))));
      assertEquals(1, index.count(new TermQuery(new Term("ds", "space2.ds2"))));

      builder = new BooleanQuery.Builder();
      builder.add(new BooleanClause(new TermQuery(new Term("ds", "space2.ds2")), BooleanClause.Occur.MUST));
      builder.add(new BooleanClause(new TermQuery(new Term("version", "v1")), BooleanClause.Occur.MUST));
      assertEquals(1, index.count(builder.build()));

      builder = new BooleanQuery.Builder();
      builder.add(new BooleanClause(new TermQuery(new Term("ds", "space1.ds1")), BooleanClause.Occur.MUST));
      builder.add(new BooleanClause(new TermQuery(new Term("version", "v1")), BooleanClause.Occur.MUST));
      assertEquals(1, index.count(builder.build()));

      Sort sorter = new Sort();
      sorter.setSort(new SortField("version", SortField.Type.STRING));
      Collection<Document> documents = index.searchForDocuments(new TermQuery(new Term("ds", "space1.ds1")), 1000, sorter);
      assertEquals(2, documents.size());

      // exists queries
      assertEquals(2, index.count(LuceneQueryConverter.INSTANCE.toLuceneQuery(SearchQueryUtils.newExistsQuery("foo"))));
      assertEquals(1, index.count(LuceneQueryConverter.INSTANCE.toLuceneQuery(SearchQueryUtils.newDoesNotExistQuery("foo"))));
    }
  }

  private class Writer extends Thread implements Runnable {
    private final Map<String, String> data;
    private final LuceneSearchIndex index;

    public Writer(Map<String, String> data, LuceneSearchIndex index) {
      this.data = data;
      this.index = index;
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < 10000; ++i) {
          final Document document = new Document();
          final String key = "key" + i;
          final String val = "value" + i;
          document.add(new StringField(key, val, Field.Store.YES));
          document.add(new SortedDocValuesField(key, new BytesRef(val.getBytes())));
          index.add(document);
          data.put(key, val);
          sleep(1);
        }
      } catch (InterruptedException e) {
      }
    }
  }

  private class Reader extends Thread implements Runnable {
    private final Map<String, String> data;
    private final LuceneSearchIndex index;
    private Exception error;
    private final int id;

    public Reader(int id, Map<String, String> data, LuceneSearchIndex index) {
      this.id = id;
      this.data = data;
      this.index = index;
      this.error = null;
    }

    public Exception getError() {
      return error;
    }

    @Override
    public void run() {
      int i = 0;
      while (i < 10000) {
        try {
          if (data.size() <= i) {
            sleep(1);
            continue;
          }
          final String key = "key" + i;
          final String val = "value" + i;
          final List<Document> documents = index.searchForDocuments(new TermQuery(new Term(key, val)), 10, new Sort(new SortField(key, SortField.Type.STRING)));
          if (documents.size() != 1) {
            throw new RuntimeException("Invalid number of matching documents for " + key + ", found " + documents);
          }
          ++i;
        } catch (IOException ioe) {
          error = ioe;
          break;
        } catch (InterruptedException e) {
        } catch (AlreadyClosedException ace) {
          error = ace;
          break;
        }
      }
    }
  }

  @Test
  @Ignore
  public void testSearcherManager() throws Exception {
    try (LuceneSearchIndex index = new LuceneSearchIndex("", "multithreaded-search", true)) {

      final Map<String, String> data = Maps.newConcurrentMap();
      final Writer writer = new Writer(data, index);
      final Reader reader1 = new Reader(1, data, index);
      final Reader reader2 = new Reader(2, data, index);
      final Reader reader3 = new Reader(3, data, index);
      final Reader reader4 = new Reader(4, data, index);

      reader1.start();
      reader2.start();
      reader3.start();
      reader4.start();
      writer.start();

      reader1.join();
      reader2.join();
      reader3.join();
      reader4.join();
      writer.join();

      index.close();
      if (reader1.getError() != null) {
        throw reader1.getError();
      }
      if (reader2.getError() != null) {
        throw reader2.getError();
      }
      if (reader3.getError() != null) {
        throw reader3.getError();
      }
      if (reader4.getError() != null) {
        throw reader4.getError();
      }
    }
  }

  @Test
  public void testIndexClose() throws Exception {
    try (LuceneSearchIndex index = new LuceneSearchIndex(folder.getRoot(), "close", false)) {
      final Document doc1 = new Document();
      doc1.add(new StringField(IndexedStore.ID_FIELD_NAME, new BytesRef("1".getBytes()), Store.YES));
      doc1.add(new StringField("user", "u1", Field.Store.YES));
      index.add(doc1);
    }

    try (LuceneSearchIndex index = new LuceneSearchIndex(folder.getRoot(), "close", false)) {
      final Document doc2 = new Document();
      doc2.add(new StringField(IndexedStore.ID_FIELD_NAME, new BytesRef("2".getBytes()), Store.YES));
      doc2.add(new StringField("user", "u2", Field.Store.YES));
      index.add(doc2);
    }
    try (LuceneSearchIndex index = new LuceneSearchIndex(folder.getRoot(), "close", false)) {
      final Document doc3 = new Document();
      doc3.add(new StringField(IndexedStore.ID_FIELD_NAME, new BytesRef("3".getBytes()), Store.YES));
      doc3.add(new StringField("user", "u3", Field.Store.YES));
      index.add(doc3);
    }

    try (LuceneSearchIndex index = new LuceneSearchIndex(folder.getRoot(), "close", false)) {

      List<Document> documents = index.searchForDocuments(new TermQuery(new Term("user", "u1")), 100, new Sort());
      assertEquals(1, documents.size());
      assertEquals("u1", documents.get(0).get("user"));

      documents = index.searchForDocuments(new TermQuery(new Term("user", "u2")), 100, new Sort());
      assertEquals(1, documents.size());
      assertEquals("u2", documents.get(0).get("user"));

      documents = index.searchForDocuments(new TermQuery(new Term("user", "u3")), 100, new Sort());
      assertEquals(1, documents.size());
      assertEquals("u3", documents.get(0).get("user"));
    }
  }

}

