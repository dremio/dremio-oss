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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.datastore.CoreKVStore;
import com.dremio.datastore.KVStoreTuple;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.StringSerializer;
import com.dremio.datastore.api.ImmutableDocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Sort;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Test cases for {@code CoreSearchIterable} */
public class TestCoreSearchIterable {

  private static final byte[] FOO2_BYTES = "foo2".getBytes(UTF_8);
  private static final byte[] FOO1_BYTES = "foo1".getBytes(UTF_8);

  /**
   * Test a possible scenario where index return documents which are not present in kvstore Make
   * sure the class guards against it by ignoring missing docs.
   */
  @Test
  public void testDeletedValue() {
    @SuppressWarnings("unchecked")
    final CoreKVStore<String, String> store = mock(CoreKVStore.class);
    final LuceneSearchIndex index = new LuceneSearchIndex(null, "test", true, CommitWrapper.NO_OP);

    final IndexKey queryKey = IndexKey.newBuilder("q", "query", String.class).build();
    {
      final Document doc = new Document();
      SimpleDocumentWriter dw = new SimpleDocumentWriter(doc);
      dw.write(CoreIndexedStoreImpl.ID_KEY, FOO1_BYTES);
      dw.write(queryKey, "bar");
      index.add(doc);
    }
    {
      final Document doc = new Document();
      SimpleDocumentWriter dw = new SimpleDocumentWriter(doc);
      dw.write(CoreIndexedStoreImpl.ID_KEY, FOO2_BYTES);
      dw.write(queryKey, "bar");
      index.add(doc);
    }

    when(store.newKey())
        .then(
            new Answer<KVStoreTuple<String>>() {
              @Override
              public KVStoreTuple<String> answer(InvocationOnMock invocation) throws Throwable {
                return new KVStoreTuple<>(StringSerializer.INSTANCE);
              }
            });
    when(store.newValue())
        .then(
            new Answer<KVStoreTuple<String>>() {
              @Override
              public KVStoreTuple<String> answer(InvocationOnMock invocation) throws Throwable {
                return new KVStoreTuple<>(StringSerializer.INSTANCE);
              }
            });

    final KVStoreTuple<String> foo1Key = new KVStoreTuple<>(StringSerializer.INSTANCE);
    foo1Key.setSerializedBytes(FOO1_BYTES);
    final KVStoreTuple<String> foo2Key = new KVStoreTuple<>(StringSerializer.INSTANCE);
    foo2Key.setSerializedBytes(FOO2_BYTES);

    final com.dremio.datastore.api.Document<KVStoreTuple<String>, KVStoreTuple<String>> foo1Doc =
        new ImmutableDocument.Builder<KVStoreTuple<String>, KVStoreTuple<String>>()
            .setKey(foo1Key)
            .setValue(foo1Key)
            .setTag("test-tag")
            .build();

    when(store.get(Arrays.asList(foo1Key, foo2Key))).thenReturn(Arrays.asList(foo1Doc, null));
    when(store.get(Arrays.asList(foo2Key, foo1Key))).thenReturn(Arrays.asList(null, foo1Doc));

    final CoreSearchIterable<String, String> iterable =
        new CoreSearchIterable<>(
            store,
            index,
            LuceneQueryConverter.INSTANCE.toLuceneQuery(
                SearchQueryUtils.newTermQuery(queryKey, "bar")),
            new Sort(),
            10,
            0,
            10);

    final List<com.dremio.datastore.api.Document<KVStoreTuple<String>, KVStoreTuple<String>>>
        results = new ArrayList<>();
    iterable.forEach(results::add);

    assertThat(results).hasSize(1).containsExactly(foo1Doc);

    assertEquals(0, index.getNumCachedSearchers());
  }

  /**
   * Tests a case where CoreSearchIterable thinks the search is not done (due to deleted values),
   * but LuceneSearchIndex thinks it's done.
   */
  @Test
  public void testIteratorWithDeletedEntries() {
    @SuppressWarnings("unchecked")
    final CoreKVStore<String, String> store = mock(CoreKVStore.class);
    final LuceneSearchIndex index = new LuceneSearchIndex(null, "test", true, CommitWrapper.NO_OP);
    final int totalEntries = 12;
    final int nullValueEntries = 2;

    final IndexKey queryKey = IndexKey.newBuilder("q", "query", String.class).build();
    {
      for (int i = 0; i < totalEntries; i++) {
        final Document doc = new Document();
        SimpleDocumentWriter dw = new SimpleDocumentWriter(doc);
        dw.write(CoreIndexedStoreImpl.ID_KEY, String.valueOf(i).getBytes());
        dw.write(queryKey, "bar");
        index.add(doc);
      }
    }

    when(store.newKey())
        .then(
            new Answer<KVStoreTuple<String>>() {
              @Override
              public KVStoreTuple<String> answer(InvocationOnMock invocation) throws Throwable {
                return new KVStoreTuple<>(StringSerializer.INSTANCE);
              }
            });
    when(store.newValue())
        .then(
            new Answer<KVStoreTuple<String>>() {
              @Override
              public KVStoreTuple<String> answer(InvocationOnMock invocation) throws Throwable {
                return new KVStoreTuple<>(StringSerializer.INSTANCE);
              }
            });

    when(store.get(anyList()))
        .thenAnswer(
            invocation -> {
              List<KVStoreTuple<String>> inputList =
                  (List<KVStoreTuple<String>>) invocation.getArguments()[0];
              List<com.dremio.datastore.api.Document<KVStoreTuple<String>, KVStoreTuple<String>>>
                  retDocs = new ArrayList<>();

              // return null for the first two entries.
              for (KVStoreTuple<String> entry : inputList) {
                String key = entry.getObject();
                if (Integer.parseInt(key) < nullValueEntries) {
                  retDocs.add(null);
                  continue;
                }
                retDocs.add(
                    new ImmutableDocument.Builder<KVStoreTuple<String>, KVStoreTuple<String>>()
                        .setKey(entry)
                        .setValue(entry)
                        .setTag("test-tag")
                        .build());
              }
              return retDocs;
            });

    final CoreSearchIterable<String, String> iterable =
        new CoreSearchIterable<>(
            store,
            index,
            LuceneQueryConverter.INSTANCE.toLuceneQuery(
                SearchQueryUtils.newTermQuery(queryKey, "bar")),
            new Sort(),
            totalEntries - nullValueEntries,
            0,
            totalEntries - nullValueEntries);

    final List<com.dremio.datastore.api.Document<KVStoreTuple<String>, KVStoreTuple<String>>>
        results = new ArrayList<>();
    iterable.forEach(results::add);

    assertEquals(totalEntries - nullValueEntries, results.size());
    assertEquals(0, index.getNumCachedSearchers());
  }
}
