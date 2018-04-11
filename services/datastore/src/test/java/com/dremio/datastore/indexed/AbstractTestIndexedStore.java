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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.KVStoreProvider.DocumentWriter;
import com.dremio.datastore.KVUtil;
import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.Serializer;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.StringSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Test the indexed store implementation
 */
public abstract class AbstractTestIndexedStore {

  private KVStoreProvider provider;
  private IndexedStore<String, Doughnut> store;

  private final Doughnut d1 = new Doughnut("original", "glazed", 1.29);
  private final Doughnut d2 = new Doughnut("custard", "bavarian creme with chocolate icing", 1.39);
  private final Doughnut d3 = new Doughnut("sourdough", "cake with glaze", 1.10);

  private List<Integer> getCounts(String... filters){
    List<SearchQuery> queries = Lists.transform(Arrays.asList(filters), new Function<String, SearchQuery>(){

      @Override
      public SearchQuery apply(String input) {
        return SearchFilterToQueryConverter.toQuery(input, MAPPING);
      }});

    return store.getCounts(queries.toArray(new SearchQuery[queries.size()]));
  }

  abstract KVStoreProvider createKKStoreProvider() throws Exception;

  @Before
  public void before() throws Exception {
    provider = createKKStoreProvider();
    store = provider.getStore(Creator.class);
  }

  /**
   * Test function
   */
  public static class Creator implements StoreCreationFunction<IndexedStore<String, Doughnut>> {

    @Override
    public IndexedStore<String, Doughnut> build(StoreBuildingFactory factory) {
      return factory.<String, Doughnut>newStore()
          .name("test-dough")
          .keySerializer(StringSerializer.class)
          .valueSerializer(DoughnutSerializer.class)
          .buildIndexed(TestDocumentConverter.class);
    }

  }

  @After
  public void after() throws Exception {
    provider.close();
  }

  @Test
  public void put(){
    store.put("a", d1);
    checkFindByName(d1);
    checkFindByPrice(d1);
  }

  @Test
  public void checkAndPut() throws Exception {
    store.put("a", d1);
    checkFindByName(d1);
    assertEquals(true, store.checkAndPut("a", d1, d2));
    checkFindByName(d2);
    assertNoResultByName(d1.name);
  }

  @Test
  public void failedCheckAndPut(){
    store.put("a", d1);
    checkFindByName(d1);
    assertEquals(false, store.checkAndPut("a", d2, d3));

    // shouldn't have been replaced.
    checkFindByName(d1);
  }

  @Test
  public void counts(){
    store.put("a", d1);
    store.put("b", d2);
    store.put("c", d3);

    assertEquals(ImmutableList.of(1,2,0),
        getCounts("n==original",
            "p=gt=1.10;p=lt=1.40",
            "p=lt=1.11;n=lt=custard"));
  }

  @Test
  public void delete() {
    store.put("a", d1);
    checkFindByName(d1);
    store.delete("a");
    assertEquals(0, Iterables.size(
        store.find(new FindByCondition().setCondition("n==" + d1.name, MAPPING))
        ));

  }

  @Test
  public void failedDelete() {
    store.put("a", d1);
    checkFindByName(d1);
    assertEquals(false, store.checkAndDelete("a", d2));

    // shouldn't be deleted.
    checkFindByName(d1);
  }

  private void checkFindByName(Doughnut d){
    Iterable<Entry<String, Doughnut>> iter = store.find(new FindByCondition().setCondition("n==" + d.name, MAPPING));
    List<Doughnut> doughnuts = Lists.newArrayList(KVUtil.values(iter));
    Assert.assertEquals(1, doughnuts.size());
    Assert.assertEquals(d, doughnuts.get(0));
  }

  private void assertNoResultByName(String name) {
    assertNoResult("n==" + name);
  }

  private void assertNoResult(String filterStr){
    assertEquals(0, Iterables.size(
        store.find(new FindByCondition().setCondition(filterStr, MAPPING))
        ));
  }

  private void checkFindByPrice(Doughnut d){
    Iterable<Entry<String, Doughnut>> iter = store.find(new FindByCondition().setCondition("p==" + d.price, MAPPING));
    List<Doughnut> doughnuts = Lists.newArrayList(KVUtil.values(iter));
    Assert.assertEquals(1, doughnuts.size());
    Assert.assertEquals(d, doughnuts.get(0));
  }

  private static final IndexKey NAME = new IndexKey("n", "name", String.class, SearchFieldSorting.FieldType.STRING, false, true);
  private static final IndexKey FLAVOR = new IndexKey("f", "flavor", String.class, SearchFieldSorting.FieldType.STRING, false, true);
  private static final IndexKey PRICE = new IndexKey("p", "price", Double.class, SearchFieldSorting.FieldType.DOUBLE, false, true);
  static final FilterIndexMapping MAPPING = new FilterIndexMapping(NAME, FLAVOR, PRICE);

  private static final class TestDocumentConverter implements KVStoreProvider.DocumentConverter<String, Doughnut> {
    @Override
    public void convert(DocumentWriter writer, String key, Doughnut record) {
      writer.write(NAME, record.name);
      writer.write(FLAVOR, record.flavor);
      writer.write(PRICE, record.price);
    }
  }

  private static class Doughnut {
    private String name;
    private String flavor;
    private double price;

    // for kryo
    public Doughnut(){
    }

    public Doughnut(String name, String flavor, double price) {
      super();
      this.name = name;
      this.flavor = flavor;
      this.price = price;
    }

    @Override
    public String toString() {
      return "Doughnut [name=" + name + ", flavor=" + flavor + ", price=" + price + "]";
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, flavor, price);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Doughnut other = (Doughnut) obj;
      if (flavor == null) {
        if (other.flavor != null) {
          return false;
        }
      } else if (!flavor.equals(other.flavor)) {
        return false;
      }
      if (name == null) {
        if (other.name != null) {
          return false;
        }
      } else if (!name.equals(other.name)) {
        return false;
      }
      if (Double.doubleToLongBits(price) != Double.doubleToLongBits(other.price)) {
        return false;
      }
      return true;
    }

  }

  private static int compareByteArrays(byte[] o1, byte[] o2) {
    int minLen = Math.min(o1.length, o2.length);

    for (int i = 0; i < minLen; i++) {
      int a = (o1[i] & 0xff);
      int b = (o2[i] & 0xff);

      if (a != b) {
        return a - b;
      }
    }
    return o1.length - o2.length;
  }

  /**
   * Make sure that byte comparison works in the CKSLM.
   */
  private class BytesCSKLM extends ConcurrentSkipListMap<byte[], byte[]> {

    public BytesCSKLM() {
      super(new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
          return compareByteArrays(o1, o2);
        }
      });
    }

    @Override
    public boolean remove(Object keyO, Object valueO) {
      byte[] key = (byte[]) keyO;
      byte[] value = (byte[]) valueO;

      if (containsKey(key) && get(key).equals(value)) {
        remove(key);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public boolean replace(byte[] key, byte[] oldValue, byte[] newValue) {
      if (containsKey(key) && Arrays.equals(get(key), oldValue)) {
        put(key, newValue);
        return true;
      } else {
        return false;
      }
    }
  }

  private static final class DoughnutSerializer extends Serializer<Doughnut> {
    private final Kryo kryo = new Kryo();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String toJson(Doughnut v) throws IOException {
      return objectMapper.writeValueAsString(v);
    }

    @Override
    public Doughnut fromJson(String v) throws IOException {
      return objectMapper.readValue(v, Doughnut.class);
    }

    public DoughnutSerializer() {
    }

    @Override
    public byte[] convert(Doughnut v) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try(Output output = new Output(baos)){
        kryo.writeObject(output, v);
      }
      return baos.toByteArray();
    }

    @Override
    public Doughnut revert(byte[] v) {
      try(Input input = new Input(new ByteArrayInputStream(v))) {
        return kryo.readObject(input, Doughnut.class);
      }
    }
  }
}
