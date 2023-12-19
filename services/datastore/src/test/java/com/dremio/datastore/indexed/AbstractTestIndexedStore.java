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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.indexed.doughnut.Doughnut;
import com.dremio.datastore.indexed.doughnut.DoughnutIndexKeys;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Test the indexed store implementation
 */
public abstract class AbstractTestIndexedStore {

  private Writer writer;

  private KVStoreProvider provider;
  private IndexedStore<String, Doughnut> kvStore;

  private final Doughnut d1 = new Doughnut("original", "glazed", 1.29);
  private final Doughnut d2 = new Doughnut("custard", "bavarian creme with chocolate icing", 1.39);
  private final Doughnut d3 = new Doughnut("sourdough", "cake with glaze", 1.10);
  private final Map<String, Doughnut> doughnutMap = ImmutableMap.of("a", d1, "b", d2, "c", d3);

  private List<Integer> getCounts(String... filters) {
    List<SearchQuery> queries = Lists.transform(Arrays.asList(filters), new Function<String, SearchQuery>() {
      @Override
      public SearchQuery apply(String input) {
        return SearchFilterToQueryConverter.toQuery(input, DoughnutIndexKeys.MAPPING);
      }
    });

    return kvStore.getCounts(queries.toArray(new SearchQuery[queries.size()]));
  }

  protected abstract KVStoreProvider createKVStoreProvider() throws Exception;

  protected abstract IndexedStore<String, Doughnut> createKVStore();

  protected KVStoreProvider getProvider(){
    return provider;
  }

  protected IndexedStore<String, Doughnut> getKvStore() {
    return kvStore;
  }

  protected void closeResources() throws Exception {
  }

  protected Doughnut getD1() {
    return d1;
  }

  protected Doughnut getD2() {
    return d2;
  }

  protected Doughnut getD3() {
    return d3;
  }

  @Before
  public void before() throws Exception {
    provider = createKVStoreProvider();
    kvStore = createKVStore();
  }

  private final class Writer extends Thread implements Runnable {
    @Override
    public void run() {
      for (int i = 0; i < 5000; ++i) {
        String name = "pwriter_" + Integer.toString(i);
        kvStore.put(name, new Doughnut(name, "bad_flavor_" + (i % 10), i));
      }
    }
  }

  @After
  public final void after() throws Exception {
    closeResources();
    if (writer != null) {
      writer.join();
    }
    provider.close();
  }

  @Test
  public void put(){
    kvStore.put("a", d1);
    checkFindByName(d1);
    checkFindByPrice(d1);
  }

  @Test
  public void counts(){
    addDoughnutsToStore();

    assertEquals(
        ImmutableList.of(1, 2, 0),
        getCounts("n==original", "p=gt=1.10;p=lt=1.40", "p=lt=1.11;n=lt=custard"));
  }

  @Test
  public void term() {
    addDoughnutsToStore();
    final SearchQuery termQuery = SearchQueryUtils.newTermQuery("flavor", d2.getFlavor());
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(termQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d2), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void termInt() {
    final Doughnut d1 = new Doughnut("special", "dream", 2.1, 1, 2L);
    final Doughnut d2 = new Doughnut("regular", "blueberry", 1.8, 2, 3L);
    kvStore.put("a", d1);
    kvStore.put("b", d2);
    final SearchQuery termQuery = SearchQueryUtils.newTermQuery("thickness", d1.getThickness());
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(termQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d1), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void termDouble() {
    addDoughnutsToStore();
    final SearchQuery termDoubleQuery = SearchQueryUtils.newTermQuery("price", d2.getPrice());
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(termDoubleQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d2), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void termLong() {
    final Doughnut d1 = new Doughnut("special", "dream", 2.1, 1, 2L);
    final Doughnut d2 = new Doughnut("regular", "blueberry", 1.8, 2, 3L);
    kvStore.put("a", d1);
    kvStore.put("b", d2);
    final SearchQuery termQuery = SearchQueryUtils.newTermQuery("diameter", d2.getDiameter());
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(termQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d2), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void rangeTerm() {
    addDoughnutsToStore();
    final SearchQuery termRangeQuery = SearchQueryUtils.newRangeTerm("name", "custard", "original", true, true);
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(termRangeQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d1, d2), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void rangeDouble() {
    addDoughnutsToStore();
    final SearchQuery rangeDoubleQuery = SearchQueryUtils.newRangeDouble("price", 1.10, 1.29, true, false);
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(rangeDoubleQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d3), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void rangeInt() {
    final Doughnut d1 = new Doughnut("special", "dream", 2.1, 1, 2L);
    final Doughnut d2 = new Doughnut("regular", "blueberry", 1.8,2, 3L);
    kvStore.put("a", d1);
    kvStore.put("b", d2);
    final SearchQuery rangeIntQuery = SearchQueryUtils.newRangeInt("thickness", 0, 1, true, true);
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(rangeIntQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d1), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void rangeLong() {
    final Doughnut d1 = new Doughnut("special", "dream", 2.1, 1, 2L);
    final Doughnut d2 = new Doughnut("regular", "blueberry", 1.8,2, 3L);
    kvStore.put("a", d1);
    kvStore.put("b", d2);
    final SearchQuery rangeLongQuery = SearchQueryUtils.newRangeLong("diameter", 0L, 2L, false, true);
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(rangeLongQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d1), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void exists() {
    addDoughnutsToStore();
    final SearchQuery containsQuery = SearchQueryUtils.newExistsQuery("name");
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(containsQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d1, d2, d3), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void notExists() {
    addDoughnutsToStore();
    final SearchQuery containsQuery = SearchQueryUtils.newDoesNotExistQuery("randomfield");
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(containsQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d1, d2, d3), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void contains() {
    addDoughnutsToStore();
    final SearchQuery containsQuery = SearchQueryUtils.newContainsTerm("name", "rigi");
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(containsQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d1), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void prefix() {
    addDoughnutsToStore();
    final SearchQuery containsQuery = SearchQueryUtils.newPrefixQuery("name", "cus");
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(containsQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d2), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void and() {
    addDoughnutsToStore();
    final SearchQuery firstQuery = SearchQueryUtils.newTermQuery("name", "custard");
    final SearchQuery secondQuery = SearchQueryUtils.newTermQuery("price", 1.29);
    final SearchQuery andQuery = SearchQueryUtils.and(firstQuery, secondQuery);
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(andQuery).build();
    assertEquals(0, Iterables.size(kvStore.find(condition)));
  }

  @Test
  public void or() {
    addDoughnutsToStore();
    final SearchQuery firstQuery = SearchQueryUtils.newTermQuery("name", "custard");
    final SearchQuery secondQuery = SearchQueryUtils.newTermQuery("price", 1.29);
    final SearchQuery andQuery = SearchQueryUtils.or(firstQuery, secondQuery);
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(andQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(d1, d2), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void not() {
    addDoughnutsToStore();
    final SearchQuery query = SearchQueryUtils.not(SearchQueryUtils.newTermQuery("name", "original"));
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(query).build();
    assertEquals(2, Iterables.size(kvStore.find(condition)));
  }

  @Test
  public void containsSpecialChars() {
    final Doughnut special = new Doughnut("spe*\\?ial", "Dulce De Leche", 0.25);
    kvStore.put("special", special);
    addDoughnutsToStore();
    final SearchQuery containsQuery = SearchQueryUtils.newContainsTerm("name", "spe*\\?ial");
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(containsQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(special), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void delete() {
    kvStore.put("a", d1);
    checkFindByName(d1);
    kvStore.delete("a");
    assertEquals(
        0,
        Iterables.size(kvStore.find(newCondition("n==" + d1.getName(), DoughnutIndexKeys.MAPPING).build())));
  }

  @Test
  public void paginatedSearch() {
    final int numDoughnuts = 4000;

    addData(numDoughnuts);

    List<Doughnut> found = findByFlavor(new Doughnut("", "good_flavor_0", 0));
    assertEquals(numDoughnuts / 4, found.size());
  }

  @Test
  public void limit() {
    addData(100);
    final int limit = 2;

    final Iterable<Document<String, Doughnut>> result = kvStore.find(new ImmutableFindByCondition.Builder()
      .setCondition(SearchQueryUtils.newMatchAllQuery())
      .setLimit(limit)
      .build());

    assertEquals(limit, Iterables.size(result));
  }

  @Test
  public void skip() {
    kvStore.put("a", d1);
    kvStore.put("b", d2);
    kvStore.put("c", d3);
    final int offset = 2;

    final Iterable<Document<String, Doughnut>> result = kvStore.find(new ImmutableFindByCondition.Builder()
      .setCondition(SearchQueryUtils.newMatchAllQuery())
      .setOffset(offset)
      .build());

    final List<Doughnut> doughnuts = toListOfDoughnuts(result);
    assertEquals(d3, doughnuts.get(0));
  }

  @Test
  public void order() {
    // Test sorting by name ascending, then price descending.
    final Doughnut firstDonut = new Doughnut("2name", "1flavor", 10);
    final Doughnut secondDonut = new Doughnut("2name", "2flavor", 1);
    final Doughnut thirdDonut = new Doughnut("3name", "3flavor", 0);
    kvStore.put("doughnut1", thirdDonut); // should be third.
    kvStore.put("doughnut2", secondDonut); // should be second.
    kvStore.put("doughnut3", firstDonut); // should be first;

    final Iterable<Document<String, Doughnut>> result = kvStore.find(new ImmutableFindByCondition.Builder()
      .setCondition(SearchQueryUtils.newMatchAllQuery())
      .setSort(
        ImmutableList.of(
          SearchTypes.SearchFieldSorting.newBuilder()
            .setField("name").setType(SearchTypes.SearchFieldSorting.FieldType.STRING).setOrder(SearchTypes.SortOrder.ASCENDING).build(),
          SearchTypes.SearchFieldSorting.newBuilder()
            .setField("price").setType(SearchTypes.SearchFieldSorting.FieldType.DOUBLE).setOrder(SearchTypes.SortOrder.DESCENDING).build()))
      .build());

    final List<Doughnut> doughnuts = toListOfDoughnuts(result);
    assertEquals(firstDonut, doughnuts.get(0));
    assertEquals(secondDonut, doughnuts.get(1));
    assertEquals(thirdDonut, doughnuts.get(2));
  }

  @Test
  public void searchAfterWithParallelUpdates() throws InterruptedException {
    int numDoughnuts = 10000;

    // Populate store with good_flavor_*.
    List<String> fetchKeys = new ArrayList<>();
    for (int i = 0; i < numDoughnuts; i++) {
      String name = Integer.toString(i);
      kvStore.put(name, new Doughnut(name, "good_flavor_" + (i % 10), i));
      if (i % 10 == 0) {
        fetchKeys.add(name);
      }
    }

    // Lookup entries matching flavor=goodflavor_0, while there are updates in progress.
    writer = new Writer();
    writer.start();

    List<Doughnut> found = findByFlavor(new Doughnut("", "good_flavor_0", 0));
    assertEquals(numDoughnuts / 10, found.size());

    writer.join();
    writer = null;
  }

  @Test
  public void emptyAnd() {
    addDoughnutsToStore();
    final SearchQuery andQuery = SearchQueryUtils.and();
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(andQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void emptyOr() {
    addDoughnutsToStore();
    final SearchQuery orQuery = SearchQueryUtils.or();
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(orQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void version(){
    kvStore.put("a", d1);
    kvStore.put("b", d2);
    kvStore.put("c", d3);

    final SearchQuery termQuery = SearchQueryUtils.newTermQuery("version", kvStore.version());
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(termQuery).build();
    final List<Doughnut> doughnuts = toListOfDoughnuts(kvStore.find(condition));
    verifyDoughnutsRetrieved(new ArrayList<>(doughnutMap.values()), toListOfDoughnuts(kvStore.find(condition)));
  }

  private void addData(int numDoughnuts) {
    for (int i = 0; i < numDoughnuts; i++) {
      final String name = Integer.toString(i);
      kvStore.put(name, new Doughnut(name, "good_flavor_" + (i % 4), i));
    }
  }

  protected void verifyDoughnutsRetrieved(List<Doughnut> expected, List<Doughnut> retrieved) {
    assertEquals(expected.size(), retrieved.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), retrieved.get(i));
    }
  }

  protected void addDoughnutsToStore() {
    doughnutMap.forEach( (key, value) -> kvStore.put(key, value));
  }

  private void checkFindByName(Doughnut d) {
    final Iterable<Document<String, Doughnut>> iter =
        kvStore.find(newCondition("n==" + d.getName(), DoughnutIndexKeys.MAPPING).build());
    final List<Doughnut> doughnuts = toListOfDoughnuts(iter);
    Assert.assertEquals(1, doughnuts.size());
    Assert.assertEquals(d, doughnuts.get(0));
  }

  private void assertNoResult(String filterStr) {
    assertEquals(
        0, Iterables.size(kvStore.find(newCondition(filterStr, DoughnutIndexKeys.MAPPING).build())));
  }

  private void checkFindByPrice(Doughnut d) {
    final Iterable<Document<String, Doughnut>> iter =
        kvStore.find(newCondition("p==" + d.getPrice(), DoughnutIndexKeys.MAPPING).build());
    final List<Doughnut> doughnuts = toListOfDoughnuts(iter);
    Assert.assertEquals(1, doughnuts.size());
    Assert.assertEquals(d, doughnuts.get(0));
  }

  private List<Doughnut> findByFlavor(Doughnut d){
    Iterable<Document<String, Doughnut>> iter = kvStore.find(
      newCondition("f==" + d.getFlavor(), DoughnutIndexKeys.MAPPING)
      .setPageSize(100)
      .build());

    return toListOfDoughnuts(iter);
  }

  private ImmutableFindByCondition.Builder newCondition(String conditionStr, FilterIndexMapping mapping) {
    return new ImmutableFindByCondition.Builder()
      .setCondition(SearchFilterToQueryConverter.toQuery(conditionStr, mapping));
  }

  protected List<Doughnut> toListOfDoughnuts(Iterable<Document<String, Doughnut>> docs) {
    return StreamSupport.stream(docs.spliterator(), false)
      .map(doc -> doc.getValue())
      .collect(Collectors.toList());
  }


  @Test
  public void wildcard() {
    addDoughnutsToStore();
    final Doughnut e = new Doughnut("rigi", "strawberry", 4.5, 2, 3L);
    kvStore.put("e", e);
    final SearchQuery containsQuery = SearchQueryUtils.newWildcardQuery("name", "rigi");
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(containsQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(e), toListOfDoughnuts(kvStore.find(condition)));
  }

  @Test
  public void wildcardLeadingTrailing() {
    addDoughnutsToStore();
    final Doughnut e = new Doughnut("rigi", "strawberry", 4.5, 2, 3L);
    kvStore.put("e", e);
    final SearchQuery containsQuery = SearchQueryUtils.newWildcardQuery("name", "*rigi*");
    final FindByCondition condition = new ImmutableFindByCondition.Builder().setCondition(containsQuery).build();
    verifyDoughnutsRetrieved(ImmutableList.of(getD1(), e), toListOfDoughnuts(kvStore.find(condition)));
  }
}
