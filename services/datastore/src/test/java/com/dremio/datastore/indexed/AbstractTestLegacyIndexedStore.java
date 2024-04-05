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

import com.dremio.datastore.KVUtil;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.indexed.doughnut.Doughnut;
import com.dremio.datastore.indexed.doughnut.DoughnutIndexKeys;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Test the legacy indexed store implementation */
public abstract class AbstractTestLegacyIndexedStore {

  private LegacyKVStoreProvider provider;
  private LegacyIndexedStore<String, Doughnut> store;
  private Writer writer;

  private final Doughnut d1 = new Doughnut("original", "glazed", 1.29);
  private final Doughnut d2 = new Doughnut("custard", "bavarian creme with chocolate icing", 1.39);
  private final Doughnut d3 = new Doughnut("sourdough", "cake with glaze", 1.10);

  private List<Integer> getCounts(String... filters) {
    List<SearchQuery> queries =
        Lists.transform(
            Arrays.asList(filters),
            new Function<String, SearchQuery>() {
              @Override
              public SearchQuery apply(String input) {
                return SearchFilterToQueryConverter.toQuery(input, DoughnutIndexKeys.MAPPING);
              }
            });

    return store.getCounts(queries.toArray(new SearchQuery[queries.size()]));
  }

  protected abstract LegacyKVStoreProvider createKVStoreProvider() throws Exception;

  protected abstract LegacyIndexedStore<String, Doughnut> supplyStore();

  protected LegacyKVStoreProvider getProvider() {
    return provider;
  }

  protected LegacyIndexedStore<String, Doughnut> getStore() {
    return store;
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
    provider.start();
    store = supplyStore();
  }

  private final class Writer extends Thread implements Runnable {
    @Override
    public void run() {
      for (int i = 0; i < 100000; ++i) {
        String name = "pwriter_" + Integer.toString(i);
        store.put(name, new Doughnut(name, "bad_flavor_" + (i % 10), i));
      }
    }
  }

  @After
  public void after() throws Exception {
    if (writer != null) {
      writer.join();
    }
    provider.close();
  }

  @Test
  public void put() {
    store.put("a", d1);
    checkFindByName(d1);
    checkFindByPrice(d1);
  }

  @Test
  public void counts() {
    addDoughnutsToStore();

    assertEquals(
        ImmutableList.of(1, 2, 0),
        getCounts("n==original", "p=gt=1.10;p=lt=1.40", "p=lt=1.11;n=lt=custard"));
  }

  @Test
  public void term() {
    addDoughnutsToStore();
    final SearchQuery termQuery = SearchQueryUtils.newTermQuery("flavor", d2.getFlavor());
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(termQuery);
    verifyDoughnutsRetrieved(ImmutableList.of(d2), toListOfDoughnuts(store.find(condition)));
  }

  @Test
  public void termInt() {
    final Doughnut d1 = new Doughnut("special", "dream", 2.1, 1, 2L);
    final Doughnut d2 = new Doughnut("regular", "blueberry", 1.8, 2, 3L);
    store.put("a", d1);
    store.put("b", d2);
    final SearchQuery termIntQuery = SearchQueryUtils.newTermQuery("thickness", d1.getThickness());
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(termIntQuery);
    verifyDoughnutsRetrieved(ImmutableList.of(d1), toListOfDoughnuts(store.find(condition)));
  }

  @Test
  public void termDouble() {
    addDoughnutsToStore();
    final SearchQuery termDoubleQuery = SearchQueryUtils.newTermQuery("price", d2.getPrice());
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(termDoubleQuery);
    verifyDoughnutsRetrieved(ImmutableList.of(d2), toListOfDoughnuts(store.find(condition)));
  }

  @Test
  public void termLong() {
    final Doughnut d1 = new Doughnut("special", "dream", 2.1, 1, 2L);
    final Doughnut d2 = new Doughnut("regular", "blueberry", 1.8, 2, 3L);
    store.put("a", d1);
    store.put("b", d2);
    final SearchQuery termLongQuery = SearchQueryUtils.newTermQuery("diameter", d2.getDiameter());
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(termLongQuery);
    verifyDoughnutsRetrieved(ImmutableList.of(d2), toListOfDoughnuts(store.find(condition)));
  }

  @Test
  public void rangeTerm() {
    addDoughnutsToStore();
    final SearchQuery rangeTermQuery =
        SearchQueryUtils.newRangeTerm("name", "custard", "original", true, true);
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(rangeTermQuery);
    verifyDoughnutsRetrieved(ImmutableList.of(d1, d2), toListOfDoughnuts(store.find(condition)));
  }

  @Test
  public void rangeInt() {
    final Doughnut d1 = new Doughnut("special", "dream", 2.1, 1, 2L);
    final Doughnut d2 = new Doughnut("regular", "blueberry", 1.8, 2, 3L);
    store.put("a", d1);
    store.put("b", d2);
    final SearchQuery rangeIntQuery = SearchQueryUtils.newRangeInt("thickness", 0, 1, true, true);
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(rangeIntQuery);
    verifyDoughnutsRetrieved(ImmutableList.of(d1), toListOfDoughnuts(store.find(condition)));
  }

  @Test
  public void rangeDouble() {
    addDoughnutsToStore();
    final SearchQuery rangeDoubleQuery =
        SearchQueryUtils.newRangeDouble("price", 1.10, 1.29, true, false);
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(rangeDoubleQuery);
    verifyDoughnutsRetrieved(ImmutableList.of(d3), toListOfDoughnuts(store.find(condition)));
  }

  @Test
  public void rangeLong() {
    final Doughnut d1 = new Doughnut("special", "dream", 2.1, 1, 2L);
    final Doughnut d2 = new Doughnut("regular", "blueberry", 1.8, 2, 3L);
    store.put("a", d1);
    store.put("b", d2);
    final SearchQuery rangeLongQuery =
        SearchQueryUtils.newRangeLong("diameter", 0L, 2L, false, true);
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(rangeLongQuery);
    verifyDoughnutsRetrieved(ImmutableList.of(d1), toListOfDoughnuts(store.find(condition)));
  }

  @Test
  public void exists() {
    addDoughnutsToStore();
    final SearchQuery query = SearchQueryUtils.newExistsQuery("name");
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(query);
    assertEquals(3, Iterables.size(store.find(condition)));
  }

  @Test
  public void notExists() {
    addDoughnutsToStore();
    final SearchQuery query = SearchQueryUtils.newDoesNotExistQuery("randomfield");
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(query);
    assertEquals(3, Iterables.size(store.find(condition)));
  }

  @Test
  public void contains() {
    store.put("a", d1);
    final SearchQuery containsQuery = SearchQueryUtils.newContainsTerm("name", "rigi");
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(containsQuery);
    assertEquals(1, Iterables.size(store.find(condition)));
  }

  @Test
  public void prefix() {
    addDoughnutsToStore();
    final SearchQuery prefixQuery = SearchQueryUtils.newPrefixQuery("name", "cus");
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(prefixQuery);
    assertEquals(1, Iterables.size(store.find(condition)));
  }

  @Test
  public void and() {
    addDoughnutsToStore();
    final SearchQuery firstQuery = SearchQueryUtils.newTermQuery("name", "custard");
    final SearchQuery secondQuery = SearchQueryUtils.newTermQuery("price", 1.29);
    final SearchQuery andQuery = SearchQueryUtils.and(firstQuery, secondQuery);
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(andQuery);
    assertEquals(0, Iterables.size(store.find(condition)));
  }

  @Test
  public void or() {
    addDoughnutsToStore();
    final SearchQuery firstQuery = SearchQueryUtils.newTermQuery("name", "custard");
    final SearchQuery secondQuery = SearchQueryUtils.newTermQuery("price", 1.29);
    final SearchQuery orQuery = SearchQueryUtils.or(firstQuery, secondQuery);
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(orQuery);
    assertEquals(2, Iterables.size(store.find(condition)));
  }

  @Test
  public void not() {
    addDoughnutsToStore();
    final SearchQuery query =
        SearchQueryUtils.not(SearchQueryUtils.newTermQuery("name", "custard"));
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(query);
    assertEquals(2, Iterables.size(store.find(condition)));
  }

  @Test
  public void containsSpecialChars() {
    final Doughnut d = new Doughnut("spe*\\?ial", "Dulce De Leche", 0.25);
    store.put("a", d);
    store.put("b", d1);
    final SearchQuery containsQuery = SearchQueryUtils.newContainsTerm("name", "spe*\\?ial");
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(containsQuery);
    assertEquals(1, Iterables.size(store.find(condition)));
  }

  @Test
  public void delete() {
    store.put("a", d1);
    checkFindByName(d1);
    store.delete("a");
    assertEquals(
        0,
        Iterables.size(
            store.find(
                new LegacyIndexedStore.LegacyFindByCondition()
                    .setCondition("n==" + d1.getName(), DoughnutIndexKeys.MAPPING))));
  }

  @Test
  public void paginatedSearch() {
    final int numDoughnuts = 4000;
    addData(numDoughnuts);

    final List<Doughnut> found = findByFlavor(new Doughnut("", "good_flavor_0", 0));
    assertEquals(numDoughnuts / 4, found.size());
  }

  @Test
  public void limit() {
    addData(100);
    final int limit = 2;

    final Iterable<Map.Entry<String, Doughnut>> result =
        store.find(
            new LegacyIndexedStore.LegacyFindByCondition()
                .setCondition(SearchQueryUtils.newMatchAllQuery())
                .setLimit(limit));

    assertEquals(limit, Iterables.size(result));
  }

  @Test
  public void skip() {
    store.put("a", d1);
    store.put("b", d2);
    store.put("c", d3);
    final int offset = 2;

    final Iterable<Map.Entry<String, Doughnut>> result =
        store.find(
            new LegacyIndexedStore.LegacyFindByCondition()
                .setCondition(SearchQueryUtils.newMatchAllQuery())
                .setOffset(offset));

    final List<Doughnut> doughnuts = Lists.newArrayList(KVUtil.values(result));
    assertEquals(d3, doughnuts.get(0));
  }

  @Test
  public void order() {
    // Test sorting by name ascending, then price descending.
    final Doughnut firstDonut = new Doughnut("2name", "1flavor", 10);
    final Doughnut secondDonut = new Doughnut("2name", "2flavor", 1);
    final Doughnut thirdDonut = new Doughnut("3name", "3flavor", 0);
    store.put("doughnut1", thirdDonut); // should be third.
    store.put("doughnut2", secondDonut); // should be second.
    store.put("doughnut3", firstDonut); // should be first;

    final Iterable<Map.Entry<String, Doughnut>> result =
        store.find(
            new LegacyIndexedStore.LegacyFindByCondition()
                .setCondition(SearchQueryUtils.newMatchAllQuery())
                .addSorting(
                    SearchTypes.SearchFieldSorting.newBuilder()
                        .setField("name")
                        .setType(SearchTypes.SearchFieldSorting.FieldType.STRING)
                        .setOrder(SearchTypes.SortOrder.ASCENDING)
                        .build())
                .addSorting(
                    SearchTypes.SearchFieldSorting.newBuilder()
                        .setField("price")
                        .setType(SearchTypes.SearchFieldSorting.FieldType.DOUBLE)
                        .setOrder(SearchTypes.SortOrder.DESCENDING)
                        .build()));

    final List<Doughnut> doughnuts = Lists.newArrayList(KVUtil.values(result));
    assertEquals(firstDonut, doughnuts.get(0));
    assertEquals(secondDonut, doughnuts.get(1));
    assertEquals(thirdDonut, doughnuts.get(2));
  }

  @Test
  public void searchAfterWithParallelUpdates() throws InterruptedException {
    int numDoughnuts = 100000;

    // Populate store with good_flavor_*.
    List<String> fetchKeys = new ArrayList<>();
    for (int i = 0; i < numDoughnuts; i++) {
      String name = Integer.toString(i);
      store.put(name, new Doughnut(name, "good_flavor_" + (i % 10), i));
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
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(andQuery);
    verifyDoughnutsRetrieved(ImmutableList.of(), toListOfDoughnuts(store.find(condition)));
  }

  @Test
  public void emptyOr() {
    addDoughnutsToStore();
    final SearchQuery orQuery = SearchQueryUtils.or();
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(orQuery);
    verifyDoughnutsRetrieved(ImmutableList.of(), toListOfDoughnuts(store.find(condition)));
  }

  @Test
  public void testWildCardMatch() {
    final LegacyIndexedStore<String, Doughnut> store = getStore();
    final int totalDocs = 20;
    final String pattern = "*rigi*";
    final String nameToUse = "original";

    // populate documents
    for (int i = 0; i < totalDocs; ++i) {
      String dname = nameToUse;
      if (i % 2 != 0) {
        dname = "random_name_" + Integer.toString(i);
      }
      final Doughnut d = new Doughnut(dname, nameToUse + "_" + Integer.toString(i), 1.5 * i);
      store.put(Integer.toString(i), d);
    }

    final SearchTypes.SearchQuery query =
        SearchQueryUtils.or(
            SearchQueryUtils.newWildcardQuery(DoughnutIndexKeys.NAME.getIndexFieldName(), pattern),
            SearchQueryUtils.newWildcardQuery(
                DoughnutIndexKeys.FLAVOR.getIndexFieldName(), pattern));
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(query);

    final Iterable<Map.Entry<String, Doughnut>> items = store.find(condition);

    // either 'name' or 'flavor' should have matched
    Assert.assertEquals(totalDocs, Iterables.size(items));
  }

  @Test
  public void testWildCardWithSomeMatches() {
    final LegacyIndexedStore<String, Doughnut> store = getStore();
    final int totalDocs = 10;
    final int totalNonMatchDocs = 10;
    final String pattern = "*rigi*";
    final String nameToUse = "original";

    // populate documents that would be matched
    for (int i = 0; i < totalDocs; ++i) {
      final Doughnut d = new Doughnut(nameToUse, nameToUse + "_" + Integer.toString(i), 1.5 * i);
      store.put(Integer.toString(i), d);
    }

    // populate documents that would not-be matched
    for (int i = totalDocs; i < totalNonMatchDocs + totalDocs; ++i) {
      final Doughnut d = new Doughnut("random", nameToUse + "_" + Integer.toString(i), 1.5 * i);
      store.put(Integer.toString(i), d);
    }

    final SearchTypes.SearchQuery query =
        SearchQueryUtils.and(
            SearchQueryUtils.newWildcardQuery(DoughnutIndexKeys.NAME.getIndexFieldName(), pattern),
            SearchQueryUtils.newWildcardQuery(
                DoughnutIndexKeys.FLAVOR.getIndexFieldName(), pattern));
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(query);

    // we should see all the documents in the store
    final Iterable<Map.Entry<String, Doughnut>> allItems = store.find();
    Assert.assertEquals((totalDocs + totalNonMatchDocs), Iterables.size(allItems));

    final Iterable<Map.Entry<String, Doughnut>> items = store.find(condition);

    // either 'name' or 'flavor' should have matched
    Assert.assertEquals(totalDocs, Iterables.size(items));
  }

  @Test
  public void testWildCardNoMatch() {
    final LegacyIndexedStore<String, Doughnut> store = getStore();
    final int totalDocs = 20;
    final String nameToUse = "original";
    final String pattern = "testWildCardNoMatch";

    // populate documents
    for (int i = 0; i < totalDocs; ++i) {
      String dname = nameToUse;
      if (i % 2 != 0) {
        dname = "random_name_" + Integer.toString(i);
      }
      final Doughnut d = new Doughnut(dname, nameToUse + "_" + Integer.toString(i), 1.5 * i);
      store.put(Integer.toString(i), d);
    }

    final SearchTypes.SearchQuery query =
        SearchQueryUtils.or(
            SearchQueryUtils.newWildcardQuery(DoughnutIndexKeys.NAME.getIndexFieldName(), pattern),
            SearchQueryUtils.newWildcardQuery(
                DoughnutIndexKeys.FLAVOR.getIndexFieldName(), pattern));
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(query);

    final Iterable<Map.Entry<String, Doughnut>> items = store.find(condition);

    // no documents should have matched
    Assert.assertEquals(0, Iterables.size(items));
  }

  @Test
  public void testWildCardSpecialChars() {
    final LegacyIndexedStore<String, Doughnut> store = getStore();
    final int totalDocs = 20;
    final String pattern = "*r\\*igi*";
    final String nameToUse = "or*iginal";

    // populate documents
    for (int i = 0; i < totalDocs; ++i) {
      String dname = nameToUse;
      if (i % 2 != 0) {
        dname = "random_name_" + Integer.toString(i);
      }
      final Doughnut d = new Doughnut(dname, nameToUse + "_" + Integer.toString(i), 1.5 * i);
      store.put(Integer.toString(i), d);
    }

    final SearchTypes.SearchQuery query =
        SearchQueryUtils.or(
            SearchQueryUtils.newWildcardQuery(DoughnutIndexKeys.NAME.getIndexFieldName(), pattern),
            SearchQueryUtils.newWildcardQuery(
                DoughnutIndexKeys.FLAVOR.getIndexFieldName(), pattern));
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(query);

    final Iterable<Map.Entry<String, Doughnut>> items = store.find(condition);

    // either 'name' or 'flavor' should have matched
    Assert.assertEquals(totalDocs, Iterables.size(items));
  }

  @Test
  public void testWildCardCustomPattern() {
    final LegacyIndexedStore<String, Doughnut> store = getStore();
    final int totalDocs = 5;

    // populate documents
    for (int i = 0; i < totalDocs; ++i) {
      final Doughnut d = new Doughnut("best_dougnout", "best_vanilla", 1.5 * i);
      store.put(Integer.toString(i), d);
    }

    final String pattern = "*best*d*";

    final SearchTypes.SearchQuery query =
        SearchQueryUtils.or(
            SearchQueryUtils.newWildcardQuery(DoughnutIndexKeys.NAME.getIndexFieldName(), pattern),
            SearchQueryUtils.newWildcardQuery(
                DoughnutIndexKeys.FLAVOR.getIndexFieldName(), pattern));
    final LegacyIndexedStore.LegacyFindByCondition condition =
        new LegacyIndexedStore.LegacyFindByCondition().setCondition(query);

    final Iterable<Map.Entry<String, Doughnut>> items = store.find(condition);
    Assert.assertEquals(5, Iterables.size(items));
  }

  private void addData(int numDoughnuts) {
    for (int i = 0; i < numDoughnuts; i++) {
      final String name = Integer.toString(i);
      store.put(name, new Doughnut(name, "good_flavor_" + (i % 4), i));
    }
  }

  protected void addDoughnutsToStore() {
    store.put("a", d1);
    store.put("b", d2);
    store.put("c", d3);
  }

  protected void verifyDoughnutsRetrieved(List<Doughnut> expected, List<Doughnut> retrieved) {
    assertEquals(expected.size(), retrieved.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), retrieved.get(i));
    }
  }

  protected List<Doughnut> toListOfDoughnuts(Iterable<Map.Entry<String, Doughnut>> docs) {
    return Lists.newArrayList(KVUtil.values(docs));
  }

  private void checkFindByName(Doughnut d) {
    final Iterable<Map.Entry<String, Doughnut>> iter =
        store.find(
            new LegacyIndexedStore.LegacyFindByCondition()
                .setCondition("n==" + d.getName(), DoughnutIndexKeys.MAPPING));
    final List<Doughnut> doughnuts = Lists.newArrayList(KVUtil.values(iter));
    Assert.assertEquals(1, doughnuts.size());
    Assert.assertEquals(d, doughnuts.get(0));
  }

  private void assertNoResult(String filterStr) {
    assertEquals(
        0,
        Iterables.size(
            store.find(
                new LegacyIndexedStore.LegacyFindByCondition()
                    .setCondition(filterStr, DoughnutIndexKeys.MAPPING))));
  }

  private void checkFindByPrice(Doughnut d) {
    final Iterable<Map.Entry<String, Doughnut>> iter =
        store.find(
            new LegacyIndexedStore.LegacyFindByCondition()
                .setCondition("p==" + d.getPrice(), DoughnutIndexKeys.MAPPING));
    final List<Doughnut> doughnuts = Lists.newArrayList(KVUtil.values(iter));
    Assert.assertEquals(1, doughnuts.size());
    Assert.assertEquals(d, doughnuts.get(0));
  }

  private List<Doughnut> findByFlavor(Doughnut d) {
    final Iterable<Map.Entry<String, Doughnut>> iter =
        store.find(
            new LegacyIndexedStore.LegacyFindByCondition()
                .setCondition("f==" + d.getFlavor(), DoughnutIndexKeys.MAPPING));

    return Lists.newArrayList(KVUtil.values(iter));
  }
}
