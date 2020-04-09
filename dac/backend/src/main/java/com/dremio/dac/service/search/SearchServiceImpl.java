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
package com.dremio.dac.service.search;

import static com.dremio.dac.service.search.SearchIndexManager.DATASET_COLUMNS_LC;
import static com.dremio.dac.service.search.SearchIndexManager.NAME_LC;
import static com.dremio.dac.service.search.SearchIndexManager.PATH_UNQUOTED_LC;
import static com.dremio.dac.service.search.SearchIndexManager.TAGS_LC;
import static com.dremio.datastore.SearchQueryUtils.newBoost;
import static com.dremio.datastore.SearchQueryUtils.newTermQuery;
import static com.dremio.datastore.SearchQueryUtils.newWildcardQuery;
import static com.dremio.datastore.SearchQueryUtils.or;
import static com.dremio.exec.ExecConstants.SEARCH_SERVICE_RELEASE_LEADERSHIP_MS;
import static com.dremio.service.namespace.NamespaceServiceImpl.DAC_NAMESPACE;
import static com.dremio.service.scheduler.ScheduleUtils.scheduleForRunningOnceAt;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Provider;

import com.dremio.common.WakeupHandler;
import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.dac.service.collaboration.CollaborationTagStore;
import com.dremio.datastore.KVStoreTuple;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.indexed.AuxiliaryIndex;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceUtils;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.services.configuration.ConfigurationStore;

/**
 * Search Service - allows searching of namespace entities using a separate search index
 */
public class SearchServiceImpl implements SearchService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SearchServiceImpl.class);

  static final int MAX_SEARCH_RESULTS = 50;

  public static final String LOCAL_TASK_LEADER_NAME = "searchservice";

  private final Provider<NamespaceService> namespaceServiceProvider;
  private final Provider<SystemOptionManager> optionManagerProvider;

  private final Provider<LegacyKVStoreProvider> storeProvider;
  private final Provider<SchedulerService> schedulerService;
  private final ExecutorService executorService;
  private WakeupHandler wakeupHandler;
  private SearchIndexManager manager;
  private CollaborationTagStore collaborationTagStore;
  private ConfigurationStore configurationStore;
  private AuxiliaryIndex<String, NameSpaceContainer, SearchContainer> searchIndex;

  public SearchServiceImpl(
    Provider<NamespaceService> namespaceServiceProvider,
    Provider<SystemOptionManager> optionManagerProvider,
    Provider<LegacyKVStoreProvider> storeProvider,
    Provider<SchedulerService> schedulerService,
    ExecutorService executorService
  ) {
    this.namespaceServiceProvider = namespaceServiceProvider;
    this.optionManagerProvider = optionManagerProvider;
    this.storeProvider = storeProvider;
    this.schedulerService = schedulerService;
    this.executorService = executorService;
  }

  @Override
  public void start() throws Exception {
    LegacyKVStoreProvider kvStoreProvider = storeProvider.get();
    final LocalKVStoreProvider localKVStoreProvider = kvStoreProvider.unwrap(LocalKVStoreProvider.class);
    // TODO DX-14433 - should have better way to deal with Local/Remote KVStore
    if (localKVStoreProvider == null) {
      logger.warn("Search search could not start as kv store is not local");
      return;
    }

    searchIndex = localKVStoreProvider.getAuxiliaryIndex("catalog-search", DAC_NAMESPACE, SearchIndexManager.NamespaceSearchConverter.class);
    collaborationTagStore = new CollaborationTagStore(storeProvider.get());
    configurationStore = new ConfigurationStore(storeProvider.get());
    manager = new SearchIndexManager(namespaceServiceProvider, collaborationTagStore, configurationStore, searchIndex);
    wakeupHandler = new WakeupHandler(executorService, manager);

    schedulerService.get().schedule(scheduleForRunningOnceAt(
      getNextRefreshTimeInMillis(),
      LOCAL_TASK_LEADER_NAME, getNextReleaseLeadership(), TimeUnit.MILLISECONDS),
      new Runnable() {
        @Override
        public void run() {
          wakeupManager("periodic refresh");
          schedulerService.get().schedule(scheduleForRunningOnceAt(
            getNextRefreshTimeInMillis(),
            LOCAL_TASK_LEADER_NAME, getNextReleaseLeadership(), TimeUnit.MILLISECONDS),this);
        }
      });
  }

  @Override
  public List<SearchContainer> search(String query, String username) throws NamespaceException {
    final Stream<NameSpaceContainer> namespaceResults = searchNamespace(query);

    return createSearchEntities(namespaceResults);
  }

  /**
   * Takes a list of NameSpaceContainers and creates SearchEntities from them.
   *
   * @param namespaceResults list of namespace search results
   */
  List<SearchContainer> createSearchEntities(Stream<NameSpaceContainer> namespaceResults) {
    final List<SearchContainer> results = namespaceResults.map(input -> {
      return new SearchContainer(input, null);
    }).collect(Collectors.toList());

    fillCollaborationTags(results);

    return results;
  }

  /**
   * Takes a list of SearchContainer and fills in the collaboration tags.
   *
   * @param results list of SearchContainer with no tags
   */
  private void fillCollaborationTags(List<SearchContainer> results) {
    final LegacyIndexedStore.LegacyFindByCondition findByCondition = new LegacyIndexedStore.LegacyFindByCondition();
    final List<SearchTypes.SearchQuery> searchQueries = StreamSupport.stream(results.spliterator(), false)
      .map(input -> {
        return newTermQuery(CollaborationTagStore.ENTITY_ID, NamespaceUtils.getId(input.getNamespaceContainer()));
      }).collect(Collectors.toList());

    findByCondition.setCondition(or(searchQueries));

    final Map<String, CollaborationTag> hash = new HashMap<>();

    collaborationTagStore.find(findByCondition).forEach(input -> {
      hash.put(input.getKey(), input.getValue());
    });

    // fill in
    results.forEach(input -> {
      String id = NamespaceUtils.getId(input.getNamespaceContainer());
      if (hash.containsKey(id)) {
        input.setCollaborationTag(hash.get(id));
      }
    });
  }

  /**
   * Searches the namespace store using the auxiliary index.
   *
   * @param query the search query to run
   * @return the namespace store results for the search query
   */
  Stream<NameSpaceContainer> searchNamespace(String query) {
    // exact query matches are boosted 10 fold
    final List<SearchTypes.SearchQuery> queries = new ArrayList<>(getQueriesForSearchTerm(query, 10));

    if (query != null) {
      // split the trimmed query by space
      Arrays.stream(query.trim().split(" ")).forEach(term -> {
        queries.addAll(getQueriesForSearchTerm(term, 1));
      });
    }

    final FindByCondition findByCondition = getFindByCondition(queries);
    Iterable<Document<KVStoreTuple<String>, KVStoreTuple<NameSpaceContainer>>> entries = searchIndex.find(findByCondition);

    return StreamSupport.stream(entries.spliterator(), false).map(input -> {
      return input.getValue().getObject();
    });
  }

  protected FindByCondition getFindByCondition(List<SearchTypes.SearchQuery> queries) {
    final ImmutableFindByCondition.Builder builder = new ImmutableFindByCondition.Builder()
      .setCondition(or(queries));

    // Note: Reproducing the behavior of the legacy version of FindByCondition, which sets the page
    // size to be the limit if the page size is smaller than the limit.
    if (FindByCondition.DEFAULT_PAGE_SIZE < MAX_SEARCH_RESULTS) {
      builder.setPageSize(MAX_SEARCH_RESULTS);
    }
    builder.setLimit(MAX_SEARCH_RESULTS);
    return builder.build();
  }

  private List<SearchTypes.SearchQuery> getQueriesForSearchTerm(String term, int boostMultiplier) {
    final String lcTerm = term == null ? "" : term.toLowerCase();
    final String lcWildcardQuery = String.format("*%s*", lcTerm);

    final List<SearchTypes.SearchQuery> queries = new ArrayList<>();

    // exact name matches
    queries.add(newBoost(newTermQuery(NAME_LC.getIndexFieldName(), lcTerm), 5 * boostMultiplier));

    // exact tag matches
    queries.add(newBoost(newTermQuery(TAGS_LC.getIndexFieldName(), lcTerm), 4 * boostMultiplier));

    // exact path segment matches
    queries.add(newBoost(newTermQuery(PATH_UNQUOTED_LC.getIndexFieldName(), lcTerm), 3 * boostMultiplier));

    // wildcard searches
    queries.add(newBoost(newWildcardQuery(PATH_UNQUOTED_LC.getIndexFieldName(), lcWildcardQuery), 2 * boostMultiplier));
    queries.add(newBoost(newWildcardQuery(TAGS_LC.getIndexFieldName(), lcWildcardQuery), 2 * boostMultiplier));

    // these are the least important boost wise
    queries.add(newBoost(newWildcardQuery(DATASET_COLUMNS_LC.getIndexFieldName(), lcWildcardQuery), boostMultiplier));

    return queries;
  }

  private Instant getNextRefreshTimeInMillis() {
    long option = optionManagerProvider.get().getOption(ExecConstants.SEARCH_MANAGER_REFRESH_MILLIS);
    return Instant.ofEpochMilli(System.currentTimeMillis() + option);
  }

  private long getNextReleaseLeadership() {
    return optionManagerProvider.get().getOption(SEARCH_SERVICE_RELEASE_LEADERSHIP_MS);
  }

  @Override
  public void close() throws Exception {
  }

  public void wakeupManager(String reason) {
    if (wakeupHandler != null) {
      wakeupHandler.handle(reason);
    }
  }
}
