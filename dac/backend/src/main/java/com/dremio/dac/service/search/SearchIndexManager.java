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

import static com.dremio.datastore.SearchQueryUtils.newRangeLong;
import static com.dremio.datastore.SearchQueryUtils.newTermQuery;
import static com.dremio.datastore.SearchQueryUtils.or;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.dac.proto.model.search.SearchConfiguration;
import com.dremio.dac.service.collaboration.CollaborationTagStore;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.indexed.AuxiliaryIndex;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.namespace.DatasetIndexKeys;
import com.dremio.service.namespace.NamespaceConverter;
import com.dremio.service.namespace.NamespaceIndexKeys;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.users.SystemUser;
import com.dremio.services.configuration.ConfigurationStore;
import com.dremio.services.configuration.proto.ConfigurationEntry;
import com.google.common.base.Optional;

import io.protostuff.ByteString;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;

/**
 * Search Index Manager - maintains the search index
 */
public class SearchIndexManager implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(SearchIndexManager.class);

  public static final String CONFIG_KEY = "searchLastRefresh";
  static final IndexKey PATH_UNQUOTED_LC = IndexKey.newBuilder("pathlc", "PATH_LC", String.class)
    .setCanContainMultipleValues(true).build();
  static final IndexKey NAME_LC = IndexKey.newBuilder("namelc", "NAME_LC", String.class)
    .build();
  static final IndexKey TAGS_LC = IndexKey.newBuilder("tagslc", "TAGS_LC", String.class)
    .setCanContainMultipleValues(true)
    .build();
  static final IndexKey DATASET_COLUMNS_LC = IndexKey.newBuilder("dscolumnslc", "DATASET_COLUMNS_LC", String.class)
    .setCanContainMultipleValues(true)
    .build();

  private static final long WAKEUP_OVERLAP_MS = 10;

  private final Provider<NamespaceService> namespaceService;
  private final CollaborationTagStore collaborationTagStore;
  private final ConfigurationStore configurationStore;
  private final AuxiliaryIndex<String, NameSpaceContainer, SearchContainer> searchIndex;

  private long lastWakeupTime;

  SearchIndexManager(
    Provider<NamespaceService> namespaceService,
    CollaborationTagStore tagStore,
    ConfigurationStore configurationStore,
    AuxiliaryIndex<String, NameSpaceContainer, SearchContainer> searchIndex
  ) {
    this.collaborationTagStore = tagStore;
    this.configurationStore = configurationStore;
    this.searchIndex = searchIndex;

    this.namespaceService = namespaceService;
    this.lastWakeupTime = getSearchConfig().getLastWakeupTime();

    logger.info("Search manager created, last wakeup time was {}", getLastWakeupTime());
  }

  @Override
  public void run() {
    logger.debug("Running the search manager...");

    final long previousWakeupTime = getLastWakeupTime() - WAKEUP_OVERLAP_MS;
    updateLastWakeupTime(System.currentTimeMillis());

    try {
      handleChanges(previousWakeupTime);
    } catch (Throwable e) {
      logger.error("Search manager failed", e);
    }
  }

  /**
   * Processes any entities modified after the previous wakeup time.
   *
   * This is a join of two stores - tags and the namespace.  First get all tags that have been modified since the
   * previous wakeup.  After that, search the namespace for any newly modified entities as well as the entities that
   * have modified tags.  Finally we fetch all tags for namespace entities for whom we don't have tags from the first
   * step.
   *
   * @param previousWakeupTime previous wakeup timeout
   */
  private void handleChanges(long previousWakeupTime) {
    final Map<String, CollaborationTag> collaborationTagMap = getModifiedTags(previousWakeupTime);

    // fetch all modified namespace entities since the last wakeup and the entities that have modified collaboration tags
    final Iterable<Map.Entry<NamespaceKey, NameSpaceContainer>> namespaceEntries = getModifiedNamespaceContainers(previousWakeupTime, collaborationTagMap);

    final AtomicInteger indexCount = new AtomicInteger();

    StreamSupport.stream(namespaceEntries.spliterator(), false).filter(input -> {
      final NameSpaceContainer nameSpaceContainer = input.getValue();

      // we only allow searching of datasets
      if (nameSpaceContainer.getType() != NameSpaceContainer.Type.DATASET) {
        return false;
      }

      DatasetConfig dataset = nameSpaceContainer.getDataset();

      // if system owned dataset, skip
      final String connectorName = input.getKey().getRoot().toLowerCase();
      // TODO(DX-15663): fix this filter
      return !"sys".equals(connectorName) &&
          !"information_schema".equals(connectorName) &&
          !connectorName.startsWith("__") &&
          (dataset.getOwner() == null || !dataset.getOwner().equals(SystemUser.SYSTEM_USERNAME));
    }).forEach(input -> {
      final NameSpaceContainer nameSpaceContainer = input.getValue();

      final DatasetConfig dataset = nameSpaceContainer.getDataset();
      final String datasetId = dataset.getId().getId();

      CollaborationTag collaborationTag = null;

      if (collaborationTagMap.containsKey(datasetId)) {
        collaborationTag = collaborationTagMap.get(datasetId);
      } else if (previousWakeupTime > 0) {
        // Check if there are tags for the container.  However, if the previous wakeup is <= 0, that means that we are
        // doing a first time index and therefore we have all possible tags stored in collaborationTagMap so we can
        // skip this lookup.
        Optional<CollaborationTag> tag = collaborationTagStore.getTagsForEntityId(datasetId);
        if (tag.isPresent()) {
          collaborationTag = tag.get();
        }
      }

      final SearchContainer searchEntity = new SearchContainer(nameSpaceContainer, collaborationTag);
      final NamespaceKey namespaceKey = new NamespaceKey(nameSpaceContainer.getFullPathList());

      searchIndex.index(NamespaceServiceImpl.getKey(namespaceKey), searchEntity);
      indexCount.getAndIncrement();
    });

    logger.debug("  Indexed {} entities modified since {}", indexCount, previousWakeupTime);
  }

  private Iterable<Map.Entry<NamespaceKey, NameSpaceContainer>> getModifiedNamespaceContainers(long previousWakeupTime, Map<String, CollaborationTag> collaborationTagMap) {
    // if this is our first time, index everything as entities created in the previous versions will have a null last
    // modified time (we don't necessarily reindex on upgrade).
    if (previousWakeupTime <= 0) {
      return namespaceService.get().find(null);
    }

    final List<SearchQuery> queries = new ArrayList<>();

    queries.add(newRangeLong(NamespaceIndexKeys.LAST_MODIFIED.getIndexFieldName(), previousWakeupTime, Long.MAX_VALUE, true, false));

    // namespace ids that have modified collaboration tags
    queries.addAll(collaborationTagMap.keySet().stream().map(
      input -> newTermQuery(DatasetIndexKeys.DATASET_UUID.getIndexFieldName(), input)
    ).collect(Collectors.toList()));

    final LegacyFindByCondition condition = new LegacyFindByCondition().setCondition(or(queries));
    return namespaceService.get().find(condition);
  }

  private Map<String, CollaborationTag> getModifiedTags(long previousWakeupTime) {
    final Map<String, CollaborationTag> collaborationTagMap = new HashMap<>();

    final SearchQuery modifiedTagsQuery = newRangeLong(CollaborationTagStore.LAST_MODIFIED.getIndexFieldName(), previousWakeupTime, Long.MAX_VALUE, true, false);
    final LegacyFindByCondition newTags = new LegacyFindByCondition().setCondition(modifiedTagsQuery);

    final Iterable<Map.Entry<String, CollaborationTag>> changedTags = collaborationTagStore.find(newTags);
    StreamSupport.stream(changedTags.spliterator(), false).forEach(input -> {
      collaborationTagMap.put(input.getKey(), input.getValue());
    });

    return collaborationTagMap;
  }

  private long getLastWakeupTime() {
    return lastWakeupTime;
  }

  private void updateLastWakeupTime(long newTime) {
    lastWakeupTime = newTime;

    setLastWakeupTime(lastWakeupTime);
  }

  /**
  * DocumentConverter for the search index
  */
  public static final class NamespaceSearchConverter implements DocumentConverter<String, SearchContainer> {
    private Integer version = 0;

    @Override
    public Integer getVersion() {
      return version;
    }

    @Override
    public void convert(DocumentWriter writer, String id, SearchContainer record) {
      final NameSpaceContainer namespaceContainer = record.getNamespaceContainer();

      final List<String> fullPathList = namespaceContainer.getFullPathList();
      writer.write(PATH_UNQUOTED_LC, fullPathList.stream()
        .map(String::toLowerCase)
        .toArray(String[]::new));
      writer.write(NAME_LC, fullPathList.get(fullPathList.size() - 1).toLowerCase());

      if (namespaceContainer.getType() == NameSpaceContainer.Type.DATASET) {
        String[] columns = NamespaceConverter.getColumnsLowerCase(namespaceContainer.getDataset());
        if (columns.length > 0) {
          writer.write(DATASET_COLUMNS_LC, columns);
        }
      }

      // check if the namespace entity has any tags and index them
      final CollaborationTag collaborationTag = record.getCollaborationTag();
      if (collaborationTag != null && collaborationTag.getTagsList() != null) {
        // store lowercase and all permutations
        writer.write(TAGS_LC, collaborationTag.getTagsList().stream()
          .map(String::toLowerCase).toArray(String[]::new));
      }
    }
  }

  SearchConfiguration getSearchConfig() {
    final ConfigurationEntry configurationEntry = configurationStore.get(CONFIG_KEY);
    final SearchConfiguration searchConfiguration = new SearchConfiguration();
    if (configurationEntry != null) {
      ProtostuffIOUtil.mergeFrom(configurationEntry.getValue().toByteArray(), searchConfiguration, SearchConfiguration.getSchema());
    } else {
      searchConfiguration.setLastWakeupTime(0L);
    }

    return searchConfiguration;
  }

  private void setLastWakeupTime(long lastWakeupTime) {
    ConfigurationEntry configurationEntry = configurationStore.get(CONFIG_KEY);
    final SearchConfiguration searchConfiguration = SearchConfiguration.getDefaultInstance();
    if (configurationEntry != null) {
      ProtostuffIOUtil.mergeFrom(configurationEntry.getValue().toByteArray(), searchConfiguration, SearchConfiguration.getSchema());
    } else {
      configurationEntry = new ConfigurationEntry();
    }

    searchConfiguration.setLastWakeupTime(lastWakeupTime);
    configurationEntry.setValue(convertSearchConfigurationToByteString(searchConfiguration));

    configurationStore.put(CONFIG_KEY, configurationEntry);
  }

  private static ByteString convertSearchConfigurationToByteString(SearchConfiguration configuration) {
    final LinkedBuffer buffer = LinkedBuffer.allocate();
    byte[] bytes = ProtostuffIOUtil.toByteArray(configuration, SearchConfiguration.getSchema(), buffer);
    return ByteString.copyFrom(bytes);
  }
}
