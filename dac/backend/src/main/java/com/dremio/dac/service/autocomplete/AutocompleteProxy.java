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
package com.dremio.dac.service.autocomplete;

import com.dremio.dac.api.CatalogEntity;
import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.api.CatalogPageToken;
import com.dremio.dac.api.Dataset;
import com.dremio.dac.api.Source;
import com.dremio.dac.daemon.DACDaemonModule;
import com.dremio.dac.service.autocomplete.model.AutocompleteRequest;
import com.dremio.dac.service.autocomplete.model.AutocompleteResponse;
import com.dremio.dac.service.autocomplete.model.SuggestionsType;
import com.dremio.dac.service.catalog.CatalogListingResult;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.errors.SourceNotFoundException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ischema.InfoSchemaStoragePlugin;
import com.dremio.exec.store.sys.SystemStoragePlugin;
import com.dremio.service.namespace.NamespaceException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;

/** Manage to get the catalog entities based on the namespace key(s) and prefix */
public final class AutocompleteProxy {
  private static final List<String> SYSTEM_SOURCES =
      Arrays.asList(
          InfoSchemaStoragePlugin.NAME,
          SystemStoragePlugin.NAME,
          DACDaemonModule.SCRATCH_STORAGEPLUGIN_NAME);

  public static AutocompleteResponse getSuggestions(
      CatalogServiceHelper catalogServiceHelper, AutocompleteRequest request) {
    Preconditions.checkArgument(
        request.getCatalogEntityKeys() != null,
        "CatalogEntity keys can be empty but must be provided");

    switch (request.getType()) {
      case CONTAINER:
        {
          return getContainerSuggestions(
              catalogServiceHelper,
              request.getCatalogEntityKeys(),
              request.getQueryContext(),
              request.getPrefix(),
              request.getRefType(),
              request.getRefValue());
        }

      case COLUMN:
        {
          return getColumnSuggestions(
              catalogServiceHelper,
              request.getCatalogEntityKeys(),
              request.getQueryContext(),
              request.getPrefix(),
              request.getRefType(),
              request.getRefValue());
        }

      case REFERENCE:
      case BRANCH:
      case TAG:
        {
          return getReferenceSuggestions(
              catalogServiceHelper,
              request.getCatalogEntityKeys(),
              request.getPrefix(),
              request.getType());
        }

      default:
        throw new RuntimeException("Unknown autocomplete type " + request.getType());
    }
  }

  private static AutocompleteResponse getContainerSuggestions(
      CatalogServiceHelper catalogServiceHelper,
      List<List<String>> catalogEntityKeys,
      List<String> queryContext,
      String prefix,
      String refType,
      String refValue) {
    Preconditions.checkArgument(
        catalogEntityKeys.size() == 1,
        "Must have one and only one path to follow from the top level.");

    final List<CatalogItem> matchingContainers =
        getMatchingContainers(
            catalogServiceHelper,
            catalogEntityKeys.get(0),
            queryContext,
            prefix,
            refType,
            refValue);
    return AutocompleteHelper.buildContainerSuggestions(matchingContainers);
  }

  private static AutocompleteResponse getColumnSuggestions(
      CatalogServiceHelper catalogServiceHelper,
      List<List<String>> catalogEntityKeys,
      List<String> queryContext,
      String prefix,
      String refType,
      String refValue) {
    // Assuming each CatalogEntityKey is the full path to a table/view
    Preconditions.checkArgument(catalogEntityKeys.size() >= 1);
    catalogEntityKeys.forEach(
        path -> Preconditions.checkArgument(!path.isEmpty(), "Table context can't be empty."));

    final Map<List<String>, List<Field>> matchingColumnsMap =
        getMatchingColumnsMap(
            catalogServiceHelper, catalogEntityKeys, queryContext, prefix, refType, refValue);
    return AutocompleteHelper.buildColumnSuggestions(matchingColumnsMap);
  }

  private static AutocompleteResponse getReferenceSuggestions(
      CatalogServiceHelper catalogServiceHelper,
      List<List<String>> catalogEntityKeys,
      String prefix,
      SuggestionsType refType) {
    Preconditions.checkArgument(
        catalogEntityKeys.size() == 1
            && catalogEntityKeys.get(0).size() >= 1
            && !catalogEntityKeys.get(0).get(0).isEmpty(),
        "Must have a source name to start with.");

    final List<ReferenceInfo> matchingReferences =
        getMatchingReferences(
            catalogServiceHelper, catalogEntityKeys.get(0).get(0), prefix, refType);
    return AutocompleteHelper.buildReferenceSuggestions(matchingReferences, refType);
  }

  private static List<CatalogItem> getMatchingContainers(
      CatalogServiceHelper catalogServiceHelper,
      List<String> catalogEntityKey,
      List<String> queryContext,
      String prefix,
      String refType,
      String refValue) {
    // At each level, there can't be any two entities with the same name.
    // For instance, at the top level if you have a source named s3, you can't create a space named
    // s3 vice versa.
    // Query context can be considered as a special path to follow. So at most we have two paths to
    // follow.
    List<CatalogItem> matchingContainers =
        getMatchingContainersFromPath(
            catalogServiceHelper, catalogEntityKey, prefix, refType, refValue);

    // Using queryContext is null or empty as a hint whether we have to explore containers in that
    // path.
    // When it's not null nor empty, it's assuming that the queryContext is a list of non-empty
    // strings.
    if (queryContext != null && !queryContext.isEmpty()) {
      List<String> catalogEntityKeyWithQueryContext =
          Stream.concat(queryContext.stream(), catalogEntityKey.stream())
              .collect(Collectors.toList());
      matchingContainers.addAll(
          getMatchingContainersFromPath(
              catalogServiceHelper, catalogEntityKeyWithQueryContext, prefix, refType, refValue));
    }

    return matchingContainers;
  }

  private static List<CatalogItem> getMatchingContainersFromPath(
      CatalogServiceHelper catalogServiceHelper,
      List<String> catalogEntityKey,
      String prefix,
      String refType,
      String refValue) {
    List<CatalogItem> matchingContainers;
    if (catalogEntityKey.isEmpty()) {
      matchingContainers = catalogServiceHelper.getTopLevelCatalogItems(Collections.EMPTY_LIST);
      addSystemSources(matchingContainers, catalogServiceHelper);
    } else {
      try {
        matchingContainers = new ArrayList<>();
        CatalogPageToken pageToken = null;
        do {
          CatalogListingResult listingResult =
              catalogServiceHelper.getCatalogChildrenForPath(
                  catalogEntityKey, refType, refValue, pageToken, 1000);
          matchingContainers.addAll(listingResult.children());
          pageToken = listingResult.nextPageToken().orElse(null);
        } while (pageToken != null);
      } catch (AccessControlException ignored) {
        matchingContainers = Collections.EMPTY_LIST;
      }
    }

    return matchingContainers.stream()
        .filter(
            (item) -> {
              String itemName = Iterables.getLast(item.getPath(), null);
              if (itemName == null) {
                return false;
              } else if (prefix == null) {
                return true;
              } else {
                return StringUtils.startsWithIgnoreCase(itemName, prefix);
              }
            })
        .collect(Collectors.toList());
  }

  private static void addSystemSources(
      List<CatalogItem> matchingContainers, CatalogServiceHelper catalogServiceHelper) {
    for (String sourceName : SYSTEM_SOURCES) {
      try {
        // Would return null if the user has no permission
        final Optional<CatalogEntity> source =
            catalogServiceHelper.getCatalogEntityByPath(
                Collections.singletonList(sourceName),
                Collections.EMPTY_LIST,
                Collections.singletonList("children"));
        if (source.isPresent() && source.get() instanceof Source) {
          CatalogItem item = CatalogItem.fromSource((Source) source.get());
          if (item != null) {
            matchingContainers.add(item);
          }
        }
      } catch (NamespaceException ignored) {
      }
    }
  }

  private static Map<List<String>, List<Field>> getMatchingColumnsMap(
      CatalogServiceHelper catalogServiceHelper,
      List<List<String>> catalogEntityKeys,
      List<String> queryContext,
      String prefix,
      String refType,
      String refValue) {
    // Dedupe the incoming catalogEntityKeys first to avoid duplicated work
    List<List<String>> dedupedCatalogEntityKeys =
        catalogEntityKeys.stream().distinct().collect(Collectors.toList());
    Map<List<String>, List<Field>> matchingColumnsMap = new HashMap<>();
    for (List<String> catalogEntityKey : dedupedCatalogEntityKeys) {
      // The implementation here is to match the query planning implementation which will search the
      // entity key IN the
      // query context. If it can't find anything, it will use the entity key as the full path.
      // Note: If that logic changed, the logic here needs to be adjusted accordingly.
      if (queryContext != null && !queryContext.isEmpty()) {
        List<String> catalogEntityKeyWithQueryContext =
            Stream.concat(queryContext.stream(), catalogEntityKey.stream())
                .collect(Collectors.toList());
        List<Field> matchedColumns =
            getMatchingColumnsFromPath(
                catalogServiceHelper, catalogEntityKeyWithQueryContext, prefix, refType, refValue);
        if (matchedColumns != null) {
          matchingColumnsMap.put(catalogEntityKeyWithQueryContext, matchedColumns);
          continue;
        }
      }
      List<Field> matchedColumns =
          getMatchingColumnsFromPath(
              catalogServiceHelper, catalogEntityKey, prefix, refType, refValue);
      if (matchedColumns != null) {
        matchingColumnsMap.put(catalogEntityKey, matchedColumns);
      }
    }
    return matchingColumnsMap;
  }

  private static List<Field> getMatchingColumnsFromPath(
      CatalogServiceHelper catalogServiceHelper,
      List<String> catalogEntityKey,
      String prefix,
      String refType,
      String refValue) {
    try {
      Optional<CatalogEntity> entity =
          catalogServiceHelper.getCatalogEntityByPath(
              catalogEntityKey,
              Collections.EMPTY_LIST,
              Collections.EMPTY_LIST,
              refType,
              refValue,
              null,
              null);
      if (entity.isPresent() && entity.get() instanceof Dataset) {
        List<Field> matchingColumns = ((Dataset) entity.get()).getFields();
        return matchingColumns.stream()
            .filter(
                (column) -> {
                  String columnName = column.getName();
                  if (columnName == null) {
                    return false;
                  } else if (prefix == null) {
                    return true;
                  } else {
                    return StringUtils.startsWithIgnoreCase(columnName, prefix);
                  }
                })
            .collect(Collectors.toList());
      }
      return null;
    } catch (NamespaceException | RuntimeException ignored) {
      return null;
    }
  }

  private static List<ReferenceInfo> getMatchingReferences(
      CatalogServiceHelper catalogServiceHelper,
      String sourceName,
      String prefix,
      SuggestionsType refType) {
    try {
      Stream<ReferenceInfo> matchingReferences =
          catalogServiceHelper.getReferencesForVersionedSource(sourceName, refType);
      return matchingReferences
          .filter(
              (reference) -> {
                String refName = reference.refName;
                if (refName == null) {
                  return false;
                } else if (prefix == null) {
                  return true;
                } else {
                  return StringUtils.startsWithIgnoreCase(refName, prefix);
                }
              })
          .collect(Collectors.toList());
    } catch (SourceNotFoundException ignored) {
    }
    return Collections.EMPTY_LIST;
  }
}
