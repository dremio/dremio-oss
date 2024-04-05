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

import com.dremio.common.util.MajorTypeHelper;
import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.explore.DataTypeUtil;
import com.dremio.dac.service.autocomplete.model.AutocompleteResponse;
import com.dremio.dac.service.autocomplete.model.SuggestionEntity;
import com.dremio.dac.service.autocomplete.model.SuggestionEntityType;
import com.dremio.dac.service.autocomplete.model.SuggestionsType;
import com.dremio.exec.store.ReferenceInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.pojo.Field;

/** Manage Autocomplete logic for what's been returned for various types */
public final class AutocompleteHelper {
  public static AutocompleteResponse buildContainerSuggestions(
      List<CatalogItem> matchingContainers) {
    List<SuggestionEntity> suggestedContainers = new ArrayList<>();
    for (CatalogItem item : matchingContainers) {
      switch (item.getType()) {
        case CONTAINER:
          {
            suggestedContainers.add(
                new SuggestionEntity(
                    item.getPath(),
                    convertContainerTypeToSuggestionEntityType(item.getContainerType()).getType()));
            break;
          }

        case DATASET:
          {
            suggestedContainers.add(
                new SuggestionEntity(
                    item.getPath(),
                    convertDatasetTypeToSuggestionEntityType(item.getDatasetType()).getType()));
            break;
          }

        case FILE:
          {
            suggestedContainers.add(
                new SuggestionEntity(item.getPath(), SuggestionEntityType.FILE.getType()));
            break;
          }

        default:
          {
            throw new RuntimeException("Unknown type");
          }
      }
    }
    return new AutocompleteResponse(
        SuggestionsType.CONTAINER.getType(),
        suggestedContainers.size(),
        suggestedContainers.size(),
        suggestedContainers);
  }

  private static SuggestionEntityType convertContainerTypeToSuggestionEntityType(
      CatalogItem.ContainerSubType type) {
    switch (type) {
      case SPACE:
        return SuggestionEntityType.SPACE;
      case HOME:
        return SuggestionEntityType.HOME;
      case FOLDER:
        return SuggestionEntityType.FOLDER;
      case SOURCE:
        return SuggestionEntityType.SOURCE;
      case FUNCTION:
        return SuggestionEntityType.FUNCTION;
      default:
        throw new RuntimeException("Unknown Container subtype: " + type);
    }
  }

  private static SuggestionEntityType convertDatasetTypeToSuggestionEntityType(
      CatalogItem.DatasetSubType type) {
    switch (type) {
      case VIRTUAL:
        return SuggestionEntityType.VIRTUAL;
      case PROMOTED:
        // DX-63812: Can't tell if it's promoted from file or folder.
        return SuggestionEntityType.PROMOTED;
      case DIRECT:
        return SuggestionEntityType.DIRECT;
      default:
        throw new RuntimeException("Unknown Dataset subtype: " + type);
    }
  }

  public static AutocompleteResponse buildColumnSuggestions(
      Map<List<String>, List<Field>> matchingColumnsMap) {
    List<SuggestionEntity> suggestedColumns = new ArrayList<>();
    for (List<String> catalogEntityKey : matchingColumnsMap.keySet()) {
      for (Field column : matchingColumnsMap.get(catalogEntityKey)) {
        List<String> columnFullName =
            Stream.concat(catalogEntityKey.stream(), Stream.of(column.getName()))
                .collect(Collectors.toList());
        suggestedColumns.add(
            new SuggestionEntity(
                columnFullName,
                DataTypeUtil.getDataType(MajorTypeHelper.getMajorTypeForField(column)).toString()));
      }
    }
    return new AutocompleteResponse(
        SuggestionsType.COLUMN.getType(),
        suggestedColumns.size(),
        suggestedColumns.size(),
        suggestedColumns);
  }

  public static AutocompleteResponse buildReferenceSuggestions(
      List<ReferenceInfo> matchingReferences, SuggestionsType refType) {
    List<SuggestionEntity> suggestedReferences =
        matchingReferences.stream()
            .map(entity -> new SuggestionEntity(Arrays.asList(entity.refName), entity.type))
            .collect(Collectors.toList());
    return new AutocompleteResponse(
        refType.getType(),
        suggestedReferences.size(),
        suggestedReferences.size(),
        suggestedReferences);
  }
}
