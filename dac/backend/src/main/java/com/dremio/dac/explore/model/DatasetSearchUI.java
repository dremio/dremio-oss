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
package com.dremio.dac.explore.model;

import static com.dremio.common.utils.PathUtils.encodeURIComponent;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

/**
 * Dataset search result model encapsulating physical and virtual datasets.
 */
@JsonIgnoreProperties(value={"apiLinks", "links"}, allowGetters=true)
public class DatasetSearchUI {
  private final String id;
  private final List<String> fullPath;
  private final List<String> displayFullPath;
  private final List<String> context;
  private final DatasetType datasetType;

  // matching parents
  private final List<ParentDataset> parents;

  // matching fields
  private final List<DatasetFieldSearchUI> fields;

  // Dataset tags
  private List<String> tags;

  @JsonIgnore
  private final DatasetVersion datasetVersion;

  public DatasetSearchUI(DatasetConfig datasetConfig, CollaborationTag collaborationTag) {
    this.fullPath = datasetConfig.getFullPathList();
    this.datasetType = datasetConfig.getType();
    this.id = datasetConfig.getId().getId();
    if (collaborationTag != null) {
      this.tags = collaborationTag.getTagsList();
    }
    if (datasetType == DatasetType.VIRTUAL_DATASET) {
      final VirtualDataset virtualDataset = datasetConfig.getVirtualDataset();
      this.parents = virtualDataset.getParentsList();
      this.displayFullPath = fullPath;
      this.context = virtualDataset.getContextList();
      this.fields = Lists.newArrayList();
      for (ViewFieldType field: virtualDataset.getSqlFieldsList()) {
        fields.add(new DatasetFieldSearchUI(field.getName(), field.getType()));
      }
      this.datasetVersion = virtualDataset.getVersion();
    } else {
      this.displayFullPath = fullPath;
      this.context = null;
      this.parents = null;
      this.fields = null;
      this.datasetVersion = null;
    }
  }

  @JsonCreator
  public DatasetSearchUI(@JsonProperty("id") String id,
                         @JsonProperty("fullPath") List<String> fullPath,
                         @JsonProperty("displayFullPath") List<String> displayFullPath,
                         @JsonProperty("context") List<String> context,
                         @JsonProperty("parents") List<ParentDataset> parents,
                         @JsonProperty("fields") List<DatasetFieldSearchUI> fields,
                         @JsonProperty("datasetType") DatasetType datasetType) {
    this.id = id;
    this.fullPath = fullPath;
    this.displayFullPath = displayFullPath;
    this.context = context;
    this.parents = parents;
    this.fields = fields;
    this.datasetType = datasetType;
    this.datasetVersion = null;
  }

  public String getId() {
    return id;
  }

  public List<String> getFullPath() {
    return fullPath;
  }

  public List<String> getDisplayFullPath() {
    return displayFullPath;
  }

  public List<String> getContext() {
    return context;
  }

  public List<ParentDataset> getParents() {
    return parents;
  }

  public List<DatasetFieldSearchUI> getFields() {
    return fields;
  }

  public DatasetType getDatasetType() {
    return datasetType;
  }

  public List<String> getTags() {
    return tags;
  }

  /**
   * Matched field of a dataset during search.
   */
  public static class DatasetFieldSearchUI {
    private final String name;
    private final String type;

    @JsonCreator
    public DatasetFieldSearchUI(@JsonProperty("name") String name, @JsonProperty("type") String type) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }
  }

  public Map<String, String> getLinks() {
    DatasetPath datasetPath = new DatasetPath(fullPath);
    final Map<String, String> links = new HashMap<String, String>();
    links.put("self", datasetPath.getQueryUrlPath());

    // TODO: Do we need to send browser link for run/new untitled?
    if (datasetType == DatasetType.VIRTUAL_DATASET) {
      links.put("edit", datasetPath.getQueryUrlPath() + "?mode=edit&version="
        + (datasetVersion == null ? datasetVersion : encodeURIComponent(datasetVersion.toString())));
    }
    return links;
  }

  public Map<String, String> getApiLinks() throws UnsupportedEncodingException {
    final NamespaceKey datasetPath = new NamespaceKey(fullPath);
    final String dottedFullPath = datasetPath.toUrlEncodedString();
    final String fullPathString = PathUtils.toFSPath(fullPath).toString();

    Map<String, String> links = new HashMap<String, String>();
    switch (datasetType) {
      case VIRTUAL_DATASET:
        links.put("edit", "/dataset/" + dottedFullPath + "/version/" + datasetVersion + "?view=explore"); //edit dataset
        final DatasetVersion datasetVersion = DatasetVersion.newVersion();
        links.put("run", "/datasets/new_untitled?parentDataset=" + dottedFullPath + "&newVersion="
          + (datasetVersion == null ? datasetVersion : encodeURIComponent(datasetVersion.toString()))); //create new dataset
        break;
      case PHYSICAL_DATASET_HOME_FILE:
        links.put("run", "/home/" + fullPath.get(0) + "new_untitled_from_file" + fullPathString);
        break;
      case PHYSICAL_DATASET_HOME_FOLDER:
        // Folder not supported yet
        break;
      case PHYSICAL_DATASET_SOURCE_FILE:
        links.put("run", "/source/" + fullPath.get(0) + "new_untitled_from_file" + fullPathString);
        break;
      case PHYSICAL_DATASET_SOURCE_FOLDER:
        links.put("run", "/source/" + fullPath.get(0) + "new_untitled_from_folder" + fullPathString);
        break;
      case PHYSICAL_DATASET:
        links.put("run", "/source/" + fullPath.get(0) + "new_untitled_from_physical_dataset" + fullPathString);
        break;
      default:
        break;
    }
    return links;
  }

}
