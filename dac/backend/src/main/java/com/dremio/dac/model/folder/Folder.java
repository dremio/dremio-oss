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
package com.dremio.dac.model.folder;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.common.RootEntity;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.space.proto.ExtendedConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

/**
 * Folder model.
 */
@JsonIgnoreProperties(value={"fullPathList", "links" }, allowGetters=true)
public class Folder {

  private final String id;
  private final String name;
  private final Boolean physicalDataset;
  private final ExtendedConfig extendedConfig;
  private final String version;

  private final NamespacePath folderPath;

  private final boolean fileSystemFolder;
  private final boolean queryable;

  private final FileFormat fileFormat;

  private final NamespaceTree contents;

  private final List<String> tags;

  @JsonCreator
  public Folder(
    @JsonProperty("id") String id,
    @JsonProperty("name") String name,
    @JsonProperty("urlPath") String urlPath,
    @JsonProperty("isPhysicalDataset") Boolean isPhysicalDataset,
    @JsonProperty("isFileSystemFolder") boolean isFileSystemFolder,
    @JsonProperty("isQueryable") boolean isQueryable,
    @JsonProperty("extendedConfig") ExtendedConfig extendedConfig,
    @JsonProperty("version") String version,
    @JsonProperty("fileformat") FileFormat fileFormat,
    @JsonProperty("contents") NamespaceTree contents,
    @JsonProperty("tags") List<String> tags) {
    this.id = id;
    this.folderPath = Folder.parseUrlPath(urlPath);
    this.name = name;
    this.physicalDataset = isPhysicalDataset;
    this.fileSystemFolder = isFileSystemFolder;
    this.queryable = isQueryable;
    this.extendedConfig = extendedConfig;
    this.version = version;
    this.fileFormat = fileFormat;
    this.contents = contents;
    this.tags = tags;
  }

  public String getId() {
    return id;
  }

  public NamespaceTree getContents() {
    return contents;
  }

  public boolean isQueryable() {
    return queryable;
  }

  public boolean isFileSystemFolder() {
    return fileSystemFolder;
  }

  public String getUrlPath() {
    return folderPath.toUrlPath();
  }

  public List<String> getFullPathList() {
    return folderPath.toPathList();
  }

  public String getName() {
    return name;
  }

  public Boolean getIsPhysicalDataset() {
    return physicalDataset;
  }

  public ExtendedConfig getExtendedConfig() {
    return extendedConfig;
  }

  public String getVersion() {
    return version;
  }

  public List<String> getTags() {
    return tags;
  }

  public Map<String, String> getLinks() {
    Map<String, String> links = new HashMap<>();
    links.put("self", folderPath.toUrlPath());

    // always include query url because set file format response doesn't include it.
    links.put("query", folderPath.getQueryUrlPath());

    if (fileSystemFolder) {
      links.put("format", folderPath.toUrlPathWithAction("folder_format"));
      links.put("format_preview", folderPath.toUrlPathWithAction("folder_preview"));
      if (queryable && fileFormat != null && fileFormat.getVersion() != null) {
        links.put(
          "delete_format",
          folderPath.toUrlPathWithAction("folder_format") + "?version=" + fileFormat.getVersion()
        );
        // overwrite jobs link since this folder is queryable
        final JobFilters jobFilters = new JobFilters()
          .addFilter(JobIndexKeys.ALL_DATASETS, folderPath.toString())
          .addFilter(JobIndexKeys.QUERY_TYPE, JobIndexKeys.UI, JobIndexKeys.EXTERNAL);
        links.put("jobs", jobFilters.toUrl());
      }
    } else {
      if (folderPath.getRoot().getRootType() == RootEntity.RootType.HOME) {
        links.put("upload_start", folderPath.toUrlPathWithAction("upload_start"));
        // for getting format of children
        links.put("file_format", folderPath.toUrlPathWithAction("folder_format"));
        links.put("file_prefix", folderPath.toUrlPathWithAction("file"));
      }
      // renames not allowed on source folders
      links.put("rename", folderPath.toUrlPathWithAction("rename_folder"));
    }
    // add jobs if not already added.
    if (!links.containsKey("jobs")) {
      final JobFilters jobFilters = new JobFilters()
        .addContainsFilter(folderPath.toNamespaceKey().toString())
        .addFilter(JobIndexKeys.QUERY_TYPE, "UI", "EXTERNAL");
      links.put("jobs", jobFilters.toUrl());
    }
    return links;
  }

  private static final Pattern PARSER = Pattern.compile("/([^/]+)/([^/]+)/[^/]+/(.*)");
  private static final Function<String, String> PATH_DECODER = new Function<String, String>() {
    @Override
    public String apply(String input) {
      try {
        return URLDecoder.decode(input, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new UnsupportedOperationException(e);
      }
    }
  };

  static NamespacePath parseUrlPath(String urlPath) {
    Matcher m = PARSER.matcher(urlPath);
    if (m.matches()) {
      List<String> pathParts = FluentIterable
          .of(new String[] { m.group(2)} )
          .append(m.group(3).split("/"))
          .transform(PATH_DECODER)
          .toList();

      if (m.group(1).equals("source")) {
        return new SourceFolderPath(pathParts);
      } else {
        return new FolderPath(pathParts);
      }
    }
    throw new IllegalArgumentException("Not a valid filePath: " + urlPath);
  }

  public static Folder newInstance(FolderPath folderPath, FolderConfig folderConfig, NamespaceTree contents, boolean isQueryable, boolean isFileSystemFolder) {
    return newInstance(folderPath, folderConfig, null, contents, isQueryable, isFileSystemFolder, null);
  }

  public static Folder newInstance(SourceFolderPath folderPath, FolderConfig folderConfig, FileFormat fileFormat, NamespaceTree contents, boolean isQueryable, boolean isFileSystemFolder) {
    return newInstance((NamespacePath) folderPath, folderConfig, fileFormat, contents, isQueryable, isFileSystemFolder, null);
  }

  protected static Folder newInstance(NamespacePath folderPath, FolderConfig folderConfig, FileFormat fileFormat, NamespaceTree contents, boolean isQueryable, boolean isFileSystemFolder, List<String> tags) {
    String id = folderConfig.getId() == null ? folderPath.toUrlPath() : folderConfig.getId().getId();
    return new Folder(id, folderConfig.getName(), folderPath.toUrlPath(), folderConfig.getIsPhysicalDataset(),
        isFileSystemFolder, isQueryable, folderConfig.getExtendedConfig(), folderConfig.getTag(), fileFormat, contents, tags);
  }
}
