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
package com.dremio.file;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dremio.dac.explore.model.FileFormatUI;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.namespace.file.FileFormat;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

/**
 * File model.
 */
@JsonIgnoreProperties(value={"name", "links", "filePath"}, allowGetters=true)
public class File {

  private final NamespacePath filePath;

  private final FileFormatUI fileFormat;

  private final boolean queryable;
  private final String id;
  private final Integer jobCount;
  private final Integer descendants;
  private final boolean isStaged;
  private final boolean isHomeFile;

  @JsonCreator
  public File(
    @JsonProperty("id") String id,
    @JsonProperty("urlPath") String urlPath,
    @JsonProperty("fileFormat") FileFormatUI fileFormat,
    @JsonProperty("jobCount") Integer jobCount,
    @JsonProperty("descendants") Integer descendants,
    @JsonProperty("isStaged") boolean isStaged,
    @JsonProperty("isHomeFile") boolean isHomeFile,
    @JsonProperty("queryable") boolean queryable) {
    this.id = id;
    this.fileFormat = fileFormat;
    this.filePath = File.parseUrlPath(urlPath);
    this.jobCount = jobCount;
    this.descendants = descendants;
    this.isStaged = isStaged;
    this.isHomeFile = isHomeFile;
    this.queryable = queryable;
  }

  public static File newInstance(String id, NamespacePath filePath, FileFormat fileFormat, Integer jobCount, Integer descendants,
      boolean isStaged, boolean isHomeFile, boolean isQueryable) {
    return new File(id, filePath.toUrlPath(), new FileFormatUI(fileFormat, filePath), jobCount, descendants, isStaged, isHomeFile, isQueryable);
  }

  public boolean isQueryable() {
    return queryable;
  }

  public String getId() {
    return id;
  }

  public String getUrlPath() {
    return filePath.toUrlPath();
  }

  public String getName() {
    return filePath.getLeaf().getName();
  }

  public NamespacePath getFilePath() {
    return filePath;
  }

  public FileFormatUI getFileFormat() {
    return fileFormat;
  }

  public Integer getJobCount() {
    return jobCount;
  }

  public Integer getDescendants() {
    return descendants;
  }

  public boolean getIsHomeFile() {
    return isHomeFile;
  }

  public Map<String, String> getLinks() {
    Map<String, String> links = new HashMap<>();
    links.put("self", filePath.toUrlPath());
    final JobFilters jobFilters = new JobFilters()
      .addFilter(JobIndexKeys.ALL_DATASETS, filePath.toString())
      .addFilter(JobIndexKeys.QUERY_TYPE, JobIndexKeys.UI, JobIndexKeys.EXTERNAL);
    links.put("jobs", jobFilters.toUrl());
    links.put("format", filePath.toUrlPathWithAction("file_format"));
    if (isStaged) {
      links.put("format_preview", filePath.toUrlPathWithAction("file_preview_unsaved"));
      links.put("upload_finish", filePath.toUrlPathWithAction("upload_finish"));
      links.put("upload_cancel", filePath.toUrlPathWithAction("upload_cancel"));
    } else {
      links.put("format_preview", filePath.toUrlPathWithAction("file_preview"));
      if (!isHomeFile) {
        links.put("delete_format", filePath.toUrlPathWithAction("file_format") + "?version=" + fileFormat.getFileFormat().getVersion());
      }
    }
    // always include query url because set file format response doesn't include it.
    links.put("query", filePath.getQueryUrlPath());
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

  private static NamespacePath parseUrlPath(String urlPath) {
    Matcher m = PARSER.matcher(urlPath);
    if (m.matches()) {
      List<String> pathParts = FluentIterable
          .of(new String[] { m.group(2)} )
          .append(m.group(3).split("/"))
          .transform(PATH_DECODER)
          .toList();

      if (m.group(1).equals("home")) {
        return new FilePath(pathParts);
      } else {
        return new SourceFilePath(pathParts);
      }
    }
    throw new IllegalArgumentException("Not a valid filePath: " + urlPath);
  }
}
