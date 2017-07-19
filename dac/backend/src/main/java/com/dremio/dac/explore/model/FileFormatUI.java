/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.HashMap;
import java.util.Map;

import com.dremio.dac.model.common.NamespacePath;
import com.dremio.service.namespace.file.FileFormat;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * FormatFormat plus id and links for ui
 */
@JsonIgnoreProperties(value={ "id" }, allowGetters=true)
public class FileFormatUI{

  private final FileFormat fileFormat;
  private final Map<String, String> links;

  @JsonIgnore
  private static final String FILE_FORMAT = "file_format";

  @JsonIgnore
  private static final String FOLDER_FORMAT = "folder_format";

  public FileFormatUI(FileFormat fileFormat, NamespacePath namespacePath) {
    this.links = new HashMap<>();
    links.put("self", namespacePath.toUrlPathWithAction(fileFormat.getIsFolder()? FOLDER_FORMAT: FILE_FORMAT));
    links.put("format_preview", namespacePath.toUrlPathWithAction(fileFormat.getIsFolder() ? "folder_preview" : "file_preview"));
    this.fileFormat = fileFormat;
  }

  @JsonCreator
  public FileFormatUI(
    @JsonProperty("fileFormat") FileFormat fileFormat,
    @JsonProperty("links") Map<String, String> links) {
    this.fileFormat = fileFormat;
    this.links = links;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  @JsonGetter
  String getId() {
    return this.links.get("self");
  }

  public Map<String, String> getLinks() {
    return this.links;
  }

}
