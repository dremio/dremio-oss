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
package com.dremio.exec.store.dfs;

import java.util.List;

import com.dremio.common.logical.FormatPluginConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * Read entry of table on filesystem. Includes files selected, format and workspace.
 */
public class FileSystemReadEntry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemReadEntry.class);

  private final FormatPluginConfig format;
  private final FileSelection selection;

  @JsonCreator
  public FileSystemReadEntry(@JsonProperty("format") FormatPluginConfig format, @JsonProperty("files") List<String> files){
    this.format = format;
    this.selection = FileSelection.create(null, files, null);
  }

  public FileSystemReadEntry(FormatPluginConfig format, FileSelection selection) {
    super();
    this.format = format;
    this.selection = selection;
  }

  @JsonProperty("format")
  public FormatPluginConfig getFormat(){
    return format;
  }

  @JsonProperty("files")
  public List<String> getAsFiles(){
    return selection.getFiles();
  }

  @JsonIgnore
  public FileSelection getSelection(){
    return selection;
  }

  @JsonIgnore
  public boolean supportDirPruning() {
    return selection.supportDirPruning();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileSystemReadEntry that = (FileSystemReadEntry) o;
    return Objects.equal(format, that.format) &&
        Objects.equal(selection, that.selection);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(format, selection);
  }
}
