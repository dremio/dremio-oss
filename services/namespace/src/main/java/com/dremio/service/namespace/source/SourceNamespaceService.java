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
package com.dremio.service.namespace.source;

import java.util.ConcurrentModificationException;
import java.util.List;

import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;

/**
 * Namespace operations for Sources.
 */
public interface SourceNamespaceService {
  //// CREATE or UPDATE
  void addOrUpdateSource(NamespaceKey sourcePath, SourceConfig sourceConfig, NamespaceAttribute... attributes) throws NamespaceException;

  //// READ
  SourceConfig getSource(NamespaceKey sourcePath) throws NamespaceException;
  SourceConfig getSourceById(String id) throws NamespaceException;
  List<SourceConfig> getSources();

  //// DELETE
  @FunctionalInterface
  interface DeleteCallback {
    void onDatasetDelete(DatasetConfig datasetConfig);
  }
  /**
   * Delete all of a sources children but leave the source intact.
   * @param sourcePath
   * @param version
   * @param callback
   * @throws NamespaceException
   */
  void deleteSourceChildren(final NamespaceKey sourcePath, String version, DeleteCallback callback) throws NamespaceException;

  /**
   * Delete a source and all of its children.
   * @param sourcePath
   * @param version
   * @throws NamespaceException
   */
  void deleteSourceWithCallBack(NamespaceKey sourcePath, String version, DeleteCallback callback) throws NamespaceException;

  void deleteSource(NamespaceKey sourcePath, String version) throws NamespaceException;

  //// OTHER
  /**
   * Checks if a sourceConfig can be saved.  Currently does concurrency checks for the config and any passed in attributes
   *
   * @param newConfig the new config we want to save
   * @param attributes
   */
  void canSourceConfigBeSaved(SourceConfig newConfig, SourceConfig existingConfig, NamespaceAttribute... attributes) throws ConcurrentModificationException, NamespaceException;
}
