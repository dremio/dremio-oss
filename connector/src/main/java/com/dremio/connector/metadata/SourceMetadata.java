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
package com.dremio.connector.metadata;

import java.util.Optional;

import com.dremio.connector.ConnectorException;

/**
 * This is a facet of the connector that provides metadata about datasets available in the source.
 * <p>
 * The only component in core Dremio that interacts with this facet is the catalog. For details, see the
 * {@link com.dremio.exec.catalog.connector catalog-connector package}.
 * <p>
 * To fetch metadata for a dataset, the caller will get a handle to the dataset using {@link #getDatasetHandle} API (or
 * the listing interface). If further metadata is required, (a) the caller will iterate over the partition chunk
 * listing using {@link #listPartitionChunks} API providing the dataset handle from the previous call, and then,
 * (b) the caller will get the dataset metadata using the {@link #getDatasetMetadata} API providing the dataset handle
 * and the partition chunk listing from previous calls. This allows to carry forward state, which is typically required
 * to fulfill the next call; the state that is carried forward needs to have minimal memory footprint.
 */
public interface SourceMetadata {

  /**
   * Given a dataset path, return a handle that represents the dataset at the path. Returns {@link Optional#empty()}
   * if the dataset does not exist, or the entity at the given path is not a dataset.
   *
   * @param datasetPath dataset path
   * @param options     options
   * @return an optional dataset handle, not null
   */
  Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options)
      throws ConnectorException;

  /**
   * Returns a listing of partition chunk handles for the given dataset handle, where each handle represents a
   * ... . There must be one or more partition chunks in a dataset. TODO: finish the comment
   *
   * @param datasetHandle dataset handle
   * @return listing of partition chunk handles, not null
   */
  PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options)
      throws ConnectorException;

  /**
   * Given a dataset handle and a listing, return the {@link DatasetMetadata dataset metadata} for the dataset
   * represented by the handle.
   * <p>
   * Notes to implementers:
   * <ul>
   * <li>
   * Implementations are recommended to populate the returned {@code dataset metadata} on invocation
   * of this method, rather than lazily load the values in getters defined in {@link DatasetMetadata}.
   * </li>
   * <li>
   * Consider implementing {@link com.dremio.connector.metadata.extensions.SupportsReadSignature} to avoid unnecessary
   * metadata requests to the source.
   * </li>
   * </ul>
   *
   * @param datasetHandle dataset handle
   * @param chunkListing  chunk listing
   * @param options       options
   * @return dataset metadata, not null
   */
  DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle,
      PartitionChunkListing chunkListing,
      GetMetadataOption... options
  ) throws ConnectorException;

  /**
   * Check if the given path exists, and that the entity at the path is a container.
   * TODO: defined what a container is
   *
   * @param containerPath container path
   * @return true iff the given path exists and the entity at the path is a container
   */
  boolean containerExists(EntityPath containerPath);
}
