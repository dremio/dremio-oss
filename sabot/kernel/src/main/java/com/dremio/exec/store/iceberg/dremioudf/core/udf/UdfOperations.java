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
package com.dremio.exec.store.iceberg.dremioudf.core.udf;

/** SPI interface to abstract UDF metadata access and updates. */
public interface UdfOperations {

  /**
   * Return the currently loaded UDF metadata, without checking for updates.
   *
   * @return UDF metadata
   */
  UdfMetadata current();

  /**
   * Return the current UDF metadata after checking for updates.
   *
   * @return UDF metadata
   */
  UdfMetadata refresh();

  /**
   * Replace the base UDF metadata with a new version.
   *
   * <p>This method should implement and document atomicity guarantees.
   *
   * <p>Implementations must check that the base metadata is current to avoid overwriting updates.
   * Once the atomic commit operation succeeds, implementations must not perform any operations that
   * may fail because failure in this method cannot be distinguished from commit failure.
   *
   * <p>Implementations should throw a {@link
   * org.apache.iceberg.exceptions.CommitStateUnknownException} in cases where it cannot be
   * determined if the commit succeeded or failed. For example if a network partition causes the
   * confirmation of the commit to be lost, the implementation should throw a
   * CommitStateUnknownException. An unknown state indicates to downstream users of this API that it
   * is not safe to perform clean up and remove any files. In general, strict metadata cleanup will
   * only trigger cleanups when the commit fails with an exception implementing the marker interface
   * {@link org.apache.iceberg.exceptions.CleanableFailure}. All other exceptions will be treated as
   * if the commit has failed.
   *
   * @param base UDF metadata on which changes were based
   * @param metadata new UDF metadata with updates
   */
  void commit(UdfMetadata base, UdfMetadata metadata);
}
