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
package com.dremio.exec.catalog;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.store.ReferenceTypeConflictException;

public interface VersionContextResolver {

  /**
   * Resolves a version context with the underlying versioned catalog server.
   *
   * @param sourceName
   * @param versionContext
   * @return ResolvedVersionedContext
   * @throws VersionNotFoundInNessieException - The version is not found in the underlying server
   * @throws ReferenceTypeConflictException - The version type requested does not match the type on
   *     the server even though the name of the Reference name is found
   * @throws org.projectnessie.error.NessieRuntimeException - Runtime exceptions thrown specifically
   *     by Nessie
   * @throws RuntimeException -For other Unchecked Exceptions from the server.
   * @throws IllegalStateException Any other unexpected exceptions (other than
   *     UncheckedExecutionException)
   */
  ResolvedVersionContext resolveVersionContext(String sourceName, VersionContext versionContext);
}
