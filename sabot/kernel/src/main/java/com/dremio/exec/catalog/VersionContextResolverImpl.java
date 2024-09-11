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
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceNotFoundByTimestampException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.exec.store.StoragePlugin;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.projectnessie.error.NessieRuntimeException;

public class VersionContextResolverImpl implements VersionContextResolver {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(VersionContextResolverImpl.class);

  private final PluginRetriever pluginRetriever;

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  private final LoadingCache<ImmutablePair<String, VersionContext>, ResolvedVersionContext>
      sourceVersionMappingCache = CacheBuilder.newBuilder().build(new Loader());

  VersionContextResolverImpl(PluginRetriever pluginRetriever) {
    this.pluginRetriever = pluginRetriever;
  }

  @Override
  public ResolvedVersionContext resolveVersionContext(
      String sourceName, VersionContext versionContext) {
    try {
      ResolvedVersionContext resolvedVersionContext =
          sourceVersionMappingCache.get(new ImmutablePair<>(sourceName, versionContext));
      logger.debug(
          "Resolved version {} to resolved {} for source {}",
          versionContext,
          resolvedVersionContext,
          sourceName);
      return resolvedVersionContext;
    } catch (UncheckedExecutionException e) {
      if (e.getCause() instanceof NessieRuntimeException) {
        throw (NessieRuntimeException) e.getCause();
      }
      if ((e.getCause() instanceof NoDefaultBranchException)
          || (e.getCause() instanceof ReferenceNotFoundException)
          || (e.getCause() instanceof ReferenceNotFoundByTimestampException)) {
        throw new VersionNotFoundInNessieException("Reference not found in Nessie", e.getCause());
      }
      if (e.getCause() instanceof ReferenceTypeConflictException) {
        throw (ReferenceTypeConflictException) e.getCause();
      }
      throw (RuntimeException) e.getCause();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to resolve version: " + versionContext + "for: " + sourceName, e);
    }
  }

  public void invalidateVersionContext(String sourceName, VersionContext versionContext) {
    ImmutablePair<String, VersionContext> sourceVersion =
        new ImmutablePair<>(sourceName, versionContext);
    sourceVersionMappingCache.invalidate(sourceVersion);
  }

  private class Loader
      extends CacheLoader<ImmutablePair<String, VersionContext>, ResolvedVersionContext> {
    @NotNull
    @Override
    public ResolvedVersionContext load(ImmutablePair<String, VersionContext> key) {
      String sourceName = key.left;
      VersionContext versionContext = key.right;
      ManagedStoragePlugin msp = pluginRetriever.getPlugin(sourceName, false);
      if (msp != null) {
        StoragePlugin source = msp.getPlugin();
        if (source.isWrapperFor(VersionedPlugin.class)) {
          ResolvedVersionContext resolvedVersionContext =
              source.unwrap(VersionedPlugin.class).resolveVersionContext(versionContext);
          logger.debug(
              "Calling Nessie to resolve version {} for source {} ", versionContext, sourceName);
          return resolvedVersionContext;
        }
      }

      throw UserException.unsupportedError()
          .message("Source %s does not support versioning.", sourceName)
          .buildSilently();
    }
  }
}
