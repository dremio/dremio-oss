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

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

public class VersionContextResolverImpl implements VersionContextResolver {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VersionContextResolverImpl.class);

  private final PluginRetriever pluginRetriever;

  private final LoadingCache<ImmutablePair<String, VersionContext>, ResolvedVersionContext> sourceVersionMappingCache =
      CacheBuilder.newBuilder()
          .build(new Loader());

  VersionContextResolverImpl(PluginRetriever pluginRetriever) {
    this.pluginRetriever = pluginRetriever;
  }

  @Override
  public ResolvedVersionContext resolveVersionContext(
      String sourceName,
      VersionContext versionContext
  ) throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException {
    try {
      ResolvedVersionContext resolvedVersionContext = sourceVersionMappingCache.get(new ImmutablePair<>(sourceName, versionContext));
      logger.debug("Resolved version {} to resolved {} for source {}",
        versionContext,
        resolvedVersionContext,
        sourceName);
      return resolvedVersionContext;
    } catch (UncheckedExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ReferenceNotFoundException) {
        throw (ReferenceNotFoundException) cause;
      }
      if (cause instanceof NoDefaultBranchException) {
        throw (NoDefaultBranchException) cause;
      }
      if (cause instanceof ReferenceConflictException) {
        throw (ReferenceConflictException) cause;
      }
      throw new IllegalStateException(e);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public void invalidateVersionContext(String sourceName, VersionContext versionContext) {
    ImmutablePair<String, VersionContext> sourceVersion = new ImmutablePair<>(sourceName, versionContext);
    sourceVersionMappingCache.invalidate(sourceVersion);
  }

  private class Loader extends CacheLoader<ImmutablePair<String, VersionContext>, ResolvedVersionContext> {
    @NotNull
    @Override
    public ResolvedVersionContext load(ImmutablePair<String, VersionContext> key) {
      String sourceName = key.left;
      VersionContext versionContext = key.right;
      ManagedStoragePlugin msp = pluginRetriever.getPlugin(sourceName, false);
      if (msp != null) {
        StoragePlugin source = msp.getPlugin();
        if (source instanceof VersionedPlugin) {
          ResolvedVersionContext resolvedVersionContext = ((VersionedPlugin) source).resolveVersionContext(versionContext);
          logger.debug("Calling Nessie to resolve version {} for source {} ", versionContext, sourceName);
          return resolvedVersionContext;
        }
      }

      throw UserException.unsupportedError()
          .message("Source %s does not support versioning.", sourceName)
          .buildSilently();
    }
  }
}
