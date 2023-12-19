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

package com.dremio.exec.store.dfs.easy;

import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.store.dfs.implicit.PartitionImplicitColumnValuesProvider;
import com.dremio.io.file.Path;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

public class EasyImplicitColumnValuesProvider extends PartitionImplicitColumnValuesProvider {

  private final String basePath;

  public EasyImplicitColumnValuesProvider(String base) {
    basePath = base;
  }

  @Override
  public List<NameValuePair<?>> getImplicitColumnValues(BufferAllocator allocator, SplitAndPartitionInfo splitInfo,
      Map<String, Field> implicitColumns, OptionResolver options) {
    List<NameValuePair<?>> nameValuePairs = super.getImplicitColumnValues(allocator, splitInfo, implicitColumns,
        options);

    EasyProtobuf.EasyDatasetSplitXAttr splitXAttr;
    try {
      splitXAttr = LegacyProtobufSerializer.parseFrom(EasyProtobuf.EasyDatasetSplitXAttr.parser(),
          splitInfo.getDatasetSplitInfo().getExtendedProperty());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Could not deserialize split info", e);
    }

    String path = splitXAttr.getPath();
    Long mtime = splitXAttr.hasUpdateKey() && splitXAttr.getUpdateKey().hasLastModificationTime() ?
        splitXAttr.getUpdateKey().getLastModificationTime() : null;

    try (AutoCloseables.RollbackCloseable rollbackCloseable = new AutoCloseables.RollbackCloseable()) {
      if (implicitColumns.containsKey(IncrementalUpdateUtils.UPDATE_COLUMN) &&
          nameValuePairs.stream().noneMatch(nvp -> nvp.getName().equals(IncrementalUpdateUtils.UPDATE_COLUMN))) {
        Preconditions.checkState(mtime != null, "Split expected to contain mtime");
        NameValuePair<?> nvp = new ConstantColumnPopulators.BigIntNameValuePair(IncrementalUpdateUtils.UPDATE_COLUMN, mtime);
        rollbackCloseable.add(nvp);
        nameValuePairs.add(nvp);
      }

      String modifiedTimeColumn = ImplicitFilesystemColumnFinder.getModifiedTimeColumnName(options);
      if (implicitColumns.containsKey(modifiedTimeColumn) &&
          nameValuePairs.stream().noneMatch(nvp -> nvp.getName().equals(modifiedTimeColumn))) {
        NameValuePair<?> nvp = new ConstantColumnPopulators.BigIntNameValuePair(modifiedTimeColumn, mtime);
        rollbackCloseable.add(nvp);
        nameValuePairs.add(nvp);
      }

      // construct the relative path
      String fileColumn = ImplicitFilesystemColumnFinder.getFileColumnName(options);
      StringBuilder relativePath = new StringBuilder();

      if (implicitColumns.containsKey(fileColumn) &&
          nameValuePairs.stream().noneMatch(nvp -> nvp.getName().equals(fileColumn))) {

        Path fileAbsolutePath = Path.withoutSchemeAndAuthority(Path.of(path));
        if(basePath.isEmpty()){
          // primarily for unit tests
          relativePath.append(fileAbsolutePath.toString());
        } else {
          // tableAbsolutePath may correspond to an individual file or to a directory containing file(s)
          Path tableAbsolutePath = Path.of(basePath);
          if (fileAbsolutePath.compareTo(tableAbsolutePath) == 0) {
            // append the fileName
            relativePath.append(tableAbsolutePath.getName());
          } else {
            // append the relative directory
            relativePath.append(tableAbsolutePath.relativize(fileAbsolutePath));
          }
        }

        NameValuePair<?> nvp = new ConstantColumnPopulators.VarCharNameValuePair(fileColumn, relativePath.toString());
        rollbackCloseable.add(nvp);
        nameValuePairs.add(nvp);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return nameValuePairs;
  }
}
