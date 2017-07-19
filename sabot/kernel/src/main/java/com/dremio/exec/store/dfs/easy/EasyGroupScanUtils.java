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
package com.dremio.exec.store.dfs.easy;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.ScanStats;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.store.dfs.BlockMapBuilder;
import com.dremio.exec.store.dfs.CompleteFileWork;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.util.ImpersonationUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;

public class EasyGroupScanUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyGroupScanUtils.class);

  private FileSelection selection;
  private final FileSystemPlugin plugin;
  private final EasyFormatPlugin<?> formatPlugin;
  private final List<SchemaPath> columns;
  private List<CompleteFileWork> chunks;
  private String selectionRoot;
  private String userName;
  protected boolean includeModTime;

  public EasyGroupScanUtils(
      String userName,
      FileSelection selection,
      FileSystemPlugin plugin,
      EasyFormatPlugin<?> formatPlugin,
      List<SchemaPath> columns,
      String selectionRoot,
      boolean includeModTime
      ) throws IOException{
    this.plugin = plugin;
    this.selection = Preconditions.checkNotNull(selection);
    this.formatPlugin = Preconditions.checkNotNull(formatPlugin, "Unable to load format plugin for provided format config.");
    this.columns = columns == null ? GroupScan.ALL_COLUMNS : columns;
    this.selectionRoot = selectionRoot;
    this.includeModTime = includeModTime;
    this.userName = userName;
    initFromSelection(selection, formatPlugin);
  }


  private void initFromSelection(FileSelection selection, EasyFormatPlugin<?> formatPlugin) throws IOException {
    final FileSystemWrapper dfs = ImpersonationUtil.createFileSystem(userName, plugin.getFsConf());
    this.selection = selection;
    BlockMapBuilder b = new BlockMapBuilder(dfs, plugin.getExecutors());
    this.chunks = b.generateFileWork(selection.getStatuses(dfs), formatPlugin.isBlockSplittable());
    initFileStatusMap();
  }

  public Set<String> getFileSet() {
    try {
      return FluentIterable.from(selection.getStatuses(ImpersonationUtil.createFileSystem(userName, plugin.getFsConf())))
      .transform(new Function<FileStatus, String>() {

        @Nullable
        @Override
        public String apply(@Nullable FileStatus input) {
          return Path.getPathWithoutSchemeAndAuthority(input.getPath()).toString();
        }
      }).toSet();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public FileSelection getSelection() {
    return selection;
  }

  public FileSystemPlugin getPlugin() {
    return plugin;
  }

  public EasyFormatPlugin<?> getFormatPlugin() {
    return formatPlugin;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public List<CompleteFileWork> getChunks() {
    return chunks;
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  public String getUserName() {
    return userName;
  }

  public boolean isIncludeModTime() {
    return includeModTime;
  }

  public Map<String, FileStatus> getFileStatusMap() {
    return fileStatusMap;
  }

  public FileStatus getFileStatus(String name){
    return fileStatusMap.get(Path.getPathWithoutSchemeAndAuthority(new Path(name)).toString());
  }

  public ScanStats getScanStats() {
    return formatPlugin.getScanStats(this);
  }

  public ScanCostFactor getScanCostFactor() {
    return ScanCostFactor.EASY;
  }

  public Iterable<CompleteFileWork> getWorkIterable() {
    return new Iterable<CompleteFileWork>() {
      @Override
      public Iterator<CompleteFileWork> iterator() {
        return Iterators.unmodifiableIterator(chunks.iterator());
      }
    };
  }

  public MajorType getTypeForColumn(SchemaPath column) {
    if (IncrementalUpdateUtils.UPDATE_COLUMN.equals(column.getAsUnescapedPath())) {
      return Types.optional(MinorType.BIGINT);
    }
    return null;
  }

  private Map<String,FileStatus> fileStatusMap;

  private void initFileStatusMap() throws IOException {
    if (fileStatusMap != null) {
      return;
    }
    List<FileStatus> fileStatuses = selection.getStatuses(new FileSystemWrapper(plugin.getFsConf()));
    fileStatusMap = FluentIterable.from(fileStatuses).uniqueIndex(new Function<FileStatus, String>() {
      @Nullable
      @Override
      public String apply(@Nullable FileStatus input) {
        return Path.getPathWithoutSchemeAndAuthority(input.getPath()).toString();
      }
    });
  }

  public void populatePruningVector(ValueVector v, int index, SchemaPath column, String file) {
    try {
      initFileStatusMap();
      Preconditions.checkState(column.getAsUnescapedPath().equals(IncrementalUpdateUtils.UPDATE_COLUMN));
      ((NullableBigIntVector) v).getMutator().setSafe(index, fileStatusMap.get(file).getModificationTime());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return formatPlugin.supportsPushDown();
  }

  public List<SchemaPath> getPartitionColumns() {
    if (includeModTime) {
      return Collections.singletonList(SchemaPath.getSimplePath(IncrementalUpdateUtils.UPDATE_COLUMN));
    } else {
      return Collections.emptyList();
    }
  }
}
