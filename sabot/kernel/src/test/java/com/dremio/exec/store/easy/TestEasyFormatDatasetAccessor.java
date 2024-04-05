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

package com.dremio.exec.store.easy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitListing;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyGroupScanUtils;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf.EasyDatasetSplitXAttr;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.test.DremioTest;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.NotLinkException;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.junit.Test;
import org.mockito.Answers;

public class TestEasyFormatDatasetAccessor extends DremioTest {

  private static final Path ROOT = Path.of("/root");
  private static final Path PART_1 = Path.of("/root/part1");
  private static final Path SUBPART_1 = Path.of("/root/part1/subpart1");
  private static final Path SUBPART_2 = Path.of("/root/part1/subpart2");
  private static final Path PART_2 = Path.of("/root/part2");
  private static final Path SUBPART_3 = Path.of("/root/part2/subpart3");
  private static final Path SUBPART_4 = Path.of("/root/part2/subpart4");
  private static final long MILLIS_PER_DAY = 1000 * 60 * 60 * 24;

  private static final Directory PARTITIONED_DIR =
      new Directory(
          ROOT,
          ImmutableList.of(
              new Directory(
                  PART_1,
                  ImmutableList.of(
                      new Directory(SUBPART_1, addFiles(SUBPART_1, 2)),
                      new Directory(SUBPART_2, addFiles(SUBPART_2, 3)))),
              new Directory(
                  PART_2,
                  ImmutableList.of(
                      new Directory(SUBPART_3, addFiles(SUBPART_3, 4)),
                      new Directory(SUBPART_4, addFiles(SUBPART_4, 5))))));

  private static final Directory UNPARTITIONED_DIR = new Directory(ROOT, addFiles(ROOT, 5));

  @Test
  public void testPartitionedChunkGeneration() throws IOException {
    // each partition is in one chunk
    validatePartitionChunks(PARTITIONED_DIR, true, null, 4);
  }

  @Test
  public void testPartitionedChunkGenerationWithUpdateTimeAsPartitionValue() throws IOException {
    // all 14 files are each in its own chunk due to unique modified times within each partition
    validatePartitionChunks(PARTITIONED_DIR, false, null, 14);
  }

  @Test
  public void testUnpartitionedChunkGeneration() throws IOException {
    // all 5 files are in one chunk
    validatePartitionChunks(UNPARTITIONED_DIR, true, null, 1);
  }

  @Test
  public void TestUnpartitionedChunkGenerationWithUpdateTimeAsPartitionValue() throws IOException {
    // all 5 file are each in its own chunk due to unique modified times
    validatePartitionChunks(UNPARTITIONED_DIR, false, null, 5);
  }

  @Test
  public void testPartitionedChunkGenerationWithLimit() throws IOException {
    // SUBPART_1 and SUBPART_2 are in one chunk each, SUBPART_3 and SUBPART_4 are each split into
    // two chunks
    validatePartitionChunks(PARTITIONED_DIR, true, 3L, 6);
  }

  @Test
  public void testUnpartitionedChunkGenerationWithLimit() throws IOException {
    // 5 files are splits into 2 chunks
    validatePartitionChunks(UNPARTITIONED_DIR, true, 3L, 2);
  }

  private void validatePartitionChunks(
      Directory root, boolean enableOptimalChunks, Long maxChunkSize, int expectedChunks)
      throws IOException {
    EasyFormatDatasetAccessor accessor =
        createDatasetAccessor(root, getOptions(enableOptimalChunks, maxChunkSize));

    // track sets of all files and files seen in chunks
    Set<String> allFiles = root.getFilePaths();
    Set<String> seenFiles = new HashSet<>();

    PartitionChunkListing chunkListing = accessor.listPartitionChunks();
    List<PartitionChunk> chunks = ImmutableList.copyOf(chunkListing.iterator());
    assertThat(chunks).hasSize(expectedChunks);

    for (PartitionChunk chunk : chunks) {
      Path partitionDir = root.getPath();
      Long modTimeFromPartition = null;

      // construct the partition dir path based on partition values
      for (PartitionValue partitionValue : chunk.getPartitionValues()) {
        if (partitionValue.getColumn().startsWith("dir")) {
          String value = ((PartitionValue.StringPartitionValue) partitionValue).getValue();
          partitionDir = partitionDir.resolve(Path.of(value));
        } else if (partitionValue.getColumn().equals(IncrementalUpdateUtils.UPDATE_COLUMN)) {
          // if mod time is a partition value, track it for later validation
          modTimeFromPartition = ((PartitionValue.LongPartitionValue) partitionValue).getValue();
        }
      }

      FileAttributes attr = root.findFileOrDirectory(partitionDir);
      assertThat(attr).isNotNull();
      assertThat(attr.isDirectory()).isTrue();
      Directory dir = (Directory) attr;

      // validate split-level path and mod time info
      DatasetSplitListing splitListing = chunk.getSplits();
      for (Iterator<? extends DatasetSplit> it = splitListing.iterator(); it.hasNext(); ) {
        DatasetSplit split = it.next();
        assertThat(split.getSizeInBytes()).isEqualTo(1000);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        split.getExtraInfo().writeTo(stream);
        EasyDatasetSplitXAttr splitXAttr = EasyDatasetSplitXAttr.parseFrom(stream.toByteArray());
        FileAttributes fileAttr = dir.findFileOrDirectory(Path.of(splitXAttr.getPath()));
        assertThat(fileAttr).isNotNull();
        assertThat(fileAttr.isRegularFile()).isTrue();
        assertThat(splitXAttr.hasUpdateKey()).isTrue();
        assertThat(splitXAttr.getUpdateKey().getLastModificationTime())
            .isEqualTo(fileAttr.lastModifiedTime().toMillis());
        if (modTimeFromPartition != null) {
          assertThat(modTimeFromPartition).isEqualTo(fileAttr.lastModifiedTime().toMillis());
        }
        seenFiles.add(splitXAttr.getPath());
      }
    }

    // ensure that chunks contained all expected files
    assertThat(seenFiles).containsExactlyInAnyOrderElementsOf(allFiles);
  }

  private EasyFormatDatasetAccessor createDatasetAccessor(
      Directory root, OptionManager optionManager) throws IOException {
    FileSystem fs = getFileSystem(root);
    FileSelection selection =
        Preconditions.checkNotNull(FileSelection.create("root", fs, ROOT, 10_000))
            .minusDirectories();
    SabotContext context = getContext(optionManager);

    PreviousDatasetInfo previousDatasetInfo =
        new PreviousDatasetInfo(null, null, null, null, null, true);
    return new EasyFormatDatasetAccessor(
        DatasetType.PHYSICAL_DATASET,
        fs,
        selection,
        getFileSystemPlugin(context, fs),
        new NamespaceKey("table"),
        FileProtobuf.FileUpdateKey.newBuilder().build(),
        getFormatPlugin(context),
        previousDatasetInfo,
        100);
  }

  private OptionManager getOptions(boolean enableChunking, Long maxChunkSize) {
    OptionValidatorListingImpl validatorListing =
        new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT);
    OptionManager options =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionManager(new DefaultOptionManager(validatorListing))
            .withOptionManager(new SessionOptionManagerImpl(validatorListing))
            .build();
    options.setOption(
        OptionValue.createBoolean(
            OptionValue.OptionType.SESSION,
            ExecConstants.ENABLE_OPTIMAL_FILE_PARTITION_CHUNKS.getOptionName(),
            enableChunking));
    if (maxChunkSize != null) {
      options.setOption(
          OptionValue.createLong(
              OptionValue.OptionType.SESSION,
              ExecConstants.FILE_SPLITS_PER_PARTITION_CHUNK.getOptionName(),
              maxChunkSize));
    }
    return options;
  }

  private FileSystem getFileSystem(Directory root) throws IOException {
    FileSystem fs = mock(FileSystem.class);
    when(fs.list(any(), any()))
        .thenAnswer(
            i -> {
              Path path = i.getArgument(0);
              Predicate<Path> filter = i.getArgument(1);
              FileAttributes attrs = root.findFileOrDirectory(path);
              Preconditions.checkState(attrs != null && attrs.isDirectory());
              return ((Directory) attrs).getDirectoryStream(filter);
            });
    when(fs.glob(any(), any()))
        .thenAnswer(
            i -> {
              Path path = i.getArgument(0);
              Predicate<Path> filter = i.getArgument(1);
              FileAttributes attrs = root.findFileOrDirectory(path);
              Preconditions.checkState(attrs != null && attrs.isDirectory());
              return ((Directory) attrs).getDirectoryStream(filter);
            });
    return fs;
  }

  private SabotContext getContext(OptionManager optionManager) {
    SabotContext context = mock(SabotContext.class, Answers.RETURNS_DEEP_STUBS);
    when(context.getOptionManager()).thenReturn(optionManager);
    return context;
  }

  private FileSystemPlugin<?> getFileSystemPlugin(SabotContext context, FileSystem fs)
      throws IOException {
    FileSystemPlugin<?> plugin = mock(FileSystemPlugin.class, Answers.RETURNS_DEEP_STUBS);
    when(plugin.getContext()).thenReturn(context);
    when(plugin.createFS(any())).thenReturn(fs);
    return plugin;
  }

  private FormatPlugin getFormatPlugin(SabotContext context) throws IOException {
    EasyFormatPlugin<?> formatPlugin = mock(EasyFormatPlugin.class, Answers.RETURNS_DEEP_STUBS);
    when(formatPlugin.getContext()).thenReturn(context);
    when(formatPlugin.getGroupScan(any(), any(), any(), any()))
        .thenAnswer(
            i -> {
              String userName = i.getArgument(0);
              FileSystemPlugin<?> plugin = i.getArgument(1);
              FileSelection selection = i.getArgument(2);
              List<SchemaPath> columns = i.getArgument(3);
              return new EasyGroupScanUtils(
                  userName,
                  selection,
                  plugin,
                  formatPlugin,
                  columns,
                  selection.getSelectionRoot(),
                  false);
            });
    return formatPlugin;
  }

  private static long getModTime(int daysOffset) {
    return MILLIS_PER_DAY * daysOffset;
  }

  private static File addFile(Path parent, int fileNum) {
    return new File(parent.resolve("file" + fileNum), getModTime(fileNum), 1000);
  }

  private static List<FileAttributes> addFiles(Path parent, int numFiles) {
    ImmutableList.Builder<FileAttributes> builder = ImmutableList.builder();
    for (int i = 0; i < numFiles; i++) {
      builder.add(addFile(parent, i));
    }
    return builder.build();
  }

  private static class File implements FileAttributes {

    private final Path path;
    private final long lastModifiedTime;
    private final long size;

    public File(Path path, long lastModifiedTime, long size) {
      this.path = path;
      this.lastModifiedTime = lastModifiedTime;
      this.size = size;
    }

    @Override
    public Path getPath() {
      return path;
    }

    @Override
    public Path getSymbolicLink() throws NotLinkException {
      throw new NotLinkException(path.toString());
    }

    @Override
    public UserPrincipal owner() {
      throw new UnsupportedOperationException();
    }

    @Override
    public GroupPrincipal group() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<PosixFilePermission> permissions() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileTime lastModifiedTime() {
      return FileTime.fromMillis(lastModifiedTime);
    }

    @Override
    public FileTime lastAccessTime() {
      return FileTime.fromMillis(lastModifiedTime);
    }

    @Override
    public FileTime creationTime() {
      return FileTime.fromMillis(0);
    }

    @Override
    public boolean isRegularFile() {
      return true;
    }

    @Override
    public boolean isDirectory() {
      return false;
    }

    @Override
    public boolean isSymbolicLink() {
      return false;
    }

    @Override
    public boolean isOther() {
      return false;
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public Object fileKey() {
      throw new UnsupportedOperationException();
    }
  }

  private static class Directory extends File {

    private final List<FileAttributes> children;

    public Directory(Path path, List<FileAttributes> children) {
      super(path, 0, 0);
      this.children = children;
    }

    @Override
    public boolean isRegularFile() {
      return false;
    }

    @Override
    public boolean isDirectory() {
      return true;
    }

    public List<FileAttributes> getChildren() {
      return children;
    }

    public Set<String> getFilePaths() {
      Set<String> paths = new HashSet<>();
      for (FileAttributes attrs : children) {
        if (attrs instanceof Directory) {
          paths.addAll(((Directory) attrs).getFilePaths());
        } else if (attrs instanceof File) {
          paths.add(attrs.getPath().toString());
        }
      }

      return paths;
    }

    public FileAttributes findFileOrDirectory(Path path) {
      if (path.equals(getPath())) {
        return this;
      }

      for (FileAttributes attrs : children) {
        if (path.equals(attrs.getPath())) {
          return attrs;
        }

        if (attrs.isDirectory() && path.toString().startsWith(attrs.getPath().toString())) {
          return ((Directory) attrs).findFileOrDirectory(path);
        }
      }

      return null;
    }

    public DirectoryStream<FileAttributes> getDirectoryStream(Predicate<Path> filter) {
      return new DirectoryStream<FileAttributes>() {
        @Override
        public Iterator<FileAttributes> iterator() {
          return children.stream().filter(attrs -> filter.test(attrs.getPath())).iterator();
        }

        @Override
        public void close() {}
      };
    }
  }
}
