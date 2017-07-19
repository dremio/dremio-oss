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
package com.dremio.exec.store.dfs.implicit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.server.options.Options;
import com.dremio.exec.server.options.TypeValidators.BooleanValidator;
import com.dremio.exec.server.options.TypeValidators.StringValidator;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators.BigIntNameValuePair;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators.VarCharNameValuePair;
import com.dremio.exec.util.ColumnUtils;
import com.google.common.collect.ImmutableList;

@Options
public class ImplicitFilesystemColumnFinder {

  public static final StringValidator IMPLICIT_PATH_FIELD_LABEL = new StringValidator("dremio.store.file.file-field-label", "$file");
  public static final StringValidator IMPLICIT_MOD_FIELD_LABEL = new StringValidator("dremio.store.file.mod-field-label", "$mtime");
  public static final BooleanValidator IMPLICIT_FILE_FIELD_ENABLE = new BooleanValidator("dremio.store.file.file-field-enabled", false);
  public static final BooleanValidator IMPLICIT_DIRS_FIELD_ENABLE = new BooleanValidator("dremio.store.file.dir-field-enabled", true);
  public static final BooleanValidator IMPLICIT_MOD_FIELD_ENABLE = new BooleanValidator("dremio.store.file.mod-field-enabled", false);

  private final FileSystemWrapper fs;
  private final List<SchemaPath> realColumns;
  private final List<ImplicitColumnExtractor<?>> implicitColumns;
  private final boolean selectAllColumns;
  private final boolean hasImplicitColumns;
  private final String partitionDesignator;
  private final String fileDesignator;
  private final String modTimeDesignator;
  private final boolean enableFileField;
  private final boolean enableDirsFields;
  private final boolean enableModTimeField;
  private final Set<Integer> selectedPartitions;

  public ImplicitFilesystemColumnFinder(OptionManager options, FileSystemWrapper fs, List<SchemaPath> columns) {
    this(options, fs, columns, false);
  }

  /**
   * Helper class that encapsulates logic for sorting out columns
   * between actual table columns, partition columns and implicit file columns.
   * Also populates map with implicit columns names as keys and their values
   */
  public ImplicitFilesystemColumnFinder(OptionManager options, FileSystemWrapper fs, List<SchemaPath> columns, boolean disableDirFields) {
    this.partitionDesignator = options.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR);
    this.fileDesignator = options.getOption(IMPLICIT_PATH_FIELD_LABEL);
    this.modTimeDesignator = options.getOption(IMPLICIT_MOD_FIELD_LABEL);
    this.enableDirsFields = !disableDirFields && options.getOption(IMPLICIT_DIRS_FIELD_ENABLE);
    this.enableFileField = options.getOption(IMPLICIT_FILE_FIELD_ENABLE);
    this.enableModTimeField = options.getOption(IMPLICIT_MOD_FIELD_ENABLE);
    this.selectedPartitions = new HashSet<>();
    this.fs = fs;

    final Matcher directoryMatcher = Pattern.compile(String.format("%s([0-9]+)", Pattern.quote(partitionDesignator))).matcher("");
    this.selectAllColumns = columns == null || ColumnUtils.isStarQuery(columns);

    final List<ImplicitColumnExtractor<?>> extractors = new ArrayList<>();
    Set<SchemaPath> selectedPaths = new LinkedHashSet<>();
    if (selectAllColumns) {
      selectedPaths.addAll(GroupScan.ALL_COLUMNS);

    } else {

      for (SchemaPath column : columns) {
       final String originalName = column.getAsUnescapedPath();
       final String lowerName = originalName.toLowerCase();

        if(enableDirsFields && directoryMatcher.reset(lowerName).matches()){
          // this is a directory match.
          int dir = Integer.parseInt(directoryMatcher.group(1));
          selectedPartitions.add(dir);
          extractors.add(new DirectoryExtractor(originalName, dir));
          continue;
        }

        if(enableFileField && fileDesignator.equals(lowerName)){
          extractors.add(new PathExtractor(originalName));
          continue;
        }

        if (this.enableModTimeField && modTimeDesignator.equals(lowerName)) {
          extractors.add(new ModTimeExtractor(originalName));
          continue;
        }

        if (IncrementalUpdateUtils.UPDATE_COLUMN.equals(lowerName)) {
          extractors.add(new IncrementalModTimeExtractor());
          continue;
        }

        selectedPaths.add(column);

      }
    }

    this.hasImplicitColumns = !extractors.isEmpty() || selectAllColumns;
    this.implicitColumns = ImmutableList.copyOf(extractors);
    this.realColumns = ImmutableList.copyOf(selectedPaths);
  }

  public String getPartitionDesignator(){
    return partitionDesignator;
  }

  public boolean containsPartition(int i){
    return selectAllColumns || selectedPartitions.contains(i);
  }

  public boolean hasImplicitColumns() {
    if(!enableDirsFields && !enableFileField){
      return false;
    }

    // this is slightly more broad than necessary but it satisfies the selection.
    return hasImplicitColumns || selectAllColumns;
  }

  private class DirectoryExtractor implements ImplicitColumnExtractor<String> {
    private final String name;
    private final int position;

    public DirectoryExtractor(String name, int position) {
      super();
      this.name = name;
      this.position = position;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getValue(ComponentizedPath work, String selectionRoot) {
      if(work.directories.length > position){
        return work.directories[position];
      }else {
        return null;
      }
    }

    @Override
    public VarCharNameValuePair getNameValuePair(ComponentizedPath work, String selectionRoot) {
      return new VarCharNameValuePair(name, getValue(work, selectionRoot));
    }
  }

  private class PathExtractor implements ImplicitColumnExtractor<String> {
    private final String name;

    public PathExtractor(String name) {
      super();
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getValue(ComponentizedPath work, String selectionRoot) {
      return work.path;
    }

    @Override
    public VarCharNameValuePair getNameValuePair(ComponentizedPath work, String selectionRoot) {
      return new VarCharNameValuePair(name, getValue(work, selectionRoot));
    }
  }

  private class IncrementalModTimeExtractor extends ModTimeExtractor {
    public IncrementalModTimeExtractor(){
      super(IncrementalUpdateUtils.UPDATE_COLUMN);
    }
  }

  private class ModTimeExtractor implements ImplicitColumnExtractor<Long> {
    private final String name;

    public ModTimeExtractor(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Long getValue(ComponentizedPath work, String selectionRoot) {
      try {
        return fs.getFileStatus(new Path(work.completePath)).getModificationTime();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public BigIntNameValuePair getNameValuePair(ComponentizedPath work, String selectionRoot) {
      return new BigIntNameValuePair(name, getValue(work, selectionRoot));
    }
  }

  public List<SchemaPath> getRealFields(){
    return this.realColumns;
  }

  interface ImplicitColumnExtractor<T> {
    String getName();
    T getValue(ComponentizedPath work, String selectionRoot);
    NameValuePair<T> getNameValuePair(ComponentizedPath work, String selectionRoot);
  }

  class ComponentizedPath {
    private String path;
    private String completePath;
    private String[] directories;

    public ComponentizedPath() {
      super();
    }

  }

  /**
   * Get a preview of the implicit field schema without moving to file level paths.
   *
   * @param selection
   * @return A list of name value pairs with null values. These should only be used for schema learning, not reading.
   * @throws IOException
   * @throws SchemaChangeException
   */
  public List<NameValuePair<?>> getImplicitFields(FileSelection selection) throws IOException, SchemaChangeException {

    final List<NameValuePair<?>> fields = new ArrayList<>();

    if (selection.selectionRoot != null) {
    FileStatus fileStatus = fs.getFileStatus(new Path(selection.selectionRoot));
      if (enableDirsFields && fileStatus.isDirectory()) {
        int maxDepth = getMaxDepth(fileStatus, 0, fs);
        for (int i = 0; i < maxDepth - 1; i++) {
          fields.add(new VarCharNameValuePair(partitionDesignator + i, "dir0"));
        }
      }
    }

    if(enableFileField) {
      fields.add(new VarCharNameValuePair(fileDesignator, "/"));
    }

    if (enableModTimeField) {
      fields.add(new BigIntNameValuePair(modTimeDesignator, 0L));
    }

    fields.add(new BigIntNameValuePair(IncrementalUpdateUtils.UPDATE_COLUMN, 0L));

    return fields;
  }


  private static int getMaxDepth(FileStatus fileStatus, int depth, FileSystemWrapper fs) throws IOException {
    int newDepth = depth;
    for (FileStatus child : fs.listStatus(fileStatus.getPath())) {
      if (child.isDirectory()) {
        newDepth = Math.max(newDepth, getMaxDepth(child, depth + 1, fs));
      } else {
        newDepth = Math.max(newDepth, depth+1);
      }
    }
    return newDepth;
  }

  /**
   * This method is more complicated than expected since the maximum dir size is based on
   * @param selectionRoot
   * @param workPaths
   * @return
   */
  public List<List<NameValuePair<?>>> getImplicitFields(final String selectionRoot, final Iterable<String> workPaths) {
    int max = 0;
    final List<ComponentizedPath> paths = new ArrayList<>();

    for(String work : workPaths) {
      final ComponentizedPath path = new ComponentizedPath();
      paths.add(path);
      if (selectionRoot != null) {
        String prefixString = Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot)).toString();
        String fullString = Path.getPathWithoutSchemeAndAuthority(new Path(work)).toString();

        if (prefixString.length() < fullString.length()) {
          path.path = removeLeadingSlash(fullString.substring(prefixString.length(), fullString.length()));
        } else {
          path.path = removeLeadingSlash(fullString);
        }

        final String[] prefix = prefixString.split("/");
        final String[] full = fullString.split("/");
        if (full.length > prefix.length) {
          final String[] q = ArrayUtils.subarray(full, prefix.length, full.length - 1);
          path.directories = q;
          max = Math.max(max, q.length);
        } else {
          path.directories = new String[0];
        }

        path.completePath = fullString;
      } else {
        // no selection root so no columns
        path.path = Path.getPathWithoutSchemeAndAuthority(new Path(work)).toString();
        path.completePath = path.path;
        path.directories = new String[0];
      }
    }

    final List<List<NameValuePair<?>>> pairs = new ArrayList<>();

    final List<ImplicitColumnExtractor<?>> implicitColumns;
    if (selectAllColumns) {
      implicitColumns = new ArrayList<>();
      if(enableDirsFields ){
        for (int i = 0; i < max; i++) {
          implicitColumns.add(new DirectoryExtractor(partitionDesignator + i, i));
        }
      }

      if(enableFileField){
        implicitColumns.add(new PathExtractor(fileDesignator));
      }

      if (enableModTimeField) {
        implicitColumns.add(new ModTimeExtractor(modTimeDesignator));
      }

      implicitColumns.add(new IncrementalModTimeExtractor());
    } else {
      implicitColumns = this.implicitColumns;
    }

    for(ComponentizedPath path : paths){

      path.directories = Arrays.copyOf(path.directories, max);

      List<NameValuePair<?>> values = new ArrayList<>();
      for (ImplicitColumnExtractor<?> extractor : implicitColumns) {
        values.add(extractor.getNameValuePair(path, selectionRoot));
      }
      pairs.add(values);

    }

    return pairs;

  }

  private String removeLeadingSlash(String path) {
    String result = path;
    while(result.charAt(0) == '/') {
      result = result.substring(1);
    }
    return result;
  }

}
