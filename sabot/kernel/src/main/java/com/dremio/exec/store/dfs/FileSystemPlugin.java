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
package com.dremio.exec.store.dfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Function;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.security.AccessControlException;

import com.dremio.common.JSONOptions;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.dotfile.DotFile;
import com.dremio.exec.dotfile.DotFileType;
import com.dremio.exec.dotfile.DotFileUtil;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.base.OldAbstractGroupScan;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.AbstractStoragePlugin;
import com.dremio.exec.store.ClassPathFileSystem;
import com.dremio.exec.store.ConversionContext;
import com.dremio.exec.store.LocalSyncableFileSystem;
import com.dremio.exec.store.PartitionNotFoundException;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SchemaEntity;
import com.dremio.exec.store.SchemaEntity.SchemaEntityType;
import com.dremio.exec.store.StoragePluginOptimizerRule;
import com.dremio.exec.store.parquet.ParquetFooterCache;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A Storage engine associated with a Hadoop FileSystem Implementation. Examples include HDFS, MapRFS, QuantacastFileSystem,
 * LocalFileSystem, as well Dremio specific CachedFileSystem, ClassPathFileSystem and LocalSyncableFileSystem.
 * Tables are file names, directories and path patterns. This storage engine delegates to FSFormatEngines but shares
 * references to the FileSystem configuration and path management.
 */
public class FileSystemPlugin extends AbstractStoragePlugin<ConversionContext.NamespaceConversionContext> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemPlugin.class);

  /**
   * Default {@link Configuration} instance. Use this instance through {@link #getNewFsConf()} to create new copies
   * of {@link Configuration} objects.
   */
  private static final Configuration DEFAULT_CONFIGURATION = new Configuration();

  private final LogicalPlanPersistence lpPersistance;
  private final FileSystemConfig config;
  private final String storageName;
  private final SabotContext context;


  public SabotContext getContext() {
    return context;
  }

  private volatile ParquetFooterCache footerCache;
  private Configuration fsConf;
  private FormatCreator formatCreator;

  private FormatPluginOptionExtractor optionExtractor;
  private ArrayList<FormatMatcher> matchers;

  private List<FormatMatcher> dropFileMatchers;
  private CompressionCodecFactory codecFactory;

  public FileSystemPlugin(final FileSystemConfig config, final SabotContext dContext, final String storageName) throws ExecutionSetupException {
    this.storageName = storageName;
    this.config = config;
    this.lpPersistance = dContext.getLpPersistence();
    this.context = dContext;
    this.fsConf = getNewFsConf();
  }

  @Override
  public FileSystemStoragePlugin2 getStoragePlugin2() {
    return new FileSystemStoragePlugin2(storageName, config, this);
  }

  @Override
  public void start() throws IOException {
    if (config.getConfig() != null) {
      for (Entry<String, String> prop : config.getConfig().entrySet()) {
        fsConf.set(prop.getKey(), prop.getValue());
      }
    }

    if (!Strings.isEmpty(config.getConnection())) {
      FileSystem.setDefaultUri(fsConf, config.getConnection());
    }

    Map<String,String> map =  ImmutableMap.of(
            "fs.classpath.impl", ClassPathFileSystem.class.getName(),
            "fs.dremio-local.impl", LocalSyncableFileSystem.class.getName()
    );
    for(Entry<String, String> prop : map.entrySet()) {
      fsConf.set(prop.getKey(), prop.getValue());
    }

    this.optionExtractor = new FormatPluginOptionExtractor(context.getClasspathScan());
    this.matchers = Lists.newArrayList();
    this.formatCreator = createFormatCreator();
    this.codecFactory = new CompressionCodecFactory(getFsConf());

    for (FormatMatcher m : formatCreator.getFormatMatchers()) {
      matchers.add(m);
    }

//    boolean footerNoSeek = contetMutext.getOptionManager().getOption(ExecConstants.PARQUET_FOOTER_NOSEEK);

    // NOTE: Add fallback format matcher if given in the configuration. Make sure fileMatchers is an order-preserving list.
    dropFileMatchers = matchers.subList(0, matchers.size());
    refreshState();
  }


  @Override
  public SchemaMutability getMutability() {
    return config.getSchemaMutability();
  }

  public ParquetFooterCache getFooterCache(){
    int cacheSize = context.isCoordinator() ?
      (int) context.getOptionManager().getOption(ExecConstants.PARQUET_FOOTER_CACHESIZE_COORD) :
      (int) context.getOptionManager().getOption(ExecConstants.PARQUET_FOOTER_CACHESIZE_EXEC);

    if(footerCache == null){
      synchronized(this){
        if(footerCache == null){
          footerCache = new ParquetFooterCache(getFS(ImpersonationUtil.getProcessUserName()), cacheSize, true);
        }
      }
      return footerCache;
    }
    return footerCache;
  }

  @Override
  public boolean refreshState() {
    final FileSystemWrapper fs = getFS(ImpersonationUtil.getProcessUserName());
    if (!fs.isPdfs()) {
      try {
        fs.listStatus(new Path(config.getPath()));
        return setState(SourceState.GOOD);
      } catch (Exception e) {
        return setState(badState(e));
      }
    } else {
      return false;
    }
  }

  public List<SchemaEntity> list(List<String> folderPath, String userName) {
    try {
      List<FileStatus> files = Lists.newArrayList(getFS(userName).listStatus(Path.mergePaths(new Path(getConfig().getPath()), PathUtils.toFSPathSkipRoot(folderPath, getStorageName()))));

      final Set<List<String>> tableNames = Sets.newHashSet();
      final NamespaceService ns = context.getNamespaceService(userName);
      final NamespaceKey folderNSKey = new NamespaceKey(folderPath);
      if (ns.exists(folderNSKey, Type.DATASET)) {
        // if the folder is a dataset, then there is nothing to list
        return ImmutableList.of();
      }
      if (ns.exists(folderNSKey)) {
        for(NameSpaceContainer entity : ns.list(folderNSKey)) {
          if (entity.getType() == Type.DATASET) {
            tableNames.add(getFullPath(entity.getDataset().getFullPathList()));
          }
        }
      }

      Iterable<SchemaEntity> itr = Iterables.transform(files, new com.google.common.base.Function<FileStatus, SchemaEntity>() {
        @Nullable
        @Override
        public SchemaEntity apply(@Nullable FileStatus input) {
          List<String> p = PathUtils.toPathComponents(input.getPath());
          if (input.isDirectory()) {
            if (tableNames.contains(p)) {
              return new SchemaEntity(PathUtils.getQuotedFileName(input.getPath()), SchemaEntityType.FOLDER_TABLE, input.getOwner());
            } else {
              return new SchemaEntity(PathUtils.getQuotedFileName(input.getPath()), SchemaEntityType.FOLDER, input.getOwner());
            }
          } else {
            if (tableNames.contains(p)) {
              return new SchemaEntity(PathUtils.getQuotedFileName(input.getPath()), SchemaEntityType.FILE_TABLE, input.getOwner());
            } else {
              return new SchemaEntity(PathUtils.getQuotedFileName(input.getPath()), SchemaEntityType.FILE, input.getOwner());
            }
          }
        }
      });
      return ImmutableList.<SchemaEntity>builder().addAll(itr).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (NamespaceException e) {
      throw new RuntimeException(e);
    }
  }

  protected FormatCreator createFormatCreator() {
    return new FormatCreator(context, getConfig(), context.getClasspathScan(), this);
  }

  public FormatPluginConfig createConfigForTable(String tableName, Map<String, Object> storageOptions) {
    return optionExtractor.createConfigForTable(tableName, storageOptions);
  }

  public FileSystemWrapper getFS(String userName) {
    if (!config.isImpersonationEnabled()) {
      userName = ImpersonationUtil.getProcessUserName();
    }
    return ImpersonationUtil.createFileSystem(userName, getFsConf());
  }

  private List<String> getFullPath(List<String> tableSchemaPath) {
    String parentPath = config.getPath();
    List<String> fullPath = new ArrayList<>();
    fullPath.addAll(PathUtils.toPathComponents(new Path(parentPath)));
    for (String pathComponent : tableSchemaPath.subList(1, tableSchemaPath.size())) {
      fullPath.add(PathUtils.removeQuotes(pathComponent));
    }
    return fullPath;
  }

  @Override
  public RelNode getRel(final RelOptCluster cluster, final RelOptTable relOptTable, final ConversionContext.NamespaceConversionContext relContext) {
    throw new UnsupportedOperationException();
  }

  static <T> List<T> stripFirst(List<T> list) {
    return list.subList(1, list.size());
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public FileSystemConfig getConfig() {
    return config;
  }

  @Override
  public OldAbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<String> tableSchemaPath, List<SchemaPath> columns)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  public ArrayList<FormatMatcher> getMatchers() {
    return matchers;
  }

  public FormatPlugin getFormatPlugin(String name) {
    return formatCreator.getFormatPluginByName(name);
  }

  public FormatPlugin getFormatPlugin(FormatPluginConfig config) {
    FormatPlugin plugin = formatCreator.getFormatPluginByConfig(config);
    if (plugin == null) {
      plugin = formatCreator.newFormatPlugin(config);
    }
    return plugin;
  }

  public CompressionCodecFactory getCodecFactory() {
    return codecFactory;
  }

  @Override
  public Set<StoragePluginOptimizerRule> getLogicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    Builder<StoragePluginOptimizerRule> setBuilder = ImmutableSet.builder();
    for(FormatPlugin plugin : formatCreator.getConfiguredFormatPlugins()){
      Set<StoragePluginOptimizerRule> rules = plugin.getOptimizerRules(optimizerRulesContext);
      if(rules != null && rules.size() > 0){
        setBuilder.addAll(rules);
      }
    }
    return setBuilder.build();
  }

  public Configuration getFsConf() {
    return fsConf;
  }

  public String getStorageName() {
    return storageName;
  }

  public Collection<NodeEndpoint> getExecutors() {
    return context.getExecutors();
  }

  /**
   * Create a new copy of {@link Configuration} object.
   *
   * It saves the time taken for costly classpath scanning of *-site.xml files for each new {@link Configuration}
   * object created.
   *
   * @return new {@link Configuration} instance.
   */
  public static final Configuration getNewFsConf() {
    return new Configuration(DEFAULT_CONFIGURATION);
  }

  public FormatCreator getFormatCreator() {
    return formatCreator;
  }

  @Override
  public List<Function> getFunctions(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return optionExtractor.getFunctions(tableSchemaPath, this, schemaConfig);
  }

  private FormatMatcher findMatcher(FileSystemWrapper fs, FileSelection file) {
    FormatMatcher matcher = null;
    try {
      for (FormatMatcher m : dropFileMatchers) {

        if (m.matches(fs, file, codecFactory)) {
          return m;
        }
      }
    } catch (IOException e) {
      logger.debug("Failed to find format matcher for file: %s", file, e);
    }
    return matcher;
  }

  private boolean isHomogeneous(FileSystemWrapper fs, FileSelection fileSelection) throws IOException {
    FormatMatcher matcher = null;
    FileSelection noDir = fileSelection.minusDirectories();

    for(FileStatus s : noDir.getStatuses()) {
      FileSelection subSelection = FileSelection.create(s);
      if (matcher == null) {
        matcher = findMatcher(fs, subSelection);
        if(matcher == null) {
          return false;
        }
      }

      if(!matcher.matches(fs, subSelection, codecFactory)) {
        return false;
      }
    }
    return true;
  }

  /**
   * We check if the table contains homogeneous file formats that Dremio can read. Once the checks are performed
   * we rename the file to start with an "_". After the rename we issue a recursive delete of the directory.
   */
  public void dropTable(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    FileSystemWrapper fs = getFS(schemaConfig.getUserName());
    String defaultLocation = config.getPath();
    List<String> fullPath = new ArrayList<>();
    fullPath.addAll(PathUtils.toPathComponents(new Path(config.getPath())));
    for (String pathComponent : tableSchemaPath.subList(1, tableSchemaPath.size())) {
      fullPath.add(PathUtils.removeQuotes(pathComponent));
    }
    FileSelection fileSelection;
    try {
      fileSelection = FileSelection.create(fs, fullPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (fileSelection == null) {
      throw UserException
          .validationError()
          .message(String.format("Table [%s] not found", SqlUtils.quotedCompound(tableSchemaPath)))
          .build(logger);
    }

    try {
      if (!isHomogeneous(fs, fileSelection)) {
        throw UserException
                .validationError()
                .message("Table contains different file formats. \n" +
                        "Drop Table is only supported for directories that contain homogeneous file formats consumable by Dremio")
                .build(logger);
      }

      // Generate unique identifier which will be added as a suffix to the table name
      ThreadLocalRandom r = ThreadLocalRandom.current();
      long time =  (System.currentTimeMillis()/1000);
      Long p1 = ((Integer.MAX_VALUE - time) << 32) + r.nextInt();
      Long p2 = r.nextLong();
      final String fileNameDelimiter = FileSystemWrapper.HIDDEN_FILE_PREFIX;
      final String newFileName = FileSystemWrapper.HIDDEN_FILE_PREFIX + fullPath.get(fullPath.size() - 1) + fileNameDelimiter
              + p1 + fileNameDelimiter + p2;

      List<String> newFullPath = ImmutableList.<String>builder().addAll(fullPath.subList(0, fullPath.size() - 1)).add(newFileName).build();

      fs.rename(new Path(defaultLocation, PathUtils.toFSPath(fullPath)), new Path(defaultLocation, PathUtils.toFSPath(newFullPath)));
      fs.delete(new Path(defaultLocation, PathUtils.toFSPath(newFullPath)), true);
    } catch (AccessControlException e) {
      throw UserException
              .permissionError(e)
              .message("Unauthorized to drop table")
              .build(logger);
    } catch (IOException e) {
      throw UserException
              .dataWriteError(e)
              .message("Failed to drop table: " + e.getMessage())
              .build(logger);
    }
  }

  protected View getView(DotFile f) throws IOException {
    assert f.getType() == DotFileType.VIEW;
    return f.getView(lpPersistance);
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    List<DotFile> files = Collections.emptyList();
    try {
      try {
        files = DotFileUtil.getDotFiles(getFS(schemaConfig.getUserName()), new Path(config.getPath()), tableSchemaPath.get(tableSchemaPath.size() - 1), DotFileType.VIEW);
      } catch (AccessControlException e) {
        if (!schemaConfig.getIgnoreAuthErrors()) {
          logger.debug(e.getMessage());
          throw UserException.permissionError(e)
                  .message("Not authorized to list or query tables in schema %s", tableSchemaPath)
                  .build(logger);
        }
      } catch (IOException e) {
        logger.warn("Failure while trying to list view tables in workspace [{}]", tableSchemaPath, e);
      }

      for (DotFile f : files) {
        switch (f.getType()) {
        case VIEW:
          try {
            return new ViewTable(getView(f), f.getOwner(), schemaConfig.getViewExpansionContext());
          } catch (AccessControlException e) {
            if (!schemaConfig.getIgnoreAuthErrors()) {
              logger.debug(e.getMessage());
              throw UserException.permissionError(e)
                      .message("Not authorized to read view [%s] in schema %s", tableSchemaPath.get(tableSchemaPath.size() - 1), tableSchemaPath.subList(0, tableSchemaPath.size() - 1))
                      .build(logger);
            }
          } catch (IOException e) {
            logger.warn("Failure while trying to load {}.view.meta file in workspace [{}]", tableSchemaPath.get(tableSchemaPath.size() - 1), tableSchemaPath.subList(0, tableSchemaPath.size() - 1), e);
          }
        }
      }
    } catch (UnsupportedOperationException e) {
      logger.debug("The filesystem for this workspace does not support this operation.", e);
    }
    return null;
  }

  @Override
  public Iterable<String> getSubPartitions(List<String> table,
                                           List<String> partitionColumns,
                                           List<String> partitionValues,
                                           SchemaConfig schemaConfig
  ) throws PartitionNotFoundException {
    List<FileStatus> fileStatuses;
    try {
      Path fullPath = PathUtils.toFSPath(
              ImmutableList.<String>builder()
                      .addAll(PathUtils.toPathComponents(new Path(config.getPath())))
                      .addAll(table.subList(1, table.size()))
                      .build());
      fileStatuses = getFS(schemaConfig.getUserName()).list(fullPath, false);
    } catch (IOException e) {
      throw new PartitionNotFoundException("Error finding partitions for table " + table, e);
    }
    return new SubDirectoryList(fileStatuses);
  }

  @Override
  public boolean createView(List<String> tableSchemaPath, View view, SchemaConfig schemaConfig) throws IOException {
    Path viewPath = getViewPath(tableSchemaPath);
    FileSystemWrapper fs = getFS(schemaConfig.getUserName());
    boolean replaced = fs.exists(viewPath);
    final FsPermission viewPerms =
            new FsPermission(schemaConfig.getOption(ExecConstants.NEW_VIEW_DEFAULT_PERMS_KEY).string_val);
    try (OutputStream stream = FileSystemWrapper.create(fs, viewPath, viewPerms)) {
      lpPersistance.getMapper().writeValue(stream, view);
    }
    return replaced;
  }

  @Override
  public void dropView(SchemaConfig schemaConfig, List<String> tableSchemaPath) throws IOException {
    getFS(schemaConfig.getUserName()).delete(getViewPath(tableSchemaPath), false);
  }

  protected Path getViewPath(List<String> tableSchemaPath) {
    List<String> fullPath = ImmutableList.<String>builder().addAll(PathUtils.toPathComponents(new Path(config.getPath()))).addAll(tableSchemaPath.subList(1, tableSchemaPath.size() - 1)).build();
    String parentPath = PathUtils.toFSPathString(fullPath);
    return DotFileType.VIEW.getPath(parentPath, tableSchemaPath.get(tableSchemaPath.size() - 1));
  }

  @Override
  public boolean folderExists(SchemaConfig schemaConfig, List<String> folderPath) throws IOException {
    return folderExists(schemaConfig.getUserName(), folderPath);
  }

  protected boolean folderExists(String username, List<String> folderPath) throws IOException {
    return getFS(username).isDirectory(PathUtils.toFSPath(
            ImmutableList.<String>builder()
                    .addAll(PathUtils.toPathComponents(new Path(config.getPath())))
                    .addAll(folderPath.subList(1, folderPath.size()))
                    .build()));
  }

  protected boolean fileExists(String username, List<String> filePath) throws IOException {
    return getFS(username).isFile(PathUtils.toFSPath(
      ImmutableList.<String>builder()
        .addAll(PathUtils.toPathComponents(new Path(config.getPath())))
        .addAll(filePath.subList(1, filePath.size()))
        .build()));
  }

  protected boolean exists(String username, List<String> filePath) throws IOException {
    return getFS(username).exists(PathUtils.toFSPath(
      ImmutableList.<String>builder()
        .addAll(PathUtils.toPathComponents(new Path(config.getPath())))
        .addAll(filePath.subList(1, filePath.size()))
        .build()));
  }

  public FormatPluginOptionExtractor getOptionExtractor() {
    return optionExtractor;
  }

}
