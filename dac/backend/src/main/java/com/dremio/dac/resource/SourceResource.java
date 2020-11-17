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
package com.dremio.dac.resource;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.Objects;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.DatasetsResource;
import com.dremio.dac.explore.QueryExecutor;
import com.dremio.dac.explore.model.FileFormatUI;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.model.folder.Folder;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.sources.FormatTools;
import com.dremio.dac.model.sources.PhysicalDataset;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.service.errors.NewDatasetQueryException;
import com.dremio.dac.service.errors.PhysicalDatasetNotFoundException;
import com.dremio.dac.service.errors.SourceFileNotFoundException;
import com.dremio.dac.service.errors.SourceFolderNotFoundException;
import com.dremio.dac.service.errors.SourceNotFoundException;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.util.ResourceUtil;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.SourceCatalog;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.exec.server.ContextService;
import com.dremio.file.File;
import com.dremio.file.SourceFilePath;
import com.dremio.service.namespace.BoundedDatasetCount;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.physicaldataset.proto.PhysicalDatasetConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.reflection.ReflectionAdministrationService;

/**
 * Rest resource for sources.
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/source/{sourceName}")
public class SourceResource extends BaseResourceWithAllocator {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SourceResource.class);

  private final QueryExecutor executor;
  private final NamespaceService namespaceService;
  private final ReflectionAdministrationService.Factory reflectionService;
  private final SourceService sourceService;
  private final SourceName sourceName;
  private final SecurityContext securityContext;
  private final SourcePath sourcePath;
  private final DatasetsResource datasetsResource;
  private final ConnectionReader cReader;
  private final SourceCatalog sourceCatalog;
  private final FormatTools formatTools;
  private final ContextService context;

  @Inject
  public SourceResource(
      NamespaceService namespaceService,
      ReflectionAdministrationService.Factory reflectionService,
      SourceService sourceService,
      @PathParam("sourceName") SourceName sourceName,
      QueryExecutor executor,
      SecurityContext securityContext,
      DatasetsResource datasetsResource,
      ConnectionReader cReader,
      SourceCatalog sourceCatalog,
      FormatTools formatTools,
      ContextService context,
      BufferAllocatorFactory allocatorFactory
      ) throws SourceNotFoundException {
    super(allocatorFactory);
    this.namespaceService = namespaceService;
    this.reflectionService = reflectionService;
    this.sourceService = sourceService;
    this.sourceName = sourceName;
    this.securityContext = securityContext;
    this.datasetsResource = datasetsResource;
    this.sourcePath = new SourcePath(sourceName);
    this.executor = executor;
    this.cReader = cReader;
    this.sourceCatalog = sourceCatalog;
    this.formatTools = formatTools;
    this.context = context;
  }

  protected SourceUI newSource(SourceConfig config) throws Exception {
    return SourceUI.get(config, cReader);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public SourceUI getSource(@QueryParam("includeContents") @DefaultValue("true") boolean includeContents)
      throws Exception {
    try {
      final SourceConfig config = namespaceService.getSource(sourcePath.toNamespaceKey());
      final SourceState sourceState = sourceService.getSourceState(sourcePath.getSourceName().getName());
      if (sourceState == null) {
        throw new SourceNotFoundException(sourcePath.getSourceName().getName());
      }

      final BoundedDatasetCount datasetCount = namespaceService.getDatasetCount(new NamespaceKey(config.getName()),
          BoundedDatasetCount.SEARCH_TIME_LIMIT_MS, BoundedDatasetCount.COUNT_LIMIT_TO_STOP_SEARCH);
      final SourceUI source = newSource(config)
          .setNumberOfDatasets(datasetCount.getCount());
      source.setDatasetCountBounded(datasetCount.isCountBound() || datasetCount.isTimeBound());

      source.setState(sourceState);

      final AccelerationSettings settings = reflectionService.get(ReflectionContext.SYSTEM_USER_CONTEXT).getReflectionSettings().getReflectionSettings(sourcePath.toNamespaceKey());
      if (settings != null) {
        source.setAccelerationRefreshPeriod(settings.getRefreshPeriod());
        source.setAccelerationGracePeriod(settings.getGracePeriod());
      }
      if (includeContents) {
        source.setContents(sourceService.listSource(sourcePath.getSourceName(),
          namespaceService.getSource(sourcePath.toNamespaceKey()), securityContext.getUserPrincipal().getName()));
      }
      return source;
    } catch (NamespaceNotFoundException nfe) {
      throw new SourceNotFoundException(sourcePath.getSourceName().getName(), nfe);
    }
  }

  @RolesAllowed("admin")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public void deleteSource(@QueryParam("version") String version) throws NamespaceException, SourceNotFoundException {
    if (version == null) {
      throw new ClientErrorException("missing version parameter");
    }
    try {
      SourceConfig config = namespaceService.getSource(new SourcePath(sourceName).toNamespaceKey());
      if(!Objects.equals(config.getTag(), version)) {
        throw new ConcurrentModificationException(String.format("Cannot delete source \"%s\", version provided \"%s\" is different from version found \"%s\"",
          sourceName, version, config.getTag()));
      }
      sourceCatalog.deleteSource(config);
    } catch (NamespaceNotFoundException nfe) {
      throw new SourceNotFoundException(sourcePath.getSourceName().getName(), nfe);
    } catch (ConcurrentModificationException e) {
      throw ResourceUtil.correctBadVersionErrorMessage(e, "source", sourceName.getName());
    }
  }

  @GET
  @Path("/folder/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Folder getFolder(@PathParam("path") String path, @QueryParam("includeContents") @DefaultValue("true") boolean includeContents)
      throws NamespaceException, IOException, SourceFolderNotFoundException, PhysicalDatasetNotFoundException, SourceNotFoundException {
    sourceService.checkSourceExists(sourceName);
    SourceFolderPath folderPath = SourceFolderPath.fromURLPath(sourceName, path);
    return sourceService.getFolder(sourceName, folderPath, includeContents, securityContext.getUserPrincipal().getName());
  }

  @GET
  @Path("/dataset/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public PhysicalDataset getPhysicalDataset(@PathParam("path") String path)
      throws SourceNotFoundException, NamespaceException {
    sourceService.checkSourceExists(sourceName);
    PhysicalDatasetPath datasetPath = PhysicalDatasetPath.fromURLPath(sourceName, path);
    return sourceService.getPhysicalDataset(sourceName, datasetPath);
  }

  private boolean useFastPreview() {
    return context.get().getOptionManager().getOption(FormatTools.FAST_PREVIEW);
  }

  @GET
  @Path("/file/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public File getFile(@PathParam("path") String path)
      throws SourceNotFoundException, NamespaceException, PhysicalDatasetNotFoundException {
    if (useFastPreview()) {
      return sourceService.getFileDataset(sourceName, asFilePath(path), null);
    }

    sourceService.checkSourceExists(sourceName);

    final SourceFilePath filePath = SourceFilePath.fromURLPath(sourceName, path);
    return sourceService.getFileDataset(sourceName, filePath, null);
  }

  /**
   * Check if source exists then convert inner path plus source name to SourceFilePath.
   * @param path
   * @return
   * @throws SourceNotFoundException
   * @throws NamespaceException
   */
  private SourceFolderPath asFolderPath(String path) throws SourceNotFoundException, NamespaceException {
    sourceService.checkSourceExists(sourceName);
    return SourceFolderPath.fromURLPath(sourceName, path);
  }

  private SourceFilePath asFilePath(String path) throws SourceNotFoundException, NamespaceException {
    sourceService.checkSourceExists(sourceName);
    return SourceFilePath.fromURLPath(sourceName, path);
  }

  // format settings on a file.
  @GET
  @Path("/file_format/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public FileFormatUI getFileFormatSettings(@PathParam("path") String path)
      throws SourceNotFoundException, NamespaceException  {

    if (useFastPreview()) {
      SourceFilePath filePath = asFilePath(path);
      return new FileFormatUI(formatTools.getOrDetectFormat(filePath, DatasetType.PHYSICAL_DATASET_SOURCE_FILE), filePath);
    }

    sourceService.checkSourceExists(sourceName);
    SourceFilePath filePath = SourceFilePath.fromURLPath(sourceName, path);
    FileFormat fileFormat;
    try {
      final PhysicalDatasetConfig physicalDatasetConfig = sourceService.getFilesystemPhysicalDataset(sourceName, filePath);
      fileFormat = FileFormat.getForFile(physicalDatasetConfig.getFormatSettings());
      fileFormat.setVersion(physicalDatasetConfig.getTag());
    } catch (PhysicalDatasetNotFoundException nfe) {
      fileFormat = sourceService.getDefaultFileFormat(sourceName, filePath);
    }
    return new FileFormatUI(fileFormat, filePath);

  }

  @PUT
  @Path("/file_format/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public FileFormatUI saveFormatSettings(FileFormat fileFormat, @PathParam("path") String path)
      throws NamespaceException, SourceNotFoundException {
    SourceFilePath filePath = SourceFilePath.fromURLPath(sourceName, path);
    sourceService.checkSourceExists(filePath.getSourceName());
    fileFormat.setFullPath(filePath.toPathList());

    PhysicalDatasetConfig physicalDatasetConfig = new PhysicalDatasetConfig();
    physicalDatasetConfig.setName(filePath.getFileName().getName());
    physicalDatasetConfig.setFormatSettings(fileFormat.asFileConfig());
    physicalDatasetConfig.setTag(fileFormat.getVersion());
    physicalDatasetConfig.setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
    physicalDatasetConfig.setFullPathList(filePath.toPathList());
    sourceService.createPhysicalDataset(filePath, physicalDatasetConfig);
    fileFormat.setVersion(physicalDatasetConfig.getTag());
    return new FileFormatUI(fileFormat, filePath);
  }

  @POST
  @Path("/file_preview/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public JobDataFragment previewFileFormat(FileFormat format, @PathParam("path") String path)
      throws SourceFileNotFoundException, SourceNotFoundException, NamespaceException {
    if (useFastPreview()) {
      return formatTools.previewData(format, asFilePath(path), false);
    }
    SourceFilePath filePath = SourceFilePath.fromURLPath(sourceName, path);
    return executor.previewPhysicalDataset(filePath.toString(), format, getOrCreateAllocator("previewFileFormat"));
  }

  @POST
  @Path("/folder_preview/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public JobDataFragment previewFolderFormat(FileFormat format, @PathParam("path") String path)
    throws SourceFileNotFoundException, SourceNotFoundException, NamespaceException {
    if (useFastPreview()) {
      return formatTools.previewData(format, asFolderPath(path), false);
    }

    SourceFolderPath folderPath = SourceFolderPath.fromURLPath(sourceName, path);
    return executor.previewPhysicalDataset(folderPath.toString(), format, getOrCreateAllocator("previewFolderFormat"));
  }

  @DELETE
  @Path("/file_format/{path: .*}")
  public void deleteFileFormat(@PathParam("path") String path,
                               @QueryParam("version") String version) throws PhysicalDatasetNotFoundException {
    SourceFilePath filePath = SourceFilePath.fromURLPath(sourceName, path);
    if (version == null) {
      throw new ClientErrorException("missing version parameter");
    }

    try {
      sourceService.deletePhysicalDataset(sourceName, new PhysicalDatasetPath(filePath), version);
    } catch (ConcurrentModificationException e) {
      throw ResourceUtil.correctBadVersionErrorMessage(e, "file format", path);
    }
  }

  // format settings for folders.
  @GET
  @Path("/folder_format/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public FileFormatUI getFolderFormat(@PathParam("path") String path)
      throws PhysicalDatasetNotFoundException, NamespaceException, SourceNotFoundException, IOException {
    if (useFastPreview()) {
      SourceFolderPath folderPath = asFolderPath(path);
      return new FileFormatUI(formatTools.getOrDetectFormat(folderPath, DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER), folderPath);
    }

    SourceFolderPath folderPath = SourceFolderPath.fromURLPath(sourceName, path);
    sourceService.checkSourceExists(folderPath.getSourceName());

    FileFormat fileFormat;
    try {
      final PhysicalDatasetConfig physicalDatasetConfig = sourceService.getFilesystemPhysicalDataset(sourceName, folderPath);
      fileFormat = FileFormat.getForFolder(physicalDatasetConfig.getFormatSettings());
      fileFormat.setVersion(physicalDatasetConfig.getTag());
    } catch (PhysicalDatasetNotFoundException nfe) {
      fileFormat = sourceService.getDefaultFileFormat(sourceName, folderPath, securityContext.getUserPrincipal().getName());
    }
    return new FileFormatUI(fileFormat, folderPath);
  }

  @PUT
  @Path("/folder_format/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public FileFormatUI saveFolderFormat(FileFormat fileFormat, @PathParam("path") String path)
      throws NamespaceException, SourceNotFoundException {
    SourceFolderPath folderPath = SourceFolderPath.fromURLPath(sourceName, path);
    sourceService.checkSourceExists(folderPath.getSourceName());
    fileFormat.setFullPath(folderPath.toPathList());

    PhysicalDatasetConfig physicalDatasetConfig = new PhysicalDatasetConfig();
    physicalDatasetConfig.setName(folderPath.getFolderName().getName());
    physicalDatasetConfig.setFormatSettings(fileFormat.asFileConfig());
    physicalDatasetConfig.setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER);
    physicalDatasetConfig.setFullPathList(folderPath.toPathList());
    physicalDatasetConfig.setTag(fileFormat.getVersion());
    sourceService.createPhysicalDataset(folderPath, physicalDatasetConfig);
    fileFormat.setVersion(physicalDatasetConfig.getTag());
    return new FileFormatUI(fileFormat, folderPath);
  }

  @DELETE
  @Path("/folder_format/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public void deleteFolderFormat(@PathParam("path") String path,
                                 @QueryParam("version") String version) throws PhysicalDatasetNotFoundException {
    if (version == null) {
      throw new ClientErrorException("missing version parameter");
    }

    try {
      SourceFolderPath folderPath = SourceFolderPath.fromURLPath(sourceName, path);
      sourceService.deletePhysicalDataset(sourceName, new PhysicalDatasetPath(folderPath), version);
    } catch (ConcurrentModificationException e) {
      throw ResourceUtil.correctBadVersionErrorMessage(e, "folder format", path);
    }
  }

  @POST
  @Path("new_untitled_from_file/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public InitialPreviewResponse createUntitledFromSourceFile(
      @PathParam("path") String path,
      @QueryParam("limit") Integer limit)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    return datasetsResource.createUntitledFromSourceFile(sourceName, path, limit);
  }

  @POST
  @Path("new_untitled_from_folder/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public InitialPreviewResponse createUntitledFromSourceFolder(
      @PathParam("path") String path,
      @QueryParam("limit") Integer limit)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    return datasetsResource.createUntitledFromSourceFolder(sourceName, path, limit);
  }

  @POST
  @Path("new_untitled_from_physical_dataset/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public InitialPreviewResponse createUntitledFromPhysicalDataset(
      @PathParam("path") String path,
      @QueryParam("limit") Integer limit)
    throws DatasetNotFoundException, DatasetVersionNotFoundException, NamespaceException, NewDatasetQueryException {
    return datasetsResource.createUntitledFromPhysicalDataset(sourceName, path, limit);
  }

}
