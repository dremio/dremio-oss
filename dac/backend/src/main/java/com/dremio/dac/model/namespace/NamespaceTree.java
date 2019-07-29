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
package com.dremio.dac.model.namespace;

import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SOURCE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dremio.dac.explore.model.Dataset;
import com.dremio.dac.explore.model.DatasetName;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetResourcePath;
import com.dremio.dac.explore.model.DatasetVersionResourcePath;
import com.dremio.dac.model.common.DACRuntimeException;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.folder.Folder;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.sources.PhysicalDataset;
import com.dremio.dac.model.sources.PhysicalDatasetName;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.sources.PhysicalDatasetResourcePath;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.collaboration.TagsSearchResult;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.file.File;
import com.dremio.file.FilePath;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceUtils;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.physicaldataset.proto.PhysicalDatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.google.common.base.Preconditions;

/**
 * Full/Partial representation of a namespace.
 */
public class NamespaceTree {

  private static DatasetVersionMutator datasetService;
  private static List<NameSpaceContainer> children;
  private static Type rootEntityType;
  private static CollaborationHelper collaborationService;
  // TODO For now we only implement list (single level lookups)
  private final List<Folder> folders;
  private final List<Dataset> datasets;
  private final List<File> files;
  private final List<PhysicalDataset> physicalDatasets;

  private boolean canTagsBeSkipped;

  public NamespaceTree() {
    folders = new ArrayList<>();
    datasets = new ArrayList<>();
    files = new ArrayList<>();
    physicalDatasets = new ArrayList<>();
    canTagsBeSkipped = false;
  }

  // Spaces, home and sources are top level folders hence can never show in children.
  public static NamespaceTree newInstance(
      final DatasetVersionMutator datasetService,
      List<NameSpaceContainer> children,
      Type rootEntityType,
      CollaborationHelper collaborationService) throws NamespaceException, DatasetNotFoundException {
    NamespaceTree result = new NamespaceTree();

    populateInstance(result, datasetService, children, rootEntityType, collaborationService);

    return result;
  }

  protected static void populateInstance(
      NamespaceTree tree,
      DatasetVersionMutator datasetService,
      List<NameSpaceContainer> children,
      Type rootEntityType,
      CollaborationHelper collaborationService)
      throws NamespaceException, DatasetNotFoundException {

    // get a list of all ids so we can fetch all collaboration tags in one search
    final Map<String, CollaborationTag> tags = new HashMap<>();
    if (collaborationService != null) {
      TagsSearchResult tagsInfo = collaborationService.getTagsForIds(children.stream().
        map(NamespaceUtils::getId).collect(Collectors.toSet()));

      tags.putAll(tagsInfo.getTags());
      tree.setCanTagsBeSkipped(tagsInfo.getCanTagsBeSkipped());
    }

    for (final NameSpaceContainer container: children) {
      switch (container.getType()) {
        case FOLDER: {
          if (rootEntityType == SOURCE) {
            tree.addFolder(new SourceFolderPath(container.getFullPathList()), container.getFolder(), null, rootEntityType);
          } else {
            tree.addFolder(new FolderPath(container.getFullPathList()), container.getFolder(), rootEntityType);
          }
        }
        break;

        case DATASET: {
          final DatasetPath datasetPath = new DatasetPath(container.getFullPathList());
          final DatasetConfig datasetConfig = container.getDataset();
          switch (datasetConfig.getType()) {
            case VIRTUAL_DATASET:
              Preconditions.checkArgument(rootEntityType != SOURCE);
              final VirtualDatasetUI vds = datasetService.get(datasetPath, datasetConfig.getVirtualDataset().getVersion());
              tree.addDataset(
                new DatasetResourcePath(datasetPath),
                new DatasetVersionResourcePath(datasetPath, vds.getVersion()),
                datasetPath.getDataset(),
                vds.getSql(),
                vds,
                datasetService.getJobsCount(datasetPath.toNamespaceKey()),
                rootEntityType,
                tags.get(datasetConfig.getId().getId())
              );
              break;

            case PHYSICAL_DATASET_HOME_FILE:
              final String fileDSId = container.getDataset().getId().getId();
              final FileFormat fileFormat = FileFormat.getForFile(DatasetsUtil.toFileConfig(container.getDataset()));
              tree.addFile(
                fileDSId,
                new FilePath(container.getFullPathList()),
                fileFormat,
                datasetService.getJobsCount(datasetPath.toNamespaceKey()), false, true,
                fileFormat.getFileType() != FileType.UNKNOWN, datasetConfig.getType(),
                tags.get(fileDSId)
              );
              break;

            case PHYSICAL_DATASET_SOURCE_FILE:
            case PHYSICAL_DATASET_SOURCE_FOLDER:
            case PHYSICAL_DATASET:
              PhysicalDatasetPath path = new PhysicalDatasetPath(datasetConfig.getFullPathList());
              tree.addPhysicalDataset(
                new PhysicalDatasetResourcePath(new SourceName(container.getFullPathList().get(0)), path),
                new PhysicalDatasetName(path.getFileName().getName()),
                DatasetsUtil.toPhysicalDatasetConfig(container.getDataset()),
                datasetService.getJobsCount(datasetPath.toNamespaceKey()),
                tags.get(container.getDataset().getId().getId())
              );
              break;

            default:
              throw new DACRuntimeException("Possible corruption found. Invalid types in namespace tree " + children);
          }
        }
        break;

        default:
          throw new DACRuntimeException("Possible corruption found. Invalid types in namespace tree " + container.getType());
      }
    }
  }

  public void addFolder(final Folder f) {
    folders.add(f);
  }

  public void addFolder(SourceFolderPath folderPath, FolderConfig folderConfig, FileFormat fileFormat, NameSpaceContainer.Type rootEntityType) throws NamespaceNotFoundException {
    Folder folder = Folder.newInstance(folderPath, folderConfig, fileFormat, null, false, false);
    addFolder(folder);
  }


  public void addFolder(FolderPath folderPath, FolderConfig folderConfig, NameSpaceContainer.Type rootEntityType) throws NamespaceNotFoundException {
    Folder folder = Folder.newInstance(folderPath, folderConfig, null, false, false);
    addFolder(folder);
  }

  public void addFile(final File f) {
    files.add(f);
  }

  protected void addFile(String id, NamespacePath filePath, FileFormat fileFormat, Integer jobCount,
      boolean isStaged, boolean isHomeFile, boolean isQueryable, DatasetType datasetType, CollaborationTag collaborationTag) {
    final File file = File.newInstance(
        id,
        filePath,
        fileFormat,
        jobCount,
        isStaged, isHomeFile, isQueryable, getTags(collaborationTag)
      );
      addFile(file);
  }

  public void addDataset(final Dataset ds) {
    datasets.add(ds);
  }

  protected void addDataset(DatasetResourcePath resourcePath,
      DatasetVersionResourcePath versionedResourcePath,
      DatasetName datasetName,
      String sql,
      VirtualDatasetUI datasetConfig,
      int jobCount, NameSpaceContainer.Type rootEntityType,
      CollaborationTag collaborationTag
  ) throws NamespaceNotFoundException {
    Dataset dataset = Dataset.newInstance(resourcePath, versionedResourcePath, datasetName, sql, datasetConfig, jobCount, getTags(collaborationTag));

    addDataset(dataset);
  }

  protected List<String> getTags(CollaborationTag collaborationTag) {
    return null == collaborationTag ? null : collaborationTag.getTagsList();
  }

  public void addPhysicalDataset(final PhysicalDataset rds) {
    physicalDatasets.add(rds);
  }

  protected void addPhysicalDataset(
      PhysicalDatasetResourcePath resourcePath,
      PhysicalDatasetName datasetName,
      PhysicalDatasetConfig datasetConfig,
      Integer jobCount,
      CollaborationTag collaborationTag) throws NamespaceNotFoundException {

    PhysicalDataset physicalDataset = new PhysicalDataset(resourcePath, datasetName, datasetConfig, jobCount, getTags(collaborationTag));

    addPhysicalDataset(physicalDataset);
  }

  public final List<Folder> getFolders() {
    return folders;
  }

  public final List<Dataset> getDatasets() {
    return datasets;
  }

  public List<PhysicalDataset> getPhysicalDatasets() {
    return physicalDatasets;
  }

  public final List<File> getFiles() {
    return files;
  }

  public boolean getCanTagsBeSkipped() {
    return canTagsBeSkipped;
  }

  public void setCanTagsBeSkipped(boolean canTagsBeSkipped) {
    this.canTagsBeSkipped = canTagsBeSkipped;
  }
}
