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
package com.dremio.dac.explore;

import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.DatasetName;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.InitialDataPreviewResponse;
import com.dremio.dac.model.job.JobData;
import com.dremio.dac.model.job.JobDataWrapper;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.resource.BaseResourceWithAllocator;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.util.JobRequestUtil;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.CompletionListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.physicaldataset.proto.AccelerationSettingsDescriptor;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.service.users.UserNotFoundException;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;


/**
 * Serves the datasets
 *
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/dataset/{cpath}")
public class DatasetResource extends BaseResourceWithAllocator {
//  private static final Logger logger = LoggerFactory.getLogger(DatasetResource.class);

  private final DatasetVersionMutator datasetService;
  private final JobsService jobsService;
  private final SecurityContext securityContext;
  private final ReflectionServiceHelper reflectionServiceHelper;
  private final DatasetPath datasetPath;
  private final NamespaceService namespaceService;
  private final CollaborationHelper collaborationService;

  @Inject
  public DatasetResource(
    NamespaceService namespaceService,
    DatasetVersionMutator datasetService,
    JobsService jobsService,
    @Context SecurityContext securityContext,
    ReflectionServiceHelper reflectionServiceHelper,
    CollaborationHelper collaborationService,
    @PathParam("cpath") DatasetPath datasetPath,
    BufferAllocatorFactory allocatorFactory) {
    super(allocatorFactory);
    this.datasetService = datasetService;
    this.namespaceService = namespaceService;
    this.jobsService = jobsService;
    this.securityContext = securityContext;
    this.reflectionServiceHelper = reflectionServiceHelper;
    this.datasetPath = datasetPath;
    this.collaborationService = collaborationService;
  }

  /**
   * Defines the GET HTTP request responsible for getting the count of datasets depending on a given dataset.
   *
   * @return a JSON with the number of descendants datasets
   */
  @GET
  @Path("descendants/count")
  @Produces(APPLICATION_JSON)
  public long getDescendantsCount() {
    return datasetService.getDescendantsCount(datasetPath.toNamespaceKey());
  }

  /**
   * Defines the GET HTTP request responsible for getting a list of dataset paths depending on a given dataset.
   *
   * @return a JSON with a list with all datasets paths descending from the current dataset
   * @throws NamespaceException If the namespace for the current dataset can't be found
   */
  @GET
  @Path("descendants")
  @Produces(APPLICATION_JSON)
  public List<List<String>> getDescendants() throws NamespaceException {
    final List<List<String>> descendantPaths = Lists.newArrayList();
    for (DatasetPath path : datasetService.getDescendants(datasetPath)) {
      descendantPaths.add(path.toPathList());
    }
    return descendantPaths;
  }

  /**
   * Defines the GET HTTP request responsible for getting a given dataset's acceleration settings, such as its refresh period.
   *
   * @return a JSON with the acceleration settings
   * @throws NamespaceException If the namespace for the current dataset can't be found
   */
  @GET
  @Path("acceleration/settings")
  @Produces(APPLICATION_JSON)
  public AccelerationSettingsDescriptor getAccelerationSettings() throws NamespaceException {
    final DatasetConfig config = namespaceService.getDataset(datasetPath.toNamespaceKey());
    if (config.getType() == DatasetType.VIRTUAL_DATASET) {
      final String msg = String.format("acceleration settings apply only to physical dataset. %s is a virtual dataset",
          datasetPath.toPathString());
      throw new IllegalArgumentException(msg);
    }

    final ReflectionSettings reflectionSettings = reflectionServiceHelper.getReflectionSettings();
    final AccelerationSettings settings = reflectionSettings.getReflectionSettings(datasetPath.toNamespaceKey());
    final AccelerationSettingsDescriptor descriptor = new AccelerationSettingsDescriptor()
      .setAccelerationRefreshPeriod(settings.getRefreshPeriod())
      .setAccelerationGracePeriod(settings.getGracePeriod())
      .setMethod(settings.getMethod())
      .setRefreshField(settings.getRefreshField())
      .setAccelerationNeverExpire(settings.getNeverExpire())
      .setAccelerationNeverRefresh(settings.getNeverRefresh());

    final ByteString schemaBytes = DatasetHelper.getSchemaBytes(config);
    if (schemaBytes != null) {
      final BatchSchema schema = BatchSchema.deserialize(schemaBytes.toByteArray());
      descriptor.setFieldList(FluentIterable
          .from(schema)
          .transform(new Function<Field, String>() {
            @Nullable
            @Override
            public String apply(@Nullable final Field field) {
              return field.getName();
            }
          })
          .toList()
      );
    }
    return descriptor;
  }

  /**
   * Defines the PUT HTTP method to update the acceleration settings of a given dataset.
   *
   * @param descriptor a descriptor with the updated acceleration settings
   * @throws NamespaceException If the namespace for the current dataset can't be found
   */
  @PUT
  @Path("acceleration/settings")
  @Produces(APPLICATION_JSON)
  public void updateAccelerationSettings(final AccelerationSettingsDescriptor descriptor) throws NamespaceException {
    Preconditions.checkArgument(descriptor != null, "acceleration settings descriptor is required");
    Preconditions.checkArgument(descriptor.getAccelerationRefreshPeriod() != null, "refreshPeriod is required");
    Preconditions.checkArgument(descriptor.getAccelerationGracePeriod() != null, "gracePeriod is required");
    Preconditions.checkArgument(descriptor.getMethod() != null, "settings.method is required");

    final DatasetConfig config = namespaceService.getDataset(datasetPath.toNamespaceKey());

    if (config.getType() == DatasetType.VIRTUAL_DATASET) {
      final String msg = String.format("acceleration settings apply only to physical dataset. %s is a virtual dataset",
          datasetPath.toPathString());
      throw new IllegalArgumentException(msg);
    }

    if (descriptor.getMethod() == RefreshMethod.INCREMENTAL) {
      if (config.getType() == DatasetType.PHYSICAL_DATASET) {
        final String msg = "refresh field is required for incremental updates on non-filesystem datasets";
        Preconditions.checkArgument(descriptor.getRefreshField() != null, msg);
      } else {
        Preconditions.checkArgument(Strings.isNullOrEmpty(descriptor.getRefreshField()), "leave refresh field empty for non-filesystem datasets");
      }
    }

    if (descriptor.getMethod() == RefreshMethod.FULL) {
      Preconditions.checkArgument(Strings.isNullOrEmpty(descriptor.getRefreshField()), "leave refresh field empty for full updates");
    }

    final ReflectionSettings reflectionSettings = reflectionServiceHelper.getReflectionSettings();
    final AccelerationSettings settings = reflectionSettings.getReflectionSettings(datasetPath.toNamespaceKey());
    final AccelerationSettings descriptorSettings = new AccelerationSettings()
      .setAccelerationTTL(settings.getAccelerationTTL()) // needed to use protobuf equals
      .setTag(settings.getTag()) // needed to use protobuf equals
      .setRefreshPeriod(descriptor.getAccelerationRefreshPeriod())
      .setGracePeriod(descriptor.getAccelerationGracePeriod())
      .setMethod(descriptor.getMethod())
      .setRefreshField(descriptor.getRefreshField())
      .setNeverExpire(descriptor.getAccelerationNeverExpire())
      .setNeverRefresh(descriptor.getAccelerationNeverRefresh());
    final boolean settingsUpdated = !settings.equals(descriptorSettings);
    if (settingsUpdated) {
      settings.setRefreshPeriod(descriptor.getAccelerationRefreshPeriod())
        .setGracePeriod(descriptor.getAccelerationGracePeriod())
        .setMethod(descriptor.getMethod())
        .setRefreshField(descriptor.getRefreshField())
        .setNeverExpire(descriptor.getAccelerationNeverExpire())
        .setNeverRefresh(descriptor.getAccelerationNeverRefresh());
      reflectionSettings.setReflectionSettings(datasetPath.toNamespaceKey(), settings);
    }
  }

  /**
   * Defines the GET HTTP method to get a given dataset's minimal info needed by UI.
   *
   * @return the minimal info about a given dataset
   * @throws DatasetNotFoundException If it couldn't found the needed dataset
   * @throws NamespaceException       If it couldn't found the dataset's namespace
   */
  @GET
  @Produces(APPLICATION_JSON)
  public DatasetUI getDataset() throws DatasetNotFoundException, NamespaceException {
    final VirtualDatasetUI virtualDataset = datasetService.get(datasetPath);
    return newDataset(virtualDataset);
  }

  /**
   * Defines the DELETE HTTP method to delete the current dataset.
   *
   * @param savedTag the dataset's entity version to be deleted
   * @return         the minimal info about the deleted dataset
   * @throws DatasetNotFoundException If it couldn't found the needed dataset
   * @throws UserNotFoundException    If it couldn't found the user that owns the dataset
   * @throws NamespaceException       If it couldn't found the dataset's namespace
   */
  @DELETE
  @Produces(APPLICATION_JSON)
  public DatasetUI deleteDataset(@QueryParam("savedTag") String savedTag)
      throws DatasetNotFoundException, UserNotFoundException, NamespaceException {
    if (savedTag == null) {
      throw new ClientErrorException("missing savedTag parameter");
    }
    final VirtualDatasetUI virtualDataset = datasetService.get(datasetPath);

    DatasetUI datasetUI = newDataset(virtualDataset);

    datasetService.deleteDataset(datasetPath, savedTag);
    final ReflectionSettings reflectionSettings = reflectionServiceHelper.getReflectionSettings();
    reflectionSettings.removeSettings(datasetPath.toNamespaceKey());
    return datasetUI;
  }

  /**
   * Defines the PUT HTTP method to create a virtual dataset from the current one, but with a different name.
   * <p>
   * Caused when UI clicks "Copy new from Existing".
   *
   * @param existingDatasetPath the existing dataset path to be cloned with a different name
   * @return                    the minimal info about the created dataset
   * @throws DatasetNotFoundException If it couldn't found the needed dataset
   * @throws UserNotFoundException    If it couldn't found the user that owns the dataset
   * @throws NamespaceException       If it couldn't found the dataset's namespace
   */
  @PUT
  @Path("copyFrom/{cpathFrom}")
  @Produces(APPLICATION_JSON)
  public DatasetUI createDatasetFrom(@PathParam("cpathFrom") DatasetPath existingDatasetPath)
    throws NamespaceException, DatasetNotFoundException, UserNotFoundException {
    final VirtualDatasetUI ds = datasetService.get(existingDatasetPath);
    // Set a new version, name.
    ds.setFullPathList(datasetPath.toPathList());
    ds.setVersion(DatasetVersion.newVersion());
    ds.setName(datasetPath.getLeaf().getName());
    ds.setSavedTag(null);
    ds.setId(null);
    ds.setPreviousVersion(null);
    ds.setOwner(securityContext.getUserPrincipal().getName());
    datasetService.putVersion(ds);

    try {
      datasetService.put(ds);
    } catch(NamespaceNotFoundException nfe) {
      throw new ClientErrorException(format("Parent folder %s doesn't exist", existingDatasetPath.toParentPath()), nfe);
    }

    String fromEntityId = namespaceService.getEntityIdByPath(existingDatasetPath.toNamespaceKey());
    String toEntityId = namespaceService.getEntityIdByPath(datasetPath.toNamespaceKey());
    collaborationService.copyWiki(fromEntityId, toEntityId);
    collaborationService.copyTags(fromEntityId, toEntityId);

    return newDataset(ds);
  }

  /**
   * Defines the POST HTTP method to rename a given dataset.
   *
   * @param renameTo the new name for the dataset
   * @return         an object with all the dataset information needed on the UI
   * @throws NamespaceException              If the renamed dataset has an incorrect path
   * @throws DatasetNotFoundException        If the given dataset path is not found
   * @throws DatasetVersionNotFoundException If the version is not found for a given dataset
   */
  @POST @Path("/rename")
  @Produces(APPLICATION_JSON)
  public DatasetUI renameDataset(@QueryParam("renameTo") String renameTo)
    throws NamespaceException, DatasetNotFoundException, DatasetVersionNotFoundException {
    final DatasetPath newDatasetPath = datasetPath.rename(new DatasetName(renameTo));
    final VirtualDatasetUI ds = datasetService.renameDataset(datasetPath, newDatasetPath);

    return newDataset(ds);
  }

  /**
   * Defines the POST HTTP method to move a dataset to a new dataset path.
   *
   * @param newDatasetPath a dataset path
   * @return               an object with all the dataset information needed on the UI
   * @throws NamespaceException              If the {@code newDatasetPath} provided contains an incorrect path
   * @throws DatasetNotFoundException        If the given dataset path is not found
   * @throws DatasetVersionNotFoundException If the version is not found for a given dataset
   */
  @POST @Path("/moveTo/{newpath}")
  @Produces(APPLICATION_JSON)
  public DatasetUI moveDataset(@PathParam("newpath") DatasetPath newDatasetPath)
    throws NamespaceException, DatasetNotFoundException, DatasetVersionNotFoundException {
    final VirtualDatasetUI ds = datasetService.renameDataset(datasetPath, newDatasetPath);

    return newDataset(ds);
  }

  /**
   * Defines the GET HTTP method to retrieve the preview response of a dataset.
   * <p>
   * Dataset could be a physical or virtual dataset.
   *
   * @param limit the maximum number of records on the initial response
   * @return      a dataset preview response containing initial data and pagination URL to fetch the remaining data
   */
  @GET
  @Path("preview")
  @Produces(APPLICATION_JSON)
  public InitialDataPreviewResponse preview(@QueryParam("limit") @DefaultValue("50") Integer limit) {
    final SqlQuery query = JobRequestUtil.createSqlQuery(String.format("select * from %s", datasetPath.toPathString()),
      securityContext.getUserPrincipal().getName());

    try {
      final CompletionListener completionListener = new CompletionListener();
      final JobId jobId = jobsService.submitJob(SubmitJobRequest.newBuilder()
          .setSqlQuery(query)
          .setQueryType(QueryType.UI_PREVIEW)
          .build(), completionListener);
      completionListener.awaitUnchecked();
      final JobData jobData = new JobDataWrapper(jobsService, jobId, query.getUsername());
      return InitialDataPreviewResponse.of(jobData.truncate(getOrCreateAllocator("preview"), limit));
    } catch(UserException e) {
      throw DatasetTool.toInvalidQueryException(e, query.getSql(), ImmutableList.of());
    }
  }

  /**
   * Creates an object with all the dataset information needed on the UI.
   *
   * @param vds the virtual dataset UI object
   * @return    a DatasetUI object containing the dataset information needed on the UI
   * @throws NamespaceException If the {@code vds} provided contains an incorrect dataset path
   */
  protected DatasetUI newDataset(VirtualDatasetUI vds) throws NamespaceException {
    return DatasetUI.newInstance(vds, null, namespaceService);
  }
}
