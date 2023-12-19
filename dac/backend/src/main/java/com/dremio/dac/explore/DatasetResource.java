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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.io.IOException;
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

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.DatasetName;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.InitialDataPreviewResponse;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.explore.model.VersionContextReq.VersionContextType;
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
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.planner.sql.parser.ReferenceTypeUtils;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobSubmission;
import com.dremio.service.job.proto.SessionId;
import com.dremio.service.jobs.CompletionListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
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
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/dataset/{cpath}")
public class DatasetResource extends BaseResourceWithAllocator {

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

  @GET
  @Path("acceleration/settings")
  @Produces(APPLICATION_JSON)
  public AccelerationSettingsDescriptor getAccelerationSettings(
      @QueryParam("versionType") String versionType,
      @QueryParam("versionValue") String versionValue)
      throws DatasetNotFoundException, NamespaceException {
    final Catalog catalog = datasetService.getCatalog();
    final NamespaceKey namespaceKey = datasetPath.toNamespaceKey();
    final VersionContext versionContext = ReferenceTypeUtils.map(versionType, versionValue);
    final ResolvedVersionContext resolvedVersionContext = CatalogUtil.resolveVersionContext(catalog, namespaceKey.getRoot(), versionContext);
    final CatalogEntityKey catalogEntityKey = CatalogUtil.getCatalogEntityKey(
      namespaceKey,
      resolvedVersionContext,
      catalog);
    final DremioTable table = catalog.getTable(catalogEntityKey);

    if (table == null) {
      throw new DatasetNotFoundException(datasetPath);
    }

    final DatasetConfig config = table.getDatasetConfig();

    if (config.getType() == DatasetType.VIRTUAL_DATASET) {
      final String msg = String.format("acceleration settings apply only to physical dataset. %s is a virtual dataset",
          datasetPath.toPathString());
      throw new IllegalArgumentException(msg);
    }

    final ReflectionSettings reflectionSettings = reflectionServiceHelper.getReflectionSettings();
    final AccelerationSettings settings =
        reflectionSettings.getReflectionSettings(catalogEntityKey);

    final AccelerationSettingsDescriptor descriptor =
        new AccelerationSettingsDescriptor()
            .setAccelerationRefreshPeriod(settings.getRefreshPeriod())
            .setAccelerationGracePeriod(settings.getGracePeriod())
            .setAccelerationNeverExpire(settings.getNeverExpire())
            .setAccelerationNeverRefresh(settings.getNeverRefresh());

    if (reflectionServiceHelper.isIncrementalRefreshBySnapshotEnabled(config)) {
      descriptor.setMethod(RefreshMethod.AUTO);
    } else {
      // If RefreshMethod.AUTO has been saved and then support option is switched off, convert 'AUTO' to 'FULL'.
      if (settings.getMethod() == RefreshMethod.AUTO) {
        descriptor.setMethod(RefreshMethod.FULL);
      } else {
        descriptor.setMethod(settings.getMethod());
        descriptor.setRefreshField(settings.getRefreshField());
      }
    }

    final ByteString schemaBytes = DatasetHelper.getSchemaBytes(config);

    if (schemaBytes != null) {
      final BatchSchema schema = BatchSchema.deserialize(schemaBytes.toByteArray());
      descriptor.setFieldList(
          FluentIterable.from(schema)
              .transform(
                  new Function<Field, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable final Field field) {
                      return field.getName();
                    }
                  })
              .toList());
    }

    return descriptor;
  }

  @PUT
  @Path("acceleration/settings")
  @Produces(APPLICATION_JSON)
  public void updateAccelerationSettings(
      final AccelerationSettingsDescriptor descriptor,
      @QueryParam("versionType") String versionType,
      @QueryParam("versionValue") String versionValue)
      throws DatasetNotFoundException, NamespaceException {
    Preconditions.checkArgument(descriptor != null, "acceleration settings descriptor is required");
    Preconditions.checkArgument(descriptor.getAccelerationRefreshPeriod() != null, "refreshPeriod is required");
    Preconditions.checkArgument(descriptor.getAccelerationGracePeriod() != null, "gracePeriod is required");
    Preconditions.checkArgument(descriptor.getMethod() != null, "settings.method is required");
    Preconditions.checkArgument(descriptor.getAccelerationNeverExpire() //we are good here
      || descriptor.getAccelerationNeverRefresh() //user never want to refresh, assume they just want to let it expire anyway
      || descriptor.getAccelerationRefreshPeriod() <= descriptor.getAccelerationGracePeriod() , "refreshPeriod must be less than gracePeriod");


    final Catalog catalog = datasetService.getCatalog();
    final NamespaceKey namespaceKey = datasetPath.toNamespaceKey();
    final VersionContext versionContext = ReferenceTypeUtils.map(versionType, versionValue);
    final ResolvedVersionContext resolvedVersionContext = CatalogUtil.resolveVersionContext(catalog, namespaceKey.getRoot(), versionContext);
    final CatalogEntityKey catalogEntityKey = CatalogUtil.getCatalogEntityKey(
      namespaceKey,
      resolvedVersionContext,
      catalog);
    final DremioTable table = catalog.getTable(catalogEntityKey);

    if (table == null) {
      throw new DatasetNotFoundException(datasetPath);
    }

    final DatasetConfig config = table.getDatasetConfig();

    if (config.getType() == DatasetType.VIRTUAL_DATASET) {
      final String msg = String.format("acceleration settings apply only to physical dataset. %s is a virtual dataset",
          datasetPath.toPathString());
      throw new IllegalArgumentException(msg);
    }

    if (descriptor.getMethod() == RefreshMethod.INCREMENTAL) {
      if (CatalogUtil.requestedPluginSupportsVersionedTables(table.getPath(), catalog)) {
        // Validate Iceberg tables in Nessie Catalog
        final String msg = "refresh field is required for incremental updates on Iceberg tables";
        Preconditions.checkArgument(descriptor.getRefreshField() != null, msg);
      } else if (config.getType() == DatasetType.PHYSICAL_DATASET) {
        // Validate Iceberg tables outside of Nessie Catalog
        // Validate non-directory datasets such as RDBMS tables, MongoDB, elasticsearch, etc.
        final String msg = "refresh field is required for incremental updates on non-filesystem datasets";
        Preconditions.checkArgument(descriptor.getRefreshField() != null, msg);
      } else {
        Preconditions.checkArgument(Strings.isNullOrEmpty(descriptor.getRefreshField()), "leave refresh field empty for non-filesystem datasets");
      }
    }

    if (descriptor.getMethod() == RefreshMethod.FULL) {
      Preconditions.checkArgument(Strings.isNullOrEmpty(descriptor.getRefreshField()), "leave refresh field empty for full updates");
    }

    // Currently, setting of 'AUTO' method is effectively the same as 'FULL' (default) method.
    // For datasets that applicable, snapshot-based incremental refresh method is applied automatically, not rely on this setting.
    if (descriptor.getMethod() == RefreshMethod.AUTO) {
      Preconditions.checkArgument(Strings.isNullOrEmpty(descriptor.getRefreshField()), "Leave refresh field empty for 'AUTO' refresh method.");
    }

    final ReflectionSettings reflectionSettings = reflectionServiceHelper.getReflectionSettings();
    final AccelerationSettings settings =
        reflectionSettings.getReflectionSettings(catalogEntityKey);
    final AccelerationSettings descriptorSettings =
        new AccelerationSettings()
            .setAccelerationTTL(settings.getAccelerationTTL()) // needed to use protobuf equals
            .setTag(settings.getTag()) // needed to use protobuf equals
            .setRefreshPeriod(descriptor.getAccelerationRefreshPeriod())
            .setGracePeriod(descriptor.getAccelerationGracePeriod())
            .setMethod(descriptor.getMethod())
            .setRefreshField(descriptor.getRefreshField())
            .setNeverExpire(descriptor.getAccelerationNeverExpire())
            .setNeverRefresh(descriptor.getAccelerationNeverRefresh());

    if (settings.equals(descriptorSettings)) {
      return;
    }

    settings
        .setRefreshPeriod(descriptor.getAccelerationRefreshPeriod())
        .setGracePeriod(descriptor.getAccelerationGracePeriod())
        .setMethod(descriptor.getMethod())
        .setRefreshField(descriptor.getRefreshField())
        .setNeverExpire(descriptor.getAccelerationNeverExpire())
        .setNeverRefresh(descriptor.getAccelerationNeverRefresh());

    reflectionSettings.setReflectionSettings(catalogEntityKey, settings);
  }

  /**
   * returns the current version of the dataset
   * @return
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  @GET
  @Produces(APPLICATION_JSON)
  public DatasetUI getDataset() throws DatasetNotFoundException, NamespaceException {
    final VirtualDatasetUI virtualDataset = datasetService.get(datasetPath);
    return newDataset(virtualDataset);
  }

  @DELETE
  @Produces(APPLICATION_JSON)
  public DatasetUI deleteDataset(
      @QueryParam("savedTag") String savedTag,
      @QueryParam("refType") String refType,
      @QueryParam("refValue") String refValue)
      throws DatasetNotFoundException, UserNotFoundException, NamespaceException, IOException {
    final VersionContextReq versionContextReq = VersionContextReq.tryParse(refType, refValue);
    final boolean versioned = isDatasetVersioned();

    if (versioned && versionContextReq == null) {
      throw new ClientErrorException(
          "Missing a refType/refValue pair for versioned virtual dataset");
    } else if (!versioned && versionContextReq != null) {
      throw new ClientErrorException(
          "refType and refValue should be null for non-versioned virtual dataset");
    } else if (!versioned && savedTag == null) {
      throw new ClientErrorException("Missing savedTag parameter");
    } else if (versioned && versionContextReq.getType() != VersionContextType.BRANCH) {
      throw new ClientErrorException(
          "To delete a versioned virtual dataset the refType must be BRANCH");
    }

    DatasetUI datasetUI = null;
    if (versioned) {
      NamespaceKey namespaceKey = new NamespaceKey(datasetPath.toPathList());
      final Catalog catalog = datasetService.getCatalog();
      final ResolvedVersionContext resolvedVersionContext =
        CatalogUtil.resolveVersionContext(
          catalog, datasetPath.getRoot().getName(), VersionContext.ofBranch(refValue));
      DatasetType datasetType = catalog.getDatasetType(CatalogEntityKey.newBuilder()
        .keyComponents(datasetPath.toPathList())
        .tableVersionContext(TableVersionContext.of(resolvedVersionContext))
        .build());
      if (datasetType == DatasetType.VIRTUAL_DATASET) {
        catalog.validatePrivilege(namespaceKey, SqlGrant.Privilege.ALTER);
        final ViewOptions viewOptions =
            new ViewOptions.ViewOptionsBuilder().version(resolvedVersionContext).build();

        catalog.dropView(namespaceKey, viewOptions);
      } else if (datasetType == DatasetType.PHYSICAL_DATASET) {
        catalog.validatePrivilege(namespaceKey, SqlGrant.Privilege.DROP);
        final TableMutationOptions tableMutationOptions =
          TableMutationOptions.newBuilder()
            .setResolvedVersionContext(resolvedVersionContext)
            .build();

        catalog.dropTable(namespaceKey, tableMutationOptions);
      } else {
        throw new ClientErrorException("Dataset not found");
      }
    } else {
      try {
        final VirtualDatasetUI virtualDataset = datasetService.get(datasetPath);
        datasetUI = newDataset(virtualDataset);
        datasetService.deleteDataset(datasetPath, savedTag);
      } catch (DatasetVersionNotFoundException e) {
        datasetService.deleteDataset(datasetPath, null);
      }
    }

    final ReflectionSettings reflectionSettings = reflectionServiceHelper.getReflectionSettings();
    reflectionSettings.removeSettings(datasetPath.toNamespaceKey());

    return datasetUI;
  }

  private boolean isDatasetVersioned() {
    final NamespaceKey namespaceKey = datasetPath.toNamespaceKey();
    final Catalog catalog = datasetService.getCatalog();

    return CatalogUtil.requestedPluginSupportsVersionedTables(namespaceKey, catalog);
  }

  /**
   * Clones a virtual dataset with a new name. Caused when UI clicks "Copy new from Existing".
   *
   * @param existingDatasetPath
   * @return
   * @throws NamespaceException
   * @throws DatasetNotFoundException
   */
  @PUT
  @Path("copyFrom/{cpathFrom}")
  @Produces(APPLICATION_JSON)
  public DatasetUI createDatasetFrom(@PathParam("cpathFrom") DatasetPath existingDatasetPath)
    throws NamespaceException, DatasetNotFoundException, UserNotFoundException {
    final VirtualDatasetUI ds = datasetService.createDatasetFrom(existingDatasetPath, datasetPath, securityContext.getUserPrincipal().getName());

    String fromEntityId = namespaceService.getEntityIdByPath(existingDatasetPath.toNamespaceKey());
    String toEntityId = namespaceService.getEntityIdByPath(datasetPath.toNamespaceKey());
    collaborationService.copyWiki(fromEntityId, toEntityId);
    collaborationService.copyTags(fromEntityId, toEntityId);

    return newDataset(ds);
  }

  @POST @Path("/rename")
  @Produces(APPLICATION_JSON)
  public DatasetUI renameDataset(@QueryParam("renameTo") String renameTo)
    throws NamespaceException, DatasetNotFoundException, DatasetVersionNotFoundException {
    final DatasetPath newDatasetPath = datasetPath.rename(new DatasetName(renameTo));
    final VirtualDatasetUI ds = datasetService.renameDataset(datasetPath, newDatasetPath);

    return newDataset(ds);
  }

  @POST @Path("/moveTo/{newpath}")
  @Produces(APPLICATION_JSON)
  public DatasetUI moveDataset(@PathParam("newpath") DatasetPath newDatasetPath)
    throws NamespaceException, DatasetNotFoundException, DatasetVersionNotFoundException {
    final VirtualDatasetUI ds = datasetService.renameDataset(datasetPath, newDatasetPath);

    return newDataset(ds);
  }

  /**
   * Get the preview response of dataset. Dataset could be a physical or virtual dataset.
   * @param limit Maximum number of records in initial response.
   * @return
   */
  @GET
  @Path("preview")
  @Produces(APPLICATION_JSON)
  public InitialDataPreviewResponse preview(@QueryParam("limit") @DefaultValue("50") Integer limit) {
    final SqlQuery query = JobRequestUtil.createSqlQuery(String.format("select * from %s", datasetPath.toPathString()),
      securityContext.getUserPrincipal().getName());

    JobId jobId = null;
    SessionId sessionId = null;
    try {
      final CompletionListener completionListener = new CompletionListener();
      JobSubmission jobSubmission = jobsService.submitJob(SubmitJobRequest.newBuilder()
          .setSqlQuery(query)
          .setQueryType(QueryType.UI_PREVIEW)
          .build(), completionListener);
      jobId = jobSubmission.getJobId();
      sessionId = jobSubmission.getSessionId();

      completionListener.awaitUnchecked();
      final JobData jobData = new JobDataWrapper(jobsService, jobId, sessionId, query.getUsername());
      return InitialDataPreviewResponse.of(jobData.truncate(getOrCreateAllocator("preview"), limit));
    } catch(UserException e) {
      throw DatasetTool.toInvalidQueryException(e, query.getSql(), ImmutableList.of(), jobId, sessionId);
    }
  }

  protected DatasetUI newDataset(VirtualDatasetUI vds) throws NamespaceException {
    return DatasetUI.newInstance(vds, null, namespaceService);
  }
}
