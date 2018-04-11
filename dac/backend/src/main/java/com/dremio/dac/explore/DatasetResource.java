/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.util.Objects;

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
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
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
public class DatasetResource {
//  private static final Logger logger = LoggerFactory.getLogger(DatasetResource.class);

  private final DatasetVersionMutator datasetService;
  private final JobsService jobsService;
  private final SecurityContext securityContext;
  private final DatasetPath datasetPath;
  private final NamespaceService namespaceService;
  private ReflectionSettings reflectionSettings;
  private ReflectionServiceHelper reflectionServiceHelper;

  @Inject
  public DatasetResource(
    NamespaceService namespaceService,
    DatasetVersionMutator datasetService,
    JobsService jobsService,
    @Context SecurityContext securityContext,
    ReflectionServiceHelper reflectionServiceHelper,
    @PathParam("cpath") DatasetPath datasetPath) {
    this.datasetService = datasetService;
    this.namespaceService = namespaceService;
    this.jobsService = jobsService;
    this.securityContext = securityContext;
    this.datasetPath = datasetPath;
    this.reflectionSettings = reflectionServiceHelper.getReflectionSettings();
    this.reflectionServiceHelper = reflectionServiceHelper;
  }

  @GET
  @Path("descendants/count")
  @Produces(APPLICATION_JSON)
  public long getDescendantsCount() {
    return datasetService.getDescendantsCount(datasetPath.toNamespaceKey());
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
  public AccelerationSettingsDescriptor getAccelerationSettings() throws NamespaceException {
    final DatasetConfig config = namespaceService.getDataset(datasetPath.toNamespaceKey());
    if (config.getType() == DatasetType.VIRTUAL_DATASET) {
      final String msg = String.format("acceleration settings apply only to physical dataset. %s is a virtual dataset",
          datasetPath.toPathString());
      throw new IllegalArgumentException(msg);
    }

    final AccelerationSettings settings = reflectionSettings.getReflectionSettings(datasetPath.toNamespaceKey());
    final AccelerationSettingsDescriptor descriptor = new AccelerationSettingsDescriptor()
      .setAccelerationRefreshPeriod(settings.getRefreshPeriod())
      .setAccelerationGracePeriod(settings.getGracePeriod())
      .setMethod(settings.getMethod())
      .setRefreshField(settings.getRefreshField());

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

    final AccelerationSettings settings = reflectionSettings.getReflectionSettings(datasetPath.toNamespaceKey());
    final boolean settingsUpdated = !Objects.equals(settings.getRefreshPeriod(), descriptor.getAccelerationRefreshPeriod()) ||
      !Objects.equals(settings.getGracePeriod(), descriptor.getAccelerationGracePeriod()) ||
      !Objects.equals(settings.getMethod(), descriptor.getMethod()) ||
      !Objects.equals(settings.getRefreshField(), descriptor.getRefreshField());
    if (settingsUpdated) {
      settings.setRefreshPeriod(descriptor.getAccelerationRefreshPeriod())
        .setGracePeriod(descriptor.getAccelerationGracePeriod())
        .setMethod(descriptor.getMethod())
        .setRefreshField(descriptor.getRefreshField());
      reflectionSettings.setReflectionSettings(datasetPath.toNamespaceKey(), settings);
    }
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
  public DatasetUI deleteDataset(@QueryParam("savedVersion") Long savedVersion)
      throws DatasetNotFoundException, UserNotFoundException, NamespaceException {
    if (savedVersion == null) {
      throw new ClientErrorException("missing savedVersion parameter");
    }
    final VirtualDatasetUI virtualDataset = datasetService.get(datasetPath);

    DatasetUI datasetUI = newDataset(virtualDataset);

    datasetService.deleteDataset(datasetPath, savedVersion);
    reflectionSettings.removeSettings(datasetPath.toNamespaceKey());
    return datasetUI;
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
    final VirtualDatasetUI ds = datasetService.get(existingDatasetPath);
    // Set a new version, name.
    ds.setFullPathList(datasetPath.toPathList());
    ds.setVersion(DatasetVersion.newVersion());
    ds.setName(datasetPath.getDataset().getName());
    ds.setSavedVersion(null);
    ds.setId(null);
    datasetService.putVersion(ds);

    try {
      datasetService.put(ds);
    } catch(NamespaceNotFoundException nfe) {
      throw new ClientErrorException(format("Parent folder %s doesn't exist", existingDatasetPath.toParentPath()), nfe);
    }

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
    final SqlQuery query = new SqlQuery(
        String.format("select * from %s", datasetPath.toPathString()),
        securityContext.getUserPrincipal().getName());
    final JobUI job = new JobUI(jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(query)
        .setQueryType(QueryType.UI_PREVIEW)
        .build(), NoOpJobStatusListener.INSTANCE));
    try {
      return InitialDataPreviewResponse.of(job.getData().truncate(limit));
    } catch(UserException e) {
      throw DatasetTool.toInvalidQueryException(e, query.getSql(), ImmutableList.<String> of());
    }
  }

  protected DatasetUI newDataset(VirtualDatasetUI vds) throws NamespaceException {
    return DatasetUI.newInstance(vds, null);
  }
}
