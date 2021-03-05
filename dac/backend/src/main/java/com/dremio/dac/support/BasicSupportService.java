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
package com.dremio.dac.support;

import static com.dremio.common.util.DremioVersionInfo.VERSION;
import static com.dremio.dac.util.ClusterVersionUtils.toClusterVersion;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.inject.Provider;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.util.DremioEdition;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.common.util.JodaDateUtility;
import com.dremio.common.utils.ProtobufUtils;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.common.utils.SqlUtils;
import com.dremio.config.DremioConfig;
import com.dremio.dac.proto.model.source.ClusterIdentity;
import com.dremio.dac.proto.model.source.ClusterInfo;
import com.dremio.dac.proto.model.source.ClusterVersion;
import com.dremio.dac.proto.model.source.Node;
import com.dremio.dac.proto.model.source.SoftwareVersion;
import com.dremio.dac.proto.model.source.Source;
import com.dremio.dac.proto.model.source.Submission;
import com.dremio.dac.proto.model.source.SupportHeader;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.service.datasets.DatasetDownloadManager.DownloadDataResponse;
import com.dremio.dac.service.support.SupportRPC;
import com.dremio.dac.service.support.SupportRPC.ClusterIdentityRequest;
import com.dremio.dac.service.support.SupportRPC.ClusterIdentityResponse;
import com.dremio.dac.util.JobRequestUtil;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.options.OptionManager;
import com.dremio.service.Pointer;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.CompletionListener;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.services.configuration.ConfigurationStore;
import com.dremio.services.configuration.proto.ConfigurationEntry;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.services.fabric.simple.AbstractReceiveHandler;
import com.dremio.services.fabric.simple.ProtocolBuilder;
import com.dremio.services.fabric.simple.SendEndpointCreator;
import com.dremio.services.fabric.simple.SentResponseMessage;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.net.MediaType;

import io.protostuff.ByteString;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;

/**
 * Basic implementation of SupportService
 */
public class BasicSupportService implements SupportService {
  private static final Logger logger = LoggerFactory.getLogger(BasicSupportService.class);
  private static final int TYPE_SUPPORT_CLUSTERID = 1;

  private static final int PRE_TIME_BUFFER_MS = 5 * 1000;
  private static final int POST_TIME_BUFFER_MS = 10 * 1000;

  private final DACConfig config;
  private final Provider<LegacyKVStoreProvider> kvStoreProvider;
  private final Provider<JobsService> jobsService;
  private final Provider<UserService> userService;
  private final Provider<ClusterCoordinator> clusterCoordinatorProvider;
  private final Provider<OptionManager> optionManagerProvider;
  private final Provider<NamespaceService> namespaceServiceProvider;
  private final Provider<CatalogService> catalogServiceProvider;
  private final Provider<FabricService> fabricServiceProvider;
  private Path supportPath;
  private ConfigurationStore store;
  private ClusterIdentity identity;
  private BufferAllocator allocator;

  private SendEndpointCreator<ClusterIdentityRequest, ClusterIdentityResponse> getClusterIdentityEndpointCreator; // used on server and client side

  public BasicSupportService(
    DACConfig config,
    Provider<LegacyKVStoreProvider> kvStoreProvider,
    Provider<JobsService> jobsService,
    Provider<UserService> userService,
    Provider<ClusterCoordinator> clusterCoordinatorProvider,
    Provider<OptionManager> optionManagerProvider,
    Provider<NamespaceService> namespaceServiceProvider,
    Provider<CatalogService> catalogServiceProvider,
    Provider<FabricService> fabricServiceProvider,
    BufferAllocator allocator
  ) {
    this.kvStoreProvider = kvStoreProvider;
    this.jobsService = jobsService;
    this.userService = userService;
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
    this.optionManagerProvider = optionManagerProvider;
    this.namespaceServiceProvider = namespaceServiceProvider;
    this.catalogServiceProvider = catalogServiceProvider;
    this.fabricServiceProvider = fabricServiceProvider;
    this.allocator = allocator;
    this.config = config;
  }

  /**
   * Acquires the current cluster identity.
   * <p>
   * The cluster identity is used to identify that a executor node and a
   * master node are at the same cluster.
   *
   * @return the acquired cluster ID.
   */
  @Override
  public ClusterIdentity getClusterId() {
    return identity;
  }

  /**
   * Gets a configuration entry.
   *
   * @param key a key associated with the configuration value
   * @return    the configuration value associated with the given key
   */
  @Override
  public ConfigurationEntry getConfigurationEntry(String key) {
    return store.get(key);
  }

  /**
   * Sets a configuration entry.
   *
   * @param key   a key associated with the configuration entry
   * @param entry the configuration entry to be set
   */
  @Override
  public void setConfigurationEntry(String key, ConfigurationEntry entry) {
    store.put(key, entry);
  }

  /**
   * Stores the current DremioEdition (Marketplace, Enterprise or Community) in the Configuration Store.
   */
  protected void storeDremioEdition() {
    try {
      final ConfigurationEntry entry = new ConfigurationEntry();
      entry.setValue(ByteString.copyFrom(DremioEdition.getAsString().getBytes()));
      store.put(DREMIO_EDITION, entry);
    } catch (ConcurrentModificationException ex) {
      //nothing to do
      logger.info("Edition has already been entered in KV store.");
    }
  }

  /**
   * Registers a cluster identity in the current Configuration KVStore.
   * <p>
   * The cluster identity is used to identify that a executor node
   * and a master node are at the same cluster.
   * <p>
   * In case another server already have stored it, retrieves the stored identity.
   *
   * @param identity the cluster identity to be stored
   * @return         the cluster identity stored in the Configuration KVStore
   * @throws IllegalStateException If failed to retrieve or create the cluster identity
   */
  private ClusterIdentity storeIdentity(ClusterIdentity identity) {
    storeDremioEdition();
    try {
      ConfigurationEntry entry = new ConfigurationEntry();
      entry.setType(CLUSTER_IDENTITY);
      entry.setValue(convertClusterIdentityToByteString(identity));

      store.put(CLUSTER_ID, entry);
      logger.info("New Cluster Identifier Generated: {}", identity.getIdentity());
    } catch (ConcurrentModificationException ex) {
      // someone else inserted the new cluster identifier before we were able to.
      ConfigurationEntry entry = store.get(CLUSTER_ID);
      ProtostuffIOUtil.mergeFrom(entry.getValue().toByteArray(), identity, ClusterIdentity.getSchema());

      if (identity == null) {
        throw new IllegalStateException("Failed to retrieve or create cluster identity but identity is also not available.", ex);
      }
    }

    return identity;
  }

  /**
   * Acquires the current cluster identity by a RPC request.
   * <p> 
   * The RPC request returns all the metadata necessary
   * to build the cluster identity, such as its version and ID itself.
   *
   * @return the acquired cluster ID
   * @throws RpcException If it was unable to fetch the cluster identity by the RPC request
   * @throws Exception    If any other exception occurs
   */
  private ClusterIdentity getClusterIdentityFromRPC() throws Exception {
    final ClusterIdentityRequest.Builder requestBuilder = ClusterIdentityRequest.newBuilder();
    final ClusterIdentityResponse response;
    ClusterIdentity id;
    try {
      Collection<NodeEndpoint> coordinators = clusterCoordinatorProvider.get().getServiceSet(Role.COORDINATOR).getAvailableEndpoints();
      if (coordinators.isEmpty()) {
        throw new RpcException("Unable to fetch Cluster Identity, no endpoints are available");
      }

      NodeEndpoint ep = coordinators.iterator().next();
      if (ep == null) {
        throw new RpcException("Unable to fetch Cluster Identity, endpoint is down");
      }

      response = getClusterIdentityEndpointCreator
        .getEndpoint(ep.getAddress(), ep.getFabricPort())
        .send(requestBuilder.build())
        .getBody();

      ClusterVersion version = new ClusterVersion();
      version.setMajor(response.getVersion().getMajor());
      version.setMinor(response.getVersion().getMinor());
      version.setPatch(response.getVersion().getPatch());
      version.setBuildNumber(response.getVersion().getBuildNumber());
      version.setQualifier(response.getVersion().getQualifier());

      id = new ClusterIdentity();
      id.setCreated(response.getCreated());
      id.setIdentity(response.getIdentity());
      id.setVersion(version);

      if (response.hasTag()) {
        id.setTag(response.getTag());
      }

      if (response.hasSerial()) {
        id.setSerial(response.getSerial());
      }
    } catch (Exception e) {
      throw e;
    }

    return id;
  }

  /**
   * Acquires the current cluster identity from a given KVStore.
   * <p> 
   * The cluster identity is used to identify
   * that a executor node and a master node are at the same cluster.
   *
   * @param provider the KVStore to acquire the cluster identity
   * @return         the cluster identity, or null if it failed to get the cluster ID
   */
  public static Optional<ClusterIdentity> getClusterIdentity(LegacyKVStoreProvider provider) {
    ConfigurationStore store = new ConfigurationStore(provider);
    return getClusterIdentityFromStore(store, provider);
  }

  /**
   * Acquires the current cluster identity from a given Configuration Store.
   * <p> 
   * The cluster identity is used to identify
   * that a executor node and a master node are at the same cluster.
   *
   * @param store    the configuration store to get the identity
   * @param provider a KVStore that provided the configuration store
   * @return         the cluster identity acquired
   */
  private static Optional<ClusterIdentity> getClusterIdentityFromStore(ConfigurationStore store, LegacyKVStoreProvider provider) {
    final ConfigurationEntry entry = store.get(SupportService.CLUSTER_ID);

    if (entry == null) {
      Optional<ClusterIdentity> upgradedClusterIdentity = upgradeToNewSupportStore(provider);
      return upgradedClusterIdentity;
    }

    try {
      ClusterIdentity identity = ClusterIdentity.getSchema().newMessage();
      ProtostuffIOUtil.mergeFrom(entry.getValue().toByteArray(), identity, ClusterIdentity.getSchema());
      return Optional.ofNullable(identity);
    } catch (Exception e) {
      logger.info("failed to get cluster identity", e);
      return Optional.empty();
    }
  }

  /**
   * Migrates an old support store cluster identity to the new store.
   *
   * @param provider a key-value store provider to be migrated
   * @return         the migrated cluster identity
   */
  private static Optional<ClusterIdentity> upgradeToNewSupportStore(LegacyKVStoreProvider provider) {
    final LegacyKVStore<String, ClusterIdentity> oldSupportStore = provider.getStore(OldSupportStoreCreator.class);
    ClusterIdentity clusterIdentity = oldSupportStore.get(CLUSTER_ID);

    if (clusterIdentity != null) {
      // we found an old support store cluster identity, migrate it to the new store
      updateClusterIdentity(provider, clusterIdentity);
    }

    return Optional.ofNullable(clusterIdentity);
  }

  /**
   * Old support creator - used for upgrade
   */
  public static class OldSupportStoreCreator implements LegacyKVStoreCreationFunction<String, ClusterIdentity> {
    @Override
    public LegacyKVStore<String, ClusterIdentity> build(LegacyStoreBuildingFactory factory) {
      return factory.<String, ClusterIdentity>newStore()
        .name("identity")
        .keyFormat(Format.ofString())
        .valueFormat(Format.ofProtostuff(ClusterIdentity.class))
        .versionExtractor(ClusterIdentityVersionExtractor.class)
        .build();
    }
  }

  /**
   * Version extractor used for dealing with cluster identity.
   */
  public static class ClusterIdentityVersionExtractor implements VersionExtractor<ClusterIdentity> {
    @Override
    public Long getVersion(ClusterIdentity value) {
      return value.getSerial();
    }

    @Override
    public void setVersion(ClusterIdentity value, Long version) {
      value.setSerial(version);
    }

    @Override
    public String getTag(ClusterIdentity value) {
      return value.getTag();
    }

    @Override
    public void setTag(ClusterIdentity value, String tag) {
      value.setTag(tag);
    }
  }

  /**
   * Updates a cluster identity's configuration entry.
   *
   * @param provider the KVStore to acquire the identity's support store
   * @param identity the cluster identity to be updated
   */
  public static void updateClusterIdentity(LegacyKVStoreProvider provider, ClusterIdentity identity) {
    final LegacyKVStore<String, ConfigurationEntry> supportStore = provider.getStore(ConfigurationStore.ConfigurationStoreCreator.class);

    final ConfigurationEntry entry = new ConfigurationEntry();
    entry.setType(CLUSTER_IDENTITY);

    ConfigurationEntry existingSupportEntry = supportStore.get(CLUSTER_ID);
    if (existingSupportEntry != null) {
      entry.setTag(existingSupportEntry.getTag());
    }

    entry.setValue(convertClusterIdentityToByteString(identity));
    supportStore.put(CLUSTER_ID, entry);
  }

  /**
   * Converts a cluster identity object to a ByteString object.
   * <p> 
   * The ByteString will be used to register
   * the value of a configuration entry.
   *
   * @param identity the cluster identity to be converted
   * @return         the cluster identity in ByteString format
   */
  private static ByteString convertClusterIdentityToByteString(ClusterIdentity identity) {
    final LinkedBuffer buffer = LinkedBuffer.allocate();
    byte[] bytes = ProtostuffIOUtil.toByteArray(identity, ClusterIdentity.getSchema(), buffer);
    return ByteString.copyFrom(bytes);
  }

  /**
   * Registers a endpoint to retrieve the cluster identities.
   * <p> 
   * Used by {@link BasicSupportService#getClusterIdentityFromRPC}
   * to get the cluster's identity.
   */
  private void registerClusterIdentityEndpoint() {

    final ProtocolBuilder builder = ProtocolBuilder.builder()
      .protocolId(59) // TODO: DX-14395: consolidate protocol ids
      .allocator(allocator)
      .name("support-rpc")
      .timeout(10 * 1000);

    this.getClusterIdentityEndpointCreator = builder.register(TYPE_SUPPORT_CLUSTERID,
      new AbstractReceiveHandler<ClusterIdentityRequest, ClusterIdentityResponse>(
        ClusterIdentityRequest.getDefaultInstance(), ClusterIdentityResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<ClusterIdentityResponse> handle(ClusterIdentityRequest getIdRequest, ArrowBuf dBody) {

          SupportRPC.ClusterVersion version =
            SupportRPC.ClusterVersion.newBuilder()
              .setMajor(identity.getVersion().getMajor())
              .setMinor(identity.getVersion().getMinor())
              .setPatch(identity.getVersion().getPatch())
              .setBuildNumber(identity.getVersion().getBuildNumber())
              .setQualifier(identity.getVersion().getQualifier())
              .build();

          SupportRPC.ClusterIdentityResponse.Builder response = ClusterIdentityResponse.newBuilder();
          response.setCreated(identity.getCreated());
          response.setIdentity(identity.getIdentity());
          response.setVersion(version);
          if (identity.getTag() != null) {
            response.setTag(identity.getTag());
          }
          if (identity.getSerial() != null) {
            response.setSerial(identity.getSerial());
          }

          return new SentResponseMessage<>(response.build());
        }
      });

    builder.register(fabricServiceProvider.get());
  }

  /**
   * Starts a support service's metadata. Define the cluster identity and its requirements, such and its RPC endpoint
   * and a support file path.
   *
   * @throws Exception If any exception occurs while trying to get the cluster identity from its RPC endpoint
   */
  @Override
  public void start() throws Exception {
    registerClusterIdentityEndpoint();

    // Only get cluster identity from Coordinator nodes
    ClusterIdentity id;
    if (config.getConfig().getBoolean(DremioConfig.ENABLE_COORDINATOR_BOOL)) {
      store = new ConfigurationStore(kvStoreProvider.get());

      Optional<ClusterIdentity> clusterIdentity = getClusterIdentityFromStore(store, kvStoreProvider.get());

      if (!clusterIdentity.isPresent()) {
        // this is a new cluster, generating a new cluster identifier.
        id = new ClusterIdentity()
          .setIdentity(UUID.randomUUID().toString())
          .setVersion(toClusterVersion(VERSION))
          .setCreated(System.currentTimeMillis());
        id = storeIdentity(id);
      } else {
        id = clusterIdentity.get();
      }
    } else {
      id = getClusterIdentityFromRPC();
    }

    identity = id;
    FileSystemPlugin supportPlugin = catalogServiceProvider.get().getSource(LOCAL_STORAGE_PLUGIN);
    Preconditions.checkNotNull(supportPlugin);
    final String supportPathURI = supportPlugin.getConfig().getPath().toString();
    supportPath = new File(supportPathURI).toPath();
  }

  /**
   * Downloads a support request as a zip file.
   * <p> 
   * The support request file will contain the request's metadata,
   * such as its submission ID and a summary of the Job executed.
   *
   * @param request the request to have its metadata downloaded
   * @return        an object with the downloaded file's metadata
   * @throws UserNotFoundException If it couldn't find the user's profile
   * @throws IOException           If any exception occurs while trying to create the downloaded file
   * @throws JobNotFoundException  If it couldn't find the Job for the request
   */
  @Override
  public DownloadDataResponse downloadSupportRequest(SupportRequest request)
    throws UserNotFoundException, IOException, JobNotFoundException {
    Pointer<User> config = new Pointer<>();
    Pointer<Boolean> outIncludesLogs = new Pointer<>();
    Pointer<String> outSubmissionId = new Pointer<>();
    Path path = generateSupportRequest(request, outIncludesLogs, config, outSubmissionId);
    long size = path.toFile().length();
    return new DownloadDataResponse(Files.newInputStream(path), outSubmissionId.value + ".zip", size);
  }

  public OptionManager getOptions() {
    return optionManagerProvider.get();
  }

  /**
   * Builds a support zip file and upload it to s3.
   *
   * @param userId the user ID that requested support
   * @param jobId  the Job ID executed by the support request
   * @return       the built support response
   * @throws IOException           If any exception occurs while building the zip file
   * @throws UserNotFoundException If it couldn't find the user profile that requested support
   */
  @Override
  public SupportResponse uploadSupportRequest(String userId, JobId jobId) throws UserNotFoundException, IOException {

    Pointer<User> outUserConfig = new Pointer<>();
    Pointer<Boolean> outIncludesLogs = new Pointer<>(false);
    Pointer<String> outSubmissionId = new Pointer<>();

    try {
      final SupportRequest request = new ImmutableSupportRequest.Builder()
        .setJobId(jobId)
        .setUserId(userId)
        .build();

      final Path path = generateSupportRequest(request, outIncludesLogs, outUserConfig, outSubmissionId);

      final boolean includesLogs = outIncludesLogs.value;
      final String submissionId = outSubmissionId.value;

      boolean isDebug = this.config.allowTestApis;

      Response response = null;
      WebTarget target = null;

      try {
        if (isDebug) {
          return new SupportResponse(false, includesLogs, "Unable to upload diagnostics in debug, available locally at: " + path.toString());
        } else {
          final Client client = ClientBuilder.newClient();
          target = client.target(getOptions().getOption(SUPPORT_UPLOAD_BASE))
            .path(this.getClusterId().getIdentity())
            .path(submissionId + ".zip");

          response = target.request().put(Entity.entity(new FileInputStream(path.toString()), MediaType.ZIP.toString()));

          if (response.getStatus() == Status.OK.getStatusCode()) {
            return new SupportResponse(true, includesLogs, target.getUri().toString());
          } else {
            logger.error("Failure while uploading file.", response.toString());
            return new SupportResponse(false, false, "Unable to upload diagnostics, available locally at: " + path.toString());
          }
        }
      } catch (Exception ex) {
        logger.error("Failure while uploading file.", ex);
        return new SupportResponse(false, false, "Unable to upload diagnostics, available locally at: " + path.toString());
      }
    } catch (Exception ex) {
      logger.error("Failure while generating support submission.", ex);
    }
    return new SupportResponse(false, false, null);
  }

  /**
   * Generates a support request output zip file.
   *
   * @param request         the support request executed
   * @param outIncludesLogs a flag with indicates if the output file should include the job logs
   * @param outUserConfig   an object with the user's metadata
   * @param outSubmissionId an object to hold the support request submission ID
   * @return                an object that hold the output file path
   * @throws IOException           If any exception occurs while building the zip file
   * @throws UserNotFoundException If it couldn't find the user profile that requested support
   * @throws JobNotFoundException  If it couldn't find the Job for the request
   */
  private Path generateSupportRequest(SupportRequest request, Pointer<Boolean> outIncludesLogs,
                                      Pointer<User> outUserConfig, Pointer<String> outSubmissionId)
    throws IOException, UserNotFoundException, JobNotFoundException {
    final String submissionId = UUID.randomUUID().toString();
    outSubmissionId.value = submissionId;

    Files.createDirectories(supportPath);
    Path path = supportPath.resolve(submissionId + ".zip");
    outUserConfig.value = userService.get().getUser(request.getUserId());
    User config = outUserConfig.value;

    // inner try to close file once written.
    try (
      FileOutputStream fos = new FileOutputStream(path.toFile());
      BufferedOutputStream dest = new BufferedOutputStream(fos);
      ZipOutputStream zip = new ZipOutputStream(dest);
    ) {

      zip.putNextEntry(new ZipEntry("header.json"));
      recordHeader(zip, request, config, submissionId);

      final JobSummary jobSummary = getJobSummary(request, config);

      for (int attemptIndex = 0; attemptIndex < jobSummary.getNumAttempts(); attemptIndex++) {
        zip.putNextEntry(new ZipEntry(String.format("profile_attempt_%d.json", attemptIndex)));
        QueryProfile profile = recordProfile(zip, request, attemptIndex);

        if (profile.hasPrepareId()) {
          final QueryId id = profile.getPrepareId();
          zip.putNextEntry(new ZipEntry(String.format("prepare_profile_attempt_%d.json", attemptIndex)));
          JobId prepareId = new JobId(new UUID(id.getPart1(), id.getPart2()).toString());
          recordProfile(zip, prepareId, request, 0);
        }

        // If the query failed, collect log file information.
        if (profile.getState() == UserBitShared.QueryResult.QueryState.FAILED) {
          zip.putNextEntry(new ZipEntry(String.format("log_attempt_%d.json", attemptIndex)));
          outIncludesLogs.value = recordLog(zip, request.getUserId(), profile.getStart(), profile.getEnd(), request.getJobId(), submissionId);
        }
      }
    }

    return path;
  }

  /**
   * Acquires a summary of a job execution, such as the maximum number of attempts to execute it.
   *
   * @param supportRequest the support request that generated the job
   * @param config         an object with the user that requested support metadata, such as its name
   * @return               an object with the founded job execution summary
   * @throws JobNotFoundException If it couldn't find the Job for the request
   */
  protected JobSummary getJobSummary(SupportRequest supportRequest, User config) throws JobNotFoundException {
    final JobSummaryRequest request = JobSummaryRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(supportRequest.getJobId()))
      .setUserName(config.getUserName())
      .build();
    return jobsService.get().getJobSummary(request);
  }

  /**
   * Acquires the details of a job execution, such as the results of the attempts to execute it.
   *
   * @param supportRequest the support request that generated the job
   * @param user           an object with the user that requested support metadata, such as its name
   * @return               an object with the founded job execution details
   * @throws JobNotFoundException If it couldn't find the Job for the request
   */
  protected JobDetails getJobDetails(SupportRequest supportRequest, User user) throws JobNotFoundException {
    final JobDetailsRequest request = JobDetailsRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(supportRequest.getJobId()))
      .setUserName(user.getUserName())
      .build();

    return jobsService.get().getJobDetails(request);
  }

  /**
   * Registers a support request's header in a zip file.
   *
   * @param output         an output stream where the header will be written
   * @param supportRequest an object with the support request's metadata
   * @param user           an object with the user that requested support metadata, such as its name and UUID
   * @param submissionId   an identify for the support request
   * @return               a flag which indicates if the header was successfully registered
   * @throws UserNotFoundException If it couldn't find the user profile that requested support
   * @throws IOException           If any exception occurs while writing the header at the output stream
   * @throws JobNotFoundException  If it couldn't find the Job for the request
   */
  private boolean recordHeader(OutputStream output, SupportRequest supportRequest, User user, String submissionId)
    throws UserNotFoundException, IOException, JobNotFoundException {

    SupportHeader header = new SupportHeader();

    header.setClusterInfo(getClusterInfo());

    header.setJob(JobsProtoUtil.getLastAttempt(getJobDetails(supportRequest, user)));

    Submission submission = new Submission()
      .setSubmissionId(submissionId)
      .setDate(System.currentTimeMillis())
      .setEmail(user.getEmail())
      .setFirst(user.getFirstName())
      .setLast(user.getLastName());

    header.setSubmission(submission);

    // record the dremio version that was used to run the query in the header
    header.setDremioVersion(getProfile(supportRequest, 0).getDremioVersion());

    ProtostuffUtil.toJSON(output, header, SupportHeader.getSchema(), false);
    return true;
  }

  /**
   * Acquires a support request query profile.
   *
   * @param supportRequest the profile's support request
   * @param attempt        the number of attempts that was already tried to execute the current job
   * @return               an object with the query profile's metadata
   * @throws JobNotFoundException If it couldn't find the Job for the request
   */
  protected QueryProfile getProfile(SupportRequest supportRequest, int attempt) throws JobNotFoundException {
    QueryProfileRequest request = QueryProfileRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(supportRequest.getJobId()))
      .setUserName(supportRequest.getUserId())
      .setAttempt(attempt)
      .build();
    return jobsService.get().getProfile(request);
  }

  /**
   * Retrieves a profile based on a support request and writes it as JSON to a file.
   *
   * @param out            a connection to the zip file where the JSON file will be written
   * @param supportRequest the support request containing the User ID and Job ID
   * @param attempt        the number of performed attempts to retrieve the support request profile
   * @return               the retrieved support request profile
   * @throws IOException          If an error occurs during serialization
   * @throws JobNotFoundException If it doesn't find the job related to the support request
   */
  private QueryProfile recordProfile(OutputStream out, SupportRequest supportRequest, int attempt) throws IOException, JobNotFoundException {
    QueryProfile profile = getProfile(supportRequest, attempt);
    ProtobufUtils.writeAsJSONTo(out, profile);
    return profile;
  }

  /**
   * Retrieves a profile based on a support request and writes it as JSON to a file.
   * <p> 
   * It uses a Job ID generated based on the query ID.
   *
   * @param out            a connection to the zip file where the JSON file will be written
   * @param supportRequest the support request containing the User ID and Job ID
   * @param attempt        the number of performed attempts to retrieve the support request profile
   * @param id             the Job ID generated for the support request based on the query ID
   * @return               the retrieved support request profile
   * @throws IOException          If an error occurs during serialization
   * @throws JobNotFoundException If it doesn't find the job related to the support request
   */
  private QueryProfile recordProfile(OutputStream out, JobId id, SupportRequest supportRequest, int attempt) throws IOException, JobNotFoundException {
    QueryProfileRequest request = QueryProfileRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(id))
      .setUserName(supportRequest.getUserId())
      .setAttempt(attempt)
      .build();
    QueryProfile profile = jobsService.get().getProfile(request);
    ProtobufUtils.writeAsJSONTo(out, profile);
    return profile;
  }

  /**
   * Creates a log file containing the information of a failed query.
   *
   * @param output       a connection to the zip file where the JSON file containing the log information will be written
   * @param userId       the User ID from the support request
   * @param start        the time when the query processing started
   * @param end          the time when the query processing ended
   * @param id           the Job ID from the support request
   * @param submissionId the Submission ID from the support request
   * @return             returns a flag indicating if the log was successfully recorded
   */
  private boolean recordLog(OutputStream output, String userId, long start, long end, JobId id, String submissionId) {
    try {
      final String startTime = JodaDateUtility.formatTimeStampMilli.print(start - PRE_TIME_BUFFER_MS);
      final String endTime = JodaDateUtility.formatTimeStampMilli.print(end + POST_TIME_BUFFER_MS);

      final SqlQuery query = JobRequestUtil.createSqlQuery(
        String.format(LOG_QUERY, SqlUtils.quoteIdentifier(submissionId), startTime, endTime, "%" + id.getId() + "%"),
        Collections.singletonList(LOGS_STORAGE_PLUGIN), userId);

      final CompletionListener completionListener = new CompletionListener();

      jobsService.get().submitJob(SubmitJobRequest.newBuilder().setSqlQuery(query).setQueryType(QueryType.UI_INTERNAL_RUN).build(), completionListener);


      try {
        completionListener.await(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException("Log search took more than " + TIMEOUT_IN_SECONDS + " seconds to complete.");
      }

      if (!completionListener.isCompleted()) {
        throw new RuntimeException("Log search was cancelled or failed.");
      }

      Path outputFile = supportPath.resolve(submissionId).resolve("0_0_0.json");
      try (FileInputStream fis = new FileInputStream(outputFile.toFile())) {
        ByteStreams.copy(fis, output);
      }
      return true;
    } catch (Exception ex) {
      logger.warn("Failure while attempting to query log files for support submission.", ex);
      PrintWriter writer = new PrintWriter(output);
      writer.write(String.format("{\"message\": \"Log searching failed with exception %s.\"}", ex.getMessage()));
      writer.flush();
      return false;
    }

  }

  /**
   * Retrieves a cluster information such as its identity, version, sources list, nodes list and edition.
   * <p> 
   * Used when recording a support request header to save the cluster current
   * state along with other header information.
   *
   * @return a cluster information object containing the cluster metadata (e.g. version, identity)
   */
  private ClusterInfo getClusterInfo() {
    SoftwareVersion version = new SoftwareVersion().setVersion(DremioVersionInfo.getVersion());

    List<Source> sources = new ArrayList<>();
    final NamespaceService ns = namespaceServiceProvider.get();
    for (SourceConfig source : ns.getSources()) {
      String type = source.getType() == null ? source.getLegacySourceTypeEnum().name() : source.getType();
      sources.add(new Source().setName(source.getName()).setType(type));
    }
    List<Node> nodes = new ArrayList<>();
    for (NodeEndpoint ep : clusterCoordinatorProvider.get().getServiceSet(Role.EXECUTOR).getAvailableEndpoints()) {
      nodes.add(new Node().setName(ep.getAddress()).setRole("executor"));
    }

    for (NodeEndpoint ep : clusterCoordinatorProvider.get().getServiceSet(Role.COORDINATOR).getAvailableEndpoints()) {
      nodes.add(new Node().setName(ep.getAddress()).setRole("coordinator"));
    }

    return new ClusterInfo()
      .setIdentity(identity)
      .setVersion(version)
      .setSourceList(sources)
      .setNodeList(nodes)
      .setJavaVmVersion(System.getProperty("java.vm.version"))
      .setJreVersion(System.getProperty("java.specification.version"))
      .setEdition(getEditionInfo())
      ;
  }

  @Override
  public void close() {
  }

  /**
   * Gets the current edition running. Returns "community" as default, other editions (marketplace or enterprise) should override it.
   *
   * @return the current edition running
   */
  public String getEditionInfo() {
    return "community";
  }

  // this query should be improved once we support converting from ISO8660 time format.
  private static final String LOG_QUERY =
    "CREATE TABLE " + SqlUtils.quoteIdentifier(LOCAL_STORAGE_PLUGIN) + ".%s \n" +
      "  STORE AS (type => 'json', prettyPrint => false) \n" +
      "  WITH SINGLE WRITER\n" +
      "  AS\n" +
      "SELECT * \n" +
      "FROM \n" +
      "  json\n" +
      "WHERE\n" +
      "  (\n" +
      "    cast(\n" +
      "      substr(" + SqlUtils.quoteIdentifier("timestamp") + ",\n" +
      "        0,\n" +
      "        length(" + SqlUtils.quoteIdentifier("timestamp") + ") - 4\n" +
      "      ) as timestamp\n" +
      "  ) between timestamp'%s' and timestamp'%s'\n" +
      "    AND \n" +
      "    levelValue >= 30000\n" +
      "  )\n" +
      "  OR\n" +
      "  thread like '%s'\n" +
      "";
}
