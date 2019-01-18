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
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
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

import org.apache.arrow.vector.util.DateUtility;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.common.utils.SqlUtils;
import com.dremio.dac.proto.model.source.ClusterIdentity;
import com.dremio.dac.proto.model.source.ClusterInfo;
import com.dremio.dac.proto.model.source.Node;
import com.dremio.dac.proto.model.source.SoftwareVersion;
import com.dremio.dac.proto.model.source.Source;
import com.dremio.dac.proto.model.source.Submission;
import com.dremio.dac.proto.model.source.SupportHeader;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.service.datasets.DatasetDownloadManager.DownloadDataResponse;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.StringSerializer;
import com.dremio.datastore.VersionExtractor;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.SchemaUserBitShared;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.dremio.service.Pointer;
import com.dremio.service.Service;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.services.configuration.ConfigurationStore;
import com.dremio.services.configuration.proto.ConfigurationEntry;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.net.MediaType;

import io.protostuff.ByteString;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;

/**
 * Service responsible for generating cluster identity and upload data to Dremio
 * when a user asks for support help.
 */
@Options
public class SupportService implements Service {
  private static final Logger logger = LoggerFactory.getLogger(SupportService.class);

  public static final int TIMEOUT_IN_SECONDS = 5 * 60;
  public static final String DREMIO_LOG_PATH_PROPERTY = "dremio.log.path";

  public static final BooleanValidator USERS_CHAT = new BooleanValidator("support.users.chat", true);
  public static final BooleanValidator USERS_UPLOAD = new BooleanValidator("support.users.upload", true);
  public static final BooleanValidator USERS_DOWNLOAD = new BooleanValidator("support.users.download", true);
  public static final BooleanValidator USERS_EMAIL = new BooleanValidator("support.users.email", true);

  public static final StringValidator SUPPORT_EMAIL_ADDR = new StringValidator("support.email.addr", "");
  public static final StringValidator SUPPORT_EMAIL_SUBJECT = new StringValidator("support.email.jobs.subject", "");

  public static final StringValidator SUPPORT_UPLOAD_BASE = new StringValidator("support.upload.base", "https://s3-us-west-2.amazonaws.com/supportuploads.dremio.com/");
  public static final StringValidator TEMPORARY_SUPPORT_PATH = new StringValidator("support.temp", "/tmp/dremio-support/");

  public static final BooleanValidator OUTSIDE_COMMUNICATION_DISABLED = new BooleanValidator("dremio.ui.outside_communication_disabled", false);

  public static final String NAME = "support";

  public static final String LOGS_STORAGE_PLUGIN = "__logs";
  public static final String LOCAL_STORAGE_PLUGIN = "__support";
  public static final String CLUSTER_ID = "clusterId";
  public static final String CLUSTER_IDENTITY = "clusterIdentity";

  private static final int PRE_TIME_BUFFER_MS = 5 * 1000;
  private static final int POST_TIME_BUFFER_MS = 10 * 1000;

  private final DACConfig config;
  private final Provider<KVStoreProvider> kvStoreProvider;
  private final Provider<SabotContext> executionContextProvider;
  private final Provider<JobsService> jobsService;
  private final Provider<UserService> userService;
  private final Provider<CatalogService> catalogServiceProvider;
  private Path supportPath;
  private ConfigurationStore store;

  private ClusterIdentity identity;

  public SupportService(
      DACConfig config,
      Provider<KVStoreProvider> kvStoreProvider,
      Provider<JobsService> jobsService,
      Provider<UserService> userService,
      Provider<SabotContext> executionContextProvider,
      Provider<CatalogService> catalogServiceProvider
      ) {
    this.kvStoreProvider = kvStoreProvider;
    this.executionContextProvider = executionContextProvider;
    this.jobsService = jobsService;
    this.userService = userService;
    this.catalogServiceProvider = catalogServiceProvider;
    this.config = config;
  }

  public ClusterIdentity getClusterId(){
    return identity;
  }

  /**
   * tries to store identity in the KVStore, in case another server already stored it, retrieves the stored identity
   * @param identity identity we want to store
   * @return identity stored in the KVStore
   */
  private ClusterIdentity storeIdentity(ClusterIdentity identity) {
    try{
      ConfigurationEntry entry = new ConfigurationEntry();
      entry.setType(CLUSTER_IDENTITY);
      entry.setValue(convertClusterIdentityToByteString(identity));

      store.put(CLUSTER_ID, entry);
      logger.info("New Cluster Identifier Generated: {}", identity.getIdentity());
    } catch(ConcurrentModificationException ex) {
      // someone else inserted the new cluster identifier before we were able to.
      ConfigurationEntry entry = store.get(CLUSTER_ID);
      ProtostuffIOUtil.mergeFrom(entry.getValue().toByteArray(), identity, ClusterIdentity.getSchema());

      if(identity == null){
        throw new IllegalStateException("Failed to retrieve or create cluster identity but identity is also not available.", ex);
      }
    }

    return identity;
  }

  public static Optional<ClusterIdentity> getClusterIdentity(KVStoreProvider provider) {
    ConfigurationStore store = new ConfigurationStore(provider);
    return getClusterIdentityFromStore(store, provider);
  }

  private static Optional<ClusterIdentity> getClusterIdentityFromStore(ConfigurationStore store, KVStoreProvider provider) {
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

  private static Optional<ClusterIdentity> upgradeToNewSupportStore(KVStoreProvider provider) {
    final KVStore<String, ClusterIdentity> oldSupportStore = provider.getStore(OldSupportStoreCreator.class);
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
  public static class OldSupportStoreCreator implements StoreCreationFunction<KVStore<String, ClusterIdentity>> {
    @Override
    public KVStore<String, ClusterIdentity> build(StoreBuildingFactory factory) {
      return factory.<String, ClusterIdentity>newStore()
        .name("identity")
        .keySerializer(StringSerializer.class)
        .valueSerializer(ClusterIdSerializer.class)
        .versionExtractor(ClusterIdentityVersionExtractor.class)
        .build();
    }
  }

  /**
   * Serializer used for serializing cluster identity.
   */
  public static class ClusterIdSerializer extends ProtostuffSerializer<ClusterIdentity> {
    public ClusterIdSerializer() {
      super(ClusterIdentity.getSchema());
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

  public static void updateClusterIdentity(KVStoreProvider provider, ClusterIdentity identity) {
    final KVStore<String, ConfigurationEntry> supportStore = provider.getStore(ConfigurationStore.ConfigurationStoreCreator.class);

    final ConfigurationEntry entry = new ConfigurationEntry();
    entry.setType(CLUSTER_IDENTITY);

    ConfigurationEntry existingSupportEntry = supportStore.get(CLUSTER_ID);
    if (existingSupportEntry != null) {
      entry.setTag(existingSupportEntry.getTag());
    }

    entry.setValue(convertClusterIdentityToByteString(identity));
    supportStore.put(CLUSTER_ID, entry);
  }

  private static ByteString convertClusterIdentityToByteString(ClusterIdentity identity) {
    final LinkedBuffer buffer = LinkedBuffer.allocate();
    byte[] bytes = ProtostuffIOUtil.toByteArray(identity, ClusterIdentity.getSchema(), buffer);
    return ByteString.copyFrom(bytes);
  }

  @Override
  public void start() throws Exception {
    store = new ConfigurationStore(kvStoreProvider.get());

    ClusterIdentity identity;
    Optional<ClusterIdentity> clusterIdentity = getClusterIdentityFromStore(store, kvStoreProvider.get());

    if (!clusterIdentity.isPresent()) {
      // this is a new cluster, generating a new cluster identifier.
      identity = new ClusterIdentity()
          .setIdentity(UUID.randomUUID().toString())
          .setVersion(toClusterVersion(VERSION))
          .setCreated(System.currentTimeMillis());
      identity = storeIdentity(identity);
    } else {
      identity = clusterIdentity.get();
    }

    this.identity = identity;
    FileSystemPlugin supportPlugin = catalogServiceProvider.get().getSource(LOCAL_STORAGE_PLUGIN);
    Preconditions.checkNotNull(supportPlugin);
    final String supportPathURI = supportPlugin.getConfig().getPath().toString();
    supportPath = new File(supportPathURI).toPath();
  }

  public DownloadDataResponse downloadSupportRequest(String userId, JobId jobId)
      throws UserNotFoundException, IOException, JobNotFoundException {
    Pointer<User> config = new Pointer<>();
    Pointer<Boolean> outIncludesLogs = new Pointer<>();
    Pointer<String> outSubmissionId = new Pointer<>();
    Path path = generateSupportRequest(userId, jobId, outIncludesLogs, config, outSubmissionId);
    long size = path.toFile().length();
    return new DownloadDataResponse(Files.newInputStream(path), outSubmissionId.value + ".zip", size);
  }

  public OptionManager getOptions(){
    return executionContextProvider.get().getOptionManager();
  }

  /**
   * Build a support zip file and upload it to s3.
   * @param userId
   * @param jobId
   * @return
   * @throws IOException
   * @throws UserNotFoundException
   */
  public SupportResponse uploadSupportRequest(String userId, JobId jobId) throws UserNotFoundException {

    Pointer<User> outUserConfig = new Pointer<>();
    Pointer<Boolean> outIncludesLogs = new Pointer<>(false);
    Pointer<String> outSubmissionId = new Pointer<>();

    try {
      final Path path = generateSupportRequest(userId, jobId, outIncludesLogs, outUserConfig, outSubmissionId);

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
      }catch(Exception ex){
        logger.error("Failure while uploading file.", ex);
        return new SupportResponse(false, false, "Unable to upload diagnostics, available locally at: " + path.toString());
      }
    } catch (Exception ex){
      logger.error("Failure while generating support submission.", ex);
    }
    return new SupportResponse(false, false, null);
  }

  private Path generateSupportRequest(String userId, JobId jobId, Pointer<Boolean> outIncludesLogs,
                                      Pointer<User> outUserConfig, Pointer<String> outSubmissionId)
      throws IOException, UserNotFoundException, JobNotFoundException {
    final String submissionId = UUID.randomUUID().toString();
    outSubmissionId.value = submissionId;

    Files.createDirectories(supportPath);
    Path path = supportPath.resolve(submissionId + ".zip");
    outUserConfig.value = userService.get().getUser(userId);
    User config = outUserConfig.value;

    // inner try to close file once written.
    try(
        FileOutputStream fos = new FileOutputStream(path.toFile());
        BufferedOutputStream dest = new BufferedOutputStream(fos);
        ZipOutputStream zip = new ZipOutputStream(dest);
        ) {

      zip.putNextEntry(new ZipEntry("header.json"));
      recordHeader(zip, jobId, config, submissionId);

      final Job job = jobsService.get().getJob(jobId);
      for(int attemptIndex = 0; attemptIndex < job.getAttempts().size() ; attemptIndex++) {
        zip.putNextEntry(new ZipEntry(String.format("profile_attempt_%d.json", attemptIndex)));
        QueryProfile profile = recordProfile(zip, jobId, attemptIndex);

        if (profile.hasPrepareId()) {
          final QueryId id = profile.getPrepareId();
          zip.putNextEntry(new ZipEntry(String.format("prepare_profile_attempt_%d.json", attemptIndex)));
          JobId prepareId = new JobId(new UUID(id.getPart1(), id.getPart2()).toString());
          recordProfile(zip, prepareId, 0);
        }

        // If the query failed, collect log file information.
        if (profile.getState() == UserBitShared.QueryResult.QueryState.FAILED) {
          zip.putNextEntry(new ZipEntry(String.format("log_attempt_%d.json", attemptIndex)));
          outIncludesLogs.value = recordLog(zip, userId, profile.getStart(), profile.getEnd(), jobId, submissionId);
        }
      }
    }

    return path;
  }

  private boolean recordHeader(OutputStream output, JobId id, User user, String submissionId)
      throws UserNotFoundException, IOException, JobNotFoundException {

    SupportHeader header = new SupportHeader();

    header.setClusterInfo(getClusterInfo());
    header.setJob(jobsService.get().getJob(id).getJobAttempt());

    Submission submission = new Submission()
        .setSubmissionId(submissionId)
        .setDate(System.currentTimeMillis())
        .setEmail(user.getEmail())
        .setFirst(user.getFirstName())
        .setLast(user.getLastName());

    header.setSubmission(submission);

    // record the dremio version that was used to run the query in the header
    header.setDremioVersion(jobsService.get().getProfile(id, 0).getDremioVersion());

    ProtostuffUtil.toJSON(output, header, SupportHeader.getSchema(), false);
    return true;
  }

  private QueryProfile recordProfile(OutputStream out, JobId id, int attempt) throws IOException, JobNotFoundException {
    QueryProfile profile = jobsService.get().getProfile(id, attempt);
    ProtostuffUtil.toJSON(out, profile, SchemaUserBitShared.QueryProfile.WRITE, false);
    return profile;
  }

  private boolean recordLog(OutputStream output, String userId, long start, long end, JobId id, String submissionId) {
    try{
      final String startTime = DateUtility.formatTimeStampMilli.print(start - PRE_TIME_BUFFER_MS);
      final String endTime = DateUtility.formatTimeStampMilli.print(end + POST_TIME_BUFFER_MS);

      final SqlQuery query = new SqlQuery(
          String.format(LOG_QUERY, SqlUtils.quoteIdentifier(submissionId), startTime, endTime, "%" + id.getId() + "%"),
          Arrays.asList(LOGS_STORAGE_PLUGIN), userId);
      final CompletionListener listener = new CompletionListener();

      jobsService.get().submitJob(JobRequest.newBuilder()
          .setSqlQuery(query)
          .setQueryType(QueryType.UI_INTERNAL_RUN)
          .build(), listener);

      boolean completed = false;
      try {
        completed = listener.latch.await(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        completed = false;
      }

      if (!completed) {
        throw new RuntimeException("Log search took more than " + TIMEOUT_IN_SECONDS + " seconds to complete.");
      } else if(listener.ex != null) {
        throw listener.ex;
      } else if(!listener.success){
          throw new RuntimeException("Log search was cancelled or failed.");
      } else {
        Path outputFile = supportPath.resolve(submissionId).resolve("0_0_0.json");
        try(FileInputStream fis = new FileInputStream(outputFile.toFile())){
          ByteStreams.copy(fis, output);
        }
        return true;
      }
    } catch (Exception ex){
      logger.warn("Failure while attempting to query log files for support submission.", ex);
      PrintWriter writer = new PrintWriter(output);
      writer.write(String.format("{\"message\": \"Log searching failed with exception %s.\"}", ex.getMessage()));
      writer.flush();
      return false;
    }

  }

  private ClusterInfo getClusterInfo(){
    SoftwareVersion version = new SoftwareVersion().setVersion(DremioVersionInfo.getVersion());

    List<Source> sources = new ArrayList<>();
    final NamespaceService ns = executionContextProvider.get().getNamespaceService(SystemUser.SYSTEM_USERNAME);
    for(SourceConfig source : ns.getSources()){
      String type = source.getType() == null ? source.getLegacySourceTypeEnum().name() : source.getType();
      sources.add(new Source().setName(source.getName()).setType(type));
    }
    List<Node> nodes = new ArrayList<>();
    for(NodeEndpoint ep : executionContextProvider.get().getExecutors()){
      nodes.add(new Node().setName(ep.getAddress()).setRole("executor"));
    }

    for(NodeEndpoint ep : executionContextProvider.get().getCoordinators()){
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
   * @return the current edition that's running. Other editions should override this value
   */
  public String getEditionInfo() { return "community"; }

  private class CompletionListener implements JobStatusListener {
    private final CountDownLatch latch = new CountDownLatch(1);

    private Exception ex;
    private boolean success = false;

    @Override
    public void jobSubmitted(JobId jobId) {
    }

    @Override
    public void planRelTransform(PlannerPhase phase, RelNode before, RelNode after, long millisTaken) {

    }

    @Override
    public void metadataCollected(QueryMetadata metadata) {
    }

    @Override
    public void jobFailed(Exception e) {
      latch.countDown();
      ex = e;
    }

    @Override
    public void jobCompleted() {
      latch.countDown();
      success = true;
    }

    @Override
    public void jobCancelled(String reason) {
      latch.countDown();
    }

  }

  // this query should be improved once we support converting from ISO8660 time format.
  private static final String LOG_QUERY =
      "CREATE TABLE " + SqlUtils.quoteIdentifier(LOCAL_STORAGE_PLUGIN)+ ".%s \n" +
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
      "        length("+ SqlUtils.quoteIdentifier("timestamp") +") - 4\n" +
      "      ) as timestamp\n" +
      "  ) between timestamp'%s' and timestamp'%s'\n" +
      "    AND \n" +
      "    levelValue >= 30000\n" +
      "  )\n" +
      "  OR\n" +
      "  thread like '%s'\n" +
      "";
 }
