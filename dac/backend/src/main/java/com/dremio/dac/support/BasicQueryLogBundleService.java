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

import static com.dremio.dac.support.SupportService.DREMIO_LOG_PATH_PROPERTY;
import static com.dremio.dac.support.SupportService.TEMPORARY_SUPPORT_PATH;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import javax.inject.Provider;
import javax.ws.rs.NotSupportedException;

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.joda.time.DateTimeZone;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.service.datasets.DatasetDownloadManager.DownloadDataResponse;
import com.dremio.dac.service.support.CoordinatorLogServiceGrpc;
import com.dremio.dac.service.support.CoordinatorLogServiceGrpc.CoordinatorLogServiceBlockingStub;
import com.dremio.dac.service.support.SupportBundleRPC.Chunk;
import com.dremio.dac.service.support.SupportBundleRPC.LogRequest;
import com.dremio.dac.service.support.SupportBundleRPC.LogType;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.NodeQueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.provision.Cluster;
import com.dremio.provision.ClusterEnriched;
import com.dremio.provision.ClusterType;
import com.dremio.provision.ExecutorLogsProvider;
import com.dremio.provision.ExecutorLogsProvider.ExecutorLogMetadata;
import com.dremio.provision.service.ProvisioningHandlingException;
import com.dremio.provision.service.ProvisioningService;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.DistributedSemaphore.DistributedLease;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.users.UserNotFoundException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import io.grpc.ManagedChannel;


/**
 * QueryLogBundle Service for downloading logs for a query
 */
public class BasicQueryLogBundleService implements QueryLogBundleService {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicQueryLogBundleService.class);

  private final Provider<SabotContext> sabotContextProvider;
  private final Provider<ClusterCoordinator> clusterCoordinatorProvider;
  private final Provider<ProjectOptionManager> projectOptionManagerProvider;
  private final Provider<SupportService> supportServiceProvider;
  private final Provider<JobsService> jobsServiceProvider;
  private final Provider<ProvisioningService> provisioningServiceProvider;
  private final Provider<Map<ClusterType, ExecutorLogsProvider>> containerLogProvider;
  private Map<ClusterType, ExecutorLogsProvider> executorLogProviders;

  private final Provider<ConduitProvider> conduitProviderProvider;
  private final Provider<Collection<NodeEndpoint>> coordinatorEndpointsProvider;

  private static final ExecutorService executorService = Executors.newFixedThreadPool(
    1,
    r -> new Thread(r, "support-bundle-producer"));

  public BasicQueryLogBundleService(ScanResult scanResult,
                          Provider<SabotContext> sabotContextProvider,
                          Provider<ClusterCoordinator> clusterCoordinatorProvider,
                          Provider<ProjectOptionManager> projectOptionManagerProvider,
                          Provider<SupportService> supportServiceProvider,
                          Provider<JobsService> jobsServiceProvider,
                          Provider<ProvisioningService> provisioningServiceProvider,
                          Provider<ConduitProvider> conduitProvider,
                          Provider<Collection<NodeEndpoint>> coordinatorEndpoints) {
    super();
    this.sabotContextProvider = sabotContextProvider;
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
    this.projectOptionManagerProvider = projectOptionManagerProvider;
    this.supportServiceProvider = supportServiceProvider;
    this.jobsServiceProvider = jobsServiceProvider;
    this.provisioningServiceProvider = provisioningServiceProvider;
    this.containerLogProvider = () -> buildContainersLogs(scanResult);

    this.conduitProviderProvider = conduitProvider;
    this.coordinatorEndpointsProvider = coordinatorEndpoints;

    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  @Override
  public void start() {
    executorLogProviders = containerLogProvider.get();
  }

  @Override
  public void close() {
  }

  /**
   * Build Container log
   * @param scanResult
   * @return
   */
  static Map<ClusterType, ExecutorLogsProvider> buildContainersLogs(ScanResult scanResult) {
    Set<Class<? extends ExecutorLogsProvider>> logClasses =
      scanResult.getImplementations(ExecutorLogsProvider.class);
    Map<ClusterType, ExecutorLogsProvider> executorLogsProviders = new HashMap<>();

    for (Class<? extends ExecutorLogsProvider> logClass : logClasses) {
      try {
        Constructor<? extends ExecutorLogsProvider> ctor = logClass.getConstructor();
        ExecutorLogsProvider executorLogsProvider = ctor.newInstance();
        executorLogsProviders.put(executorLogsProvider.getType(), executorLogsProvider);
      } catch (ReflectiveOperationException e) {
        logger.error("Unable to create instance of {} class", logClass.getName(), e);
      }
    }
    return Collections.unmodifiableMap(executorLogsProviders);
  }

  @Override
  public void validateUser(String userName) throws UserNotFoundException, NotSupportedException {
    // userName is unused, since every user is treated as an admin in CE
    if (!projectOptionManagerProvider.get().getOption(USERS_BUNDLE_DOWNLOAD)) {
      throw new NotSupportedException("Permission denied to download query bundle.");
    }
  }

  @Override
  public InputStream getClusterLog(final String jobId, final String userName)
    throws UserNotFoundException, JobNotFoundException, IOException, ProvisioningHandlingException, NotSupportedException {

    final Path dremioLogDir = Paths.get(System.getProperty(DREMIO_LOG_PATH_PROPERTY, "/var/log/dremio"));
    final Path tempSupportDir = Paths.get(sabotContextProvider.get().getOptionManager().getOption(TEMPORARY_SUPPORT_PATH));

    // get profile info and check if user has access to the job
    final JobProtobuf.JobId jobID = JobProtobuf.JobId.newBuilder().setId(jobId).build();
    QueryProfileRequest request = QueryProfileRequest.newBuilder()
      .setJobId(jobID)
      .setUserName(userName)
      .build();
    final QueryProfile profile = jobsServiceProvider.get().getProfile(request);
    final long start = profile.getStart();
    final long end = profile.getEnd();
    final PipedOutputStream pipeOs = new PipedOutputStream();
    final PipedInputStream pipeIs = new PipedInputStream(pipeOs, BUFFER_SIZE);

    Set<String> executorsAddr = getAssociatedExecutors(profile);
    Cluster cluster = null;
    ExecutorLogsProvider executorLogsProvider = null;
    if (!executorsAddr.isEmpty()) {
      // Get Cluster for the executors
      cluster = getCluster(executorsAddr);
      if (cluster == null) {
        throw new ProvisioningHandlingException("No provisioning engine was detected.");
      }
      // get container log provider for a specific cluster type
      executorLogsProvider = executorLogProviders.get(cluster.getClusterConfig().getClusterType());
      if (executorLogsProvider == null) {
        throw new NotSupportedException("Query bundle is not supported for cluster type: "
          + cluster.getClusterConfig().getClusterType().name() + ".");
      }
    }

    // execute only one query-bundle download at a time to avoid system crawling
    final DistributedLease lease;
    try {
      lease = clusterCoordinatorProvider.get().getSemaphore("support-bundle", 1).acquire(500, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      logger.error("Failed to acquire semaphore for support bundle download request. {}", e.toString());
      throw new RuntimeException(e);
    }
    if (lease == null) {
      throw new NotSupportedException("Another Support Bundle download is in progress, please wait until it is completed and retry.");
    }

    final Cluster finalCluster = cluster;
    final ExecutorLogsProvider finalExecutorLogsProvider = executorLogsProvider;
    executorService.execute(() -> {

      try (TarArchiveOutputStream taros = new TarArchiveOutputStream(pipeOs);
           DistributedLease toClose = lease) { // allow next download request when preparation for current download request is completed
        taros.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX); // for long filename

        getConfigurations(taros); // config is synced on all coordinators
        pipeOs.flush();

        // get full profile
        QueryLogBundleService.writeAFileToTar(taros, getFullQueryProfile(jobId, userName), "query_profile.zip", true);
        pipeOs.flush();

        // get logs from the coordinator that planned the query
        final NodeEndpoint planCoordinator = profile.getForeman();
        final Collection<NodeEndpoint> allCoordinators = coordinatorEndpointsProvider.get();
        final NodeEndpoint currentEndpoint = sabotContextProvider.get().getEndpoint();

        if (!(currentEndpoint.getAddress().equals(planCoordinator.getAddress()) &&
          currentEndpoint.getUserPort() == planCoordinator.getUserPort())) {
          // if the planning coordinator is not current, then get logs over grpc
          Optional<NodeEndpoint> planCoor = allCoordinators.stream()
            .filter(coorEndpoint ->
                coorEndpoint.getAddress().equals(planCoordinator.getAddress()) &&
                  coorEndpoint.getUserPort() == planCoordinator.getUserPort()
              )
            .findFirst();
          if (planCoor.isPresent()) {
            getCoordinatorLogsOverGrpc(start, end, planCoor.get(), taros);
            pipeOs.flush();
          } else {
            logger.warn("Found no coordinators. ");
          }
        } else {
          // if the planning coordinator is current, then get logs from local node directory
          getLocalCoordinatorLog(taros, dremioLogDir, start, end);
          pipeOs.flush();
        }

        if (!executorsAddr.isEmpty()) {
          // get containers logs from all associated executors
          try {
            int numOfFilesAndErrors = finalExecutorLogsProvider.prepareExecutorLogsDownload(finalCluster, executorsAddr, tempSupportDir, profile.getStart(), profile.getEnd());
            Stream<ExecutorLogMetadata> executorLogs = Stream.generate(finalExecutorLogsProvider).limit(numOfFilesAndErrors);
            executorLogs.forEach(logMetadata -> {
              if (!logMetadata.hasError()) {
                QueryLogBundleService.writeAFileToTar(taros, logMetadata.getOutputFile(), logMetadata.getOutputFile().getName(), true);
              } // else : TODO DX-26627 add failure reason to a log in bundle
            });
            pipeOs.flush();
          } catch (Exception e) {
            logger.error("Failed to get executor log. ", e);
          }
        } // else: if no executor present in the profile, support bundle is still needed
      } catch (Exception e) {
        logger.error("Unexpected error: ", e);
      }
    });

    return pipeIs;

  }

  private Cluster getCluster(final Set<String> executors) throws ProvisioningHandlingException {
    Collection<CoordinationProtos.NodeEndpoint> executorEndpoints = sabotContextProvider.get().getExecutors();
    Optional<CoordinationProtos.NodeEndpoint> matchingExecutor = executorEndpoints.stream()
      .filter(nodeEndpoint -> executors.contains(nodeEndpoint.getAddress())).findFirst();
    if (!matchingExecutor.isPresent()) {
      return null;
    }

    String targetClusterName = matchingExecutor.get().getNodeTag();

    Cluster targetCluster = null;
    for (ClusterEnriched clusterEnriched : provisioningServiceProvider.get().getClustersInfo()) {
      String clusterName = clusterEnriched.getCluster().getClusterConfig().getName();
      // When default engine is used, executor node tag will be empty string, default engine's cluster name will be null
      // and its corresponding yarn application name is "DremioDaemon"
      if (("".equals(targetClusterName) && clusterName == null) || targetClusterName.equals(clusterName)) {
        targetCluster = clusterEnriched.getCluster();
        break;
      }
    }
    return targetCluster;
  }

  private Set<String> getAssociatedExecutors(QueryProfile profile) {
    final Set<String> result = new HashSet<>();

    for(NodeQueryProfile node: profile.getNodeProfileList()) {
      result.add(node.getEndpoint().getAddress());
    }

    return result;
  }

  private File getFullQueryProfile(String jobId, String userName) {
    final Path tempSupportPath = Paths.get(sabotContextProvider.get().getOptionManager().getOption(TEMPORARY_SUPPORT_PATH));

    try {
      // re-use the existing download profile function
      final ImmutableSupportRequest profileRequest = new ImmutableSupportRequest.Builder()
        .setUserId(userName)
        .setJobId(new JobId(jobId))
        .build();
      DownloadDataResponse response = supportServiceProvider.get().downloadSupportRequest(profileRequest);
      Path profilePath = tempSupportPath.resolve(response.getFileName());
      return profilePath.toFile();
    } catch (IOException e) {
      logger.error("Failed to get profile. ", e);
    } catch (UserNotFoundException ignored) {
    } catch (JobNotFoundException e) {
      logger.warn("Profile is not found. {}", e.toString());
    }
    return null;
  }

  private void getCoordinatorLogsOverGrpc(long start, long end,
                                  NodeEndpoint coordinatorEndPoint,
                                  TarArchiveOutputStream taros) {

    final List<LogType> logTypes = Arrays.asList(LogType.SERVER_LOG, LogType.SERVER_GC, LogType.SERVER_OUT, LogType.QUERIES_JSON);
    // a map of log type to the entry name in the resulting tar ball
    final Map<LogType, String> filenameMaps = ImmutableMap.of(LogType.SERVER_LOG, COOR_LOG_PATH, LogType.SERVER_GC, GC_LOG_PATH,
      LogType.SERVER_OUT, SERVER_OUT_LOG_PATH, LogType.QUERIES_JSON, QUERY_LOG_PATH);

    final ConduitProvider conduitProvider = conduitProviderProvider.get();
    final Path tempSupportDir = Paths.get(sabotContextProvider.get().getOptionManager().getOption(TEMPORARY_SUPPORT_PATH));

    logTypes.forEach(logType -> {
      try {
        // store the stream
        Path outputPath = tempSupportDir.resolve(UUID.randomUUID().toString() + filenameMaps.get(logType));
        try (
          BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outputPath.toFile()))
        ) {
          LogRequest logRequest = LogRequest.newBuilder().setStart(start).setEnd(end).setType(logType).build();
          ManagedChannel channel = conduitProvider.getOrCreateChannel(coordinatorEndPoint);
          // different log types are written to a local gzip file sequentially
          CoordinatorLogServiceBlockingStub stub = CoordinatorLogServiceGrpc.newBlockingStub(channel);

          Iterator<Chunk> chunks =  stub.getServerLog(logRequest);
          logger.debug("getting {} from {} over grpc", logType, coordinatorEndPoint.getAddress());
          chunks.forEachRemaining(chunk -> { // chunk: gzip format raw stream
            try {
              outputStream.write(chunk.getContent().toByteArray(), 0, chunk.getLength());
            } catch (IOException e) {
              logger.error("Failed to write grpc stream to {}. {}", logType, e);
            }
          });
        }

        QueryLogBundleService.writeAFileToTar(taros, outputPath.toFile(), filenameMaps.get(logType), true);

      } catch (Exception e) {
        logger.error("Failed to get {}. ", logType, e);
      }
    });

  }

  private void getLocalCoordinatorLog(TarArchiveOutputStream taros, Path inputDir, long start, long end) throws IOException {
    getServerOrQueryLog(inputDir, taros, start, end, "server", "log");
    getServerOrQueryLog(inputDir, taros, start, end, "queries", "json");
    getGCLog(start, end, inputDir, taros);
    getServerOut(inputDir, taros);
  }

  private void getServerOrQueryLog(Path inputDir, TarArchiveOutputStream taros,
                                   long start, long end, String logName, String logFormat) {
    try {
      final Path tempSupportDir = Paths.get(sabotContextProvider.get().getOptionManager().getOption(TEMPORARY_SUPPORT_PATH));
      // obtain all needed gzip log in archive/
      final File[] allArchiveLogFiles = inputDir.resolve("archive").toFile()
        .listFiles((dir, name) -> name.startsWith(logName + ".") && name.endsWith("." + logFormat + ".gz")
            && DateTimeUtils.isBetweenDay(name, start, end, DateTimeZone.UTC)
        );

      // write all log files to a single gzip file
      File tempFile = tempSupportDir.resolve(String.format("%s_%s.%s.gz", UUID.randomUUID().toString(), logName, logFormat)).toFile();
      try (GZIPOutputStream gzos = new GZIPOutputStream(new FileOutputStream(tempFile))) {
        // get gzip log from archive
        if (allArchiveLogFiles != null) {
          QueryLogBundleService.sortLogFilesInTimeOrder(allArchiveLogFiles);
          for (File file : allArchiveLogFiles) {
            try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
              byte[] buf = new byte[BUFFER_SIZE];
              int len;
              while ((len = inputStream.read(buf)) > -1) {
                gzos.write(buf, 0, len);
              }
            } catch (IOException e) {
              logger.error("Failed to write {} to a temp file {}. {}",
                file.getAbsolutePath(), tempFile.getAbsolutePath(), e);
            }
          }
        }
        // if query is run today, include the entire server.log or queries.json in the gzip file
        if (DateTimeUtils.isToday(end, DateTimeZone.UTC)) {
          File file = inputDir.resolve(logName + "." + logFormat).toFile();
          try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            byte[] buf = new byte[BUFFER_SIZE];
            int len;
            while ((len = inputStream.read(buf)) > -1) {
              gzos.write(buf, 0, len); // test?
            }
          } catch (IOException e) {
            logger.error("Failed to write {} to a temp file {}. {}",
              file.getAbsolutePath(), tempFile.getAbsolutePath(), e);
          }
        }
      }
      // write the gzip file to tar then delete
      QueryLogBundleService.writeAFileToTar(taros, tempFile, String.format("%s.%s.gz", logName, logFormat), true);

    } catch (Exception e) {
      logger.error("Failed to get server log. {}", e.toString());
    }

  }

  private void getServerOut(Path inputDir, TarArchiveOutputStream taros) {
    try {
      // write the entire server out to tar
      final Path tempSupportDir = Paths.get(sabotContextProvider.get().getOptionManager().getOption(TEMPORARY_SUPPORT_PATH));
      final Path inputLogPath = inputDir.resolve("server.out");
      // compress the file at {inputDir}/server.out and temporarily save to {tempSupportDir}/{uuid}_server.out.gz
      File gzipFile = CoordinatorLogService.compressAPlainText(inputLogPath, tempSupportDir, UUID.randomUUID().toString() + "_server.out.gz");
      QueryLogBundleService.writeAFileToTar(
        taros,
        gzipFile,
        "server.out.gz",
        true);
    } catch (Exception e) {
      logger.error("Failed to get server.out.gz");
    }
  }

  private void getGCLog(long start, long end, Path inputDir, TarArchiveOutputStream taros) {

    try {
      final Path tempSupportDir = Paths.get(sabotContextProvider.get().getOptionManager().getOption(TEMPORARY_SUPPORT_PATH));
      // gather all gc logs
      final File[] allGcLogFiles = inputDir.toFile().listFiles((dir, name) -> name.startsWith("server.gc"));

      if (allGcLogFiles != null) {
        // sort all gc logs in time order based on filename
        Arrays.sort(allGcLogFiles, Collections.reverseOrder());
        // compress gc logs (filter by days) into one file, then append to taros, finally delete it
        // TODO compress and append in async
        File gzipFile = tempSupportDir.resolve(UUID.randomUUID().toString() + ".gc.gz").toFile();
        CoordinatorLogService.filterFilesContentByDayAndCompress(allGcLogFiles, gzipFile, start, end);
        QueryLogBundleService.writeAFileToTar(taros, gzipFile, "server.gc.gz", true);
      }
    } catch (Exception e) {
      logger.error("Failed to get server.gc.gz. {}", e.toString());
    }
  }

  public Map<String, Object> getWLMConfig() {
    return Collections.emptyMap();
  }

  private void getConfigurations(TarArchiveOutputStream taros) {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

    try {

      final Path tempSupportDir = Paths.get(sabotContextProvider.get().getOptionManager().getOption(TEMPORARY_SUPPORT_PATH));
      final Path outputPath = tempSupportDir.resolve(UUID.randomUUID().toString() + ".json.gz");

      try (
        GZIPOutputStream gzos = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(outputPath.toFile())))
      ) {
        final Map<String, Object> map = new HashMap<>(getWLMConfig());
        map.put("system_non-default_options", projectOptionManagerProvider.get().getNonDefaultOptions());

        mapper.writeValue(gzos, map);
      } catch (FileNotFoundException e) {
        logger.error("Couldn't find {}. ", outputPath);
      } catch (Exception e) {
        logger.error("Couldn't write to gzip {}. ", outputPath, e);
      }

      // write the resulting gzip file to tar
      QueryLogBundleService.writeAFileToTar(taros, outputPath.toFile(), "system_info.json.gz", true);

    } catch (Exception e) {
      logger.error("Failed to get system_info.json.gz", e);
    }
  }

}
