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
package com.dremio.provision.yarn.support;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.glassfish.jersey.client.ClientProperties;

import com.dremio.config.DremioConfig;
import com.dremio.provision.Cluster;
import com.dremio.provision.ClusterType;
import com.dremio.provision.ExecutorLogsProvider;
import com.dremio.provision.Property;
import com.dremio.provision.service.ProvisioningHandlingException;

/**
 * A class to get container log from yarn clusters.
 */
public class YarnExecutorLogsProvider implements ExecutorLogsProvider {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(YarnExecutorLogsProvider.class);
  private static final int BUFFER_SIZE = 4096;

  private final Client client;
  private final List<String> logTypes = Arrays.asList("stdout", "stderr");
  private final boolean certificateValidationEnabled;
  private final SSLContext insecureSslContext;
  private Authenticator authenticator;
  private Boolean authenticationRequired;
  /* Used for Kerberos Authentication */
  private UserGroupInformation hadoopUserGroupInfo;
  // a queue to store the info needed for all executors' logs download and exceptions with their error message
  private Queue<ExecutorLogMetadata> executorLogsMetadata = new LinkedList<>();

  public YarnExecutorLogsProvider() {
    this(DremioConfig.create());
  }

  public YarnExecutorLogsProvider(DremioConfig dremioConfig) {
    this.certificateValidationEnabled = dremioConfig.getBoolean(DremioConfig.YARN_CERTIFICATE_VALIDATION_ENABLED);
    try {
      insecureSslContext = SSLContext.getInstance("SSL");
      TrustManager[] trustAllCerts = new TrustManager[]{
        new X509TrustManager() {
          public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
          }

          public void checkClientTrusted(X509Certificate[] certs, String authType) {
          }

          public void checkServerTrusted(X509Certificate[] certs, String authType) {
          }
        }
      };
      insecureSslContext.init(null, trustAllCerts, new java.security.SecureRandom());
    } catch (final NoSuchAlgorithmException | KeyManagementException e) {
      throw new IllegalStateException(e);
    }
    ClientBuilder clientBuilder = ClientBuilder.newBuilder();
    if (!certificateValidationEnabled) {
      clientBuilder.sslContext(insecureSslContext);
    }
    client = clientBuilder.build();
    client.property(ClientProperties.CONNECT_TIMEOUT, 1_000);
    client.property(ClientProperties.READ_TIMEOUT,    300_000); // read container log timeout 5 min
  }

  @Override
  public ClusterType getType() {
    return ClusterType.YARN;
  }

  private static boolean isRunning(ApplicationAttemptReport report) {
    return report.getYarnApplicationAttemptState().equals(YarnApplicationAttemptState.RUNNING);
  }

  private static boolean isRunning(ContainerReport report) {
    return report.getContainerState().equals(ContainerState.RUNNING);
  }

  @Override
  public ExecutorLogMetadata get() {
    if (executorLogsMetadata.isEmpty()) {
      return null;
    }

    ExecutorLogMetadata executorLogMetadata = executorLogsMetadata.poll();
    if (executorLogMetadata.hasError()) {
      return executorLogMetadata;
    }

    try {
      return new ExecutorLogMetadata(null, downloadContent(executorLogMetadata.getLogLocator()), null, null);
    } catch (LogsProviderException e) {
      return new ExecutorLogMetadata(null, null, "", e);
    }
  }

  @Override
  public int prepareExecutorLogsDownload(Cluster cluster, Collection<String> executorsAddr,
                                    Path outputDir, long startTime, long endTime) {
    executorLogsMetadata.clear();
    final YarnConfiguration yarnConfiguration = new YarnConfiguration();
    List<Property> keyValues = cluster.getClusterConfig().getSubPropertyList();
    if ( keyValues != null && !keyValues.isEmpty()) {
      keyValues.forEach( property -> yarnConfiguration.set(property.getKey(), property.getValue()));
    }
    //Force to disable create of TimelineService Client.. as the libraries used are conflicting versions
    // and also, we dont really need TimeLineService.
    yarnConfiguration.set(YarnConfiguration.TIMELINE_SERVICE_ENABLED, "false");

    List<ContainerReport> yarnContainers = getYarnContainersForExecutors(cluster, executorsAddr, yarnConfiguration, startTime, endTime);
    if (yarnContainers.isEmpty()) {
      return executorLogsMetadata.size();
    }
    logger.info("Retrieving logs from {} containers", yarnContainers.size());

    //prepare download
    yarnContainers.stream()
      .forEach(yarnContainer -> prepareDownloadLogContent(yarnContainer, outputDir)
                                  .forEach(log -> executorLogsMetadata.offer(log)));

    return executorLogsMetadata.size();
  }

  private List<ContainerReport> getYarnContainersForExecutors(final Cluster cluster,
                                                              final Collection<String> executorAddresses,
                                                              final YarnConfiguration yarnConfiguration,
                                                              final long startTime, final long endTime) {
    YarnClient yarnClient = YarnClient.createYarnClient();
    try {
      yarnClient.init(yarnConfiguration);
      yarnClient.start();
      List<ApplicationReport> applications = yarnClient.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
      //Sometimes cluster doesnt have a name associated. In that case we need to look for Default name in Yarn.
      final String clusterName = cluster.getClusterConfig().getName() == null ? "DremioDaemon": cluster.getClusterConfig().getName();

      //There can be more than one application with same name. So get the entire list with matching name.
      List<ApplicationReport> filteredApplications = applications.stream()
        .filter(application -> clusterName.equals(application.getName())).collect(Collectors.toList());
      if (filteredApplications.size() == 0) {
        throw new ProvisioningHandlingException("Unable to find a application '"+clusterName+"' in Yarn");
      }

      //Get RUNNING attempts for all the filtered applications.
      List<ApplicationAttemptReport> applicationAttempts = filteredApplications.stream()
        .flatMap(application -> getApplicationAttempts(yarnClient, application))
        .filter(YarnExecutorLogsProvider::isRunning).collect(Collectors.toList());

      if (applicationAttempts.size() == 0) {
        throw new ProvisioningHandlingException("Could not find a running application with name '"+clusterName+"'");
      }

      //Get Running containers of this application.
      return applicationAttempts.stream()
        .flatMap(applicationAttempt -> getContainers(yarnClient, applicationAttempt))
        .filter(YarnExecutorLogsProvider::isRunning) // Only running containers.
        .filter(container -> endTime == 0 || container.getCreationTime() < endTime)  //Not interested if container was created after query finished.
        .filter(container -> executorAddresses.contains(getContainerHostname(container))) //If the query ran on this node.
        .collect(Collectors.toList());
    } catch (YarnException e) {
      logger.error("Exception while retrieving application information from Yarn", e);
      executorLogsMetadata.offer(new ExecutorLogMetadata(null, null,
        "Exception while retrieving application information from Yarn", e));
    } catch (Exception e) {
      logger.error("Unknown error while retrieving application information from Yarn", e);
      executorLogsMetadata.offer(new ExecutorLogMetadata(null, null,
        "Unknown error while retrieving application information from Yarn", e));
    } finally {
      yarnClient.stop();
    }
    return Collections.emptyList();
  }

  private String getContainerHostname(ContainerReport container) {
    try {
      URL url;
      if (container.getNodeHttpAddress() == null) {
        url = new URL(container.getLogUrl());
        container.setNodeHttpAddress(url.getProtocol()+"://" + url.getHost() + ":" + url.getPort());
      } else {
        url = new URL(container.getNodeHttpAddress());
      }
      return url.getHost();
    } catch (MalformedURLException e) {
      logger.warn("Container nodeAddress is invalid : {}", container.getNodeHttpAddress());
    }
    return null;
  }

  private Stream<ApplicationAttemptReport> getApplicationAttempts(YarnClient yarnClient, ApplicationReport application) {
    try {
      return yarnClient.getApplicationAttempts(application.getApplicationId()).stream();
    } catch (YarnException e) {
      logger.warn("Yarn error when retrieving application attempts", e);
    } catch (IOException e) {
      logger.warn("IO error when retrieving application attempts", e);
    }
    return Stream.empty();
  }

  private Stream<ContainerReport> getContainers(YarnClient yarnClient, ApplicationAttemptReport applicationAttempt) {
    try {
      return yarnClient.getContainers(applicationAttempt.getApplicationAttemptId()).stream();
    } catch (YarnException e) {
      logger.warn("Yarn error when retrieving containers for attempt {}", applicationAttempt.getApplicationAttemptId(), e);
    } catch (IOException e) {
      logger.warn("IO error when retrieving containers for attempt {}", applicationAttempt.getApplicationAttemptId(), e);
    }
    return Stream.empty();
  }

  private Stream<ExecutorLogMetadata> prepareDownloadLogContent(ContainerReport container, Path outputDir) {
    List<ExecutorLogMetadata> logFilesList = new ArrayList<>();
    logTypes.forEach(logType -> logFilesList.add(new ExecutorLogMetadata(
      new YarnLogLocator(container.getNodeHttpAddress(),
                          container.getContainerId().toString(),
                          logType, outputDir.toString()),
      null, null, null)));

    return logFilesList.stream();
  }

  private File downloadContent(Object logLocator) throws LogsProviderException {
    try {
      YarnLogLocator yarnLogLocator = (YarnLogLocator) logLocator;
      String logUrlString = String.format("%s/ws/v1/node/containerlogs/%s/%s",
        yarnLogLocator.getNodeHttpAddress(), yarnLogLocator.getContainerId(), yarnLogLocator.getLogType());
      URL logUrl = new URL(logUrlString);
      Path outputPath = Paths.get(yarnLogLocator.getOutputDir())
        .resolve(logUrl.getHost() + "_application_" + yarnLogLocator.getContainerId() + "_" + yarnLogLocator.getLogType() + ".log.gz");

      HttpURLConnection httpURLConnection = openHttpUrlConnection(logUrl);
      httpURLConnection.setRequestProperty(HttpHeaders.ACCEPT_ENCODING, "application/gzip");
      httpURLConnection.connect();
      int responseCode = httpURLConnection.getResponseCode();

      if (responseCode != 200 && responseCode!=204) {
        logger.debug("Unsuccessful StatusCode - {}, when trying to fetch '{}'", responseCode, logUrl);
        throw new RuntimeException("Failed to read logs from URL: " + logUrl);
      }
      try (BufferedInputStream inputStream = new BufferedInputStream(httpURLConnection.getInputStream());
           BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outputPath.toFile()))) {
        logger.debug("getting container log {}", outputPath);
        byte[] buffer = new byte[BUFFER_SIZE];
        int len;
        while((len = inputStream.read(buffer)) != -1) {
          outputStream.write(buffer, 0, len);
        }
      }
      return outputPath.toFile();
    } catch (MalformedURLException e) {
      logger.warn("Failed to construct log URL" , e);
      throw new LogsProviderException("Failed to construct log URL", e);
    } catch (Exception e) {
      logger.warn("Failed to read log from {}.", logLocator, e);
      throw new LogsProviderException("Failed to read log from " + logLocator, e);
    }
  }


  private HttpURLConnection openHttpUrlConnection(URL url) throws LogsProviderException {
    if (authenticationRequired==null && authenticator==null) {
      String authHeaders = "";
      authenticationRequired = false;
      try {
        Response verificationResponse = client.target(url.toString()).request().options();
        List<String> authenticateHeader = verificationResponse.getStringHeaders().get(HttpHeaders.WWW_AUTHENTICATE);
        if (Response.Status.UNAUTHORIZED.getStatusCode() == verificationResponse.getStatus()) {
          authenticationRequired = true;
          authHeaders = String.join(",", authenticateHeader);
          //If MAPR ..
          if (authenticateHeader.contains("MAPR-Negotiate")) {
            Class<?> authenticatorClass = Class.forName("com.mapr.security.maprauth.MaprAuthenticator");
            authenticator = (Authenticator) authenticatorClass.newInstance();
          } else {
            //If not Not MAPR, we just use default authenticator.
            hadoopUserGroupInfo = UserGroupInformation.getLoginUser();
            if (hadoopUserGroupInfo.isFromKeytab()) {
              hadoopUserGroupInfo.checkTGTAndReloginFromKeytab();
            }
            authenticator = new KerberosAuthenticator();
          }
        }
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new LogsProviderException("Could not find MAPR Auth class", e);
      } catch (IOException e) {
        throw new LogsProviderException("Error performing kerberos login", e);
      }
      logger.info("Authentication for Nodemanager enabled : '{}', and scheme(s): '{}'" , authenticationRequired, authHeaders);
    }
    HttpURLConnection httpUrlConnection;
    try {
      if (authenticationRequired) {
        AuthenticatedURL.Token token = new AuthenticatedURL.Token();
        //Setup connection configurator to enable/disable cert validation.
        final AuthenticatedURL authenticatedURL = new AuthenticatedURL(authenticator, httpURLConnection -> {
          if (httpURLConnection instanceof HttpsURLConnection && !certificateValidationEnabled) {
            ((HttpsURLConnection) httpURLConnection).setSSLSocketFactory(insecureSslContext.getSocketFactory());
          }
          httpURLConnection.setConnectTimeout(1_000); // connection timeout in 1 sec
          httpURLConnection.setReadTimeout(5_000); // read timeout in 5 sec
          return httpURLConnection;
        });
        if (hadoopUserGroupInfo == null) {
          httpUrlConnection = authenticatedURL.openConnection(url, token);
        } else {
          httpUrlConnection = hadoopUserGroupInfo.doAs(
            (PrivilegedExceptionAction<HttpURLConnection>) () -> authenticatedURL.openConnection(url, token));
        }
      } else {
        httpUrlConnection = (HttpURLConnection) url.openConnection();
      }
      httpUrlConnection.setConnectTimeout(1_000); // connection timeout in 1 sec
      httpUrlConnection.setReadTimeout(5_000); // read timeout in 5 sec
    } catch (AuthenticationException e) {
      throw new LogsProviderException("Error opening authenticated connection to url: " + url, e);
    } catch (InterruptedException e) {
      throw new LogsProviderException("Error opening authenticated connection using UGI to url: " + url, e);
    } catch (IOException e) {
      throw new LogsProviderException("Error opening connection to url: " + url, e);
    }
    return httpUrlConnection;
  }

  /**
   * Exception class to communicate errors
   */
  public static class LogsProviderException extends Exception {
    public LogsProviderException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Store info needed to locate a yarn log
   */
  public static class YarnLogLocator {
    private final String nodeHttpAddress;
    private final String containerId;
    private final String logType;
    private final String outputDir;

    public YarnLogLocator(String nodeHttpAddress, String containerId, String logType, String outputDir) {
      this.nodeHttpAddress = nodeHttpAddress;
      this.containerId = containerId;
      this.logType = logType;
      this.outputDir = outputDir;
    }

    public String getNodeHttpAddress() {
      return nodeHttpAddress;
    }

    public String getContainerId() {
      return containerId;
    }

    public String getLogType() {
      return logType;
    }

    public String getOutputDir() {
      return outputDir;
    }

    @Override
    public String toString() {
      return String.format("[yarn node address: %s, container id: %s, log type: %s, output directory: %s]",
        nodeHttpAddress, containerId, logType, outputDir);
    }
  }
}
