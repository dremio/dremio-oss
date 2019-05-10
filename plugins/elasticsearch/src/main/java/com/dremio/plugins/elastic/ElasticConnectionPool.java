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
package com.dremio.plugins.elastic;

import static com.dremio.plugins.elastic.ElasticsearchRequestClientFilter.REGION_NAME;
import static java.lang.String.format;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.ResponseProcessingException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import org.glassfish.hk2.utilities.Binder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyInvocation;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.client.filter.EncodingFilter;
import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.message.DeflateEncoder;
import org.glassfish.jersey.message.GZipEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.dremio.common.DeferredException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserException.Builder;
import com.dremio.exec.catalog.conf.Host;
import com.dremio.plugins.Version;
import com.dremio.plugins.elastic.ElasticActions.ContextListener;
import com.dremio.plugins.elastic.ElasticActions.ElasticAction;
import com.dremio.plugins.elastic.ElasticActions.ElasticAction2;
import com.dremio.plugins.elastic.ElasticActions.NodesInfo;
import com.dremio.plugins.elastic.ElasticActions.Result;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet;
import com.dremio.service.namespace.SourceState.Message;
import com.dremio.service.namespace.SourceState.MessageLevel;
import com.dremio.service.namespace.capabilities.BooleanCapabilityValue;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.ssl.SSLHelper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Dremio's connection pool to Elasticsearch. Note that this will actually maintain a
 * connection to all nodes in an elastic cluster.
 */
public class ElasticConnectionPool implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ElasticConnectionPool.class);
  private static final String REQUEST_LOGGER_NAME = "elastic.requests";
  private static final Logger REQUEST_LOGGER = LoggerFactory.getLogger(REQUEST_LOGGER_NAME);

  private static final Version MIN_ELASTICSEARCH_VERSION = new Version(2, 0, 0);

  // Defines a cutoff for enabling newer features that have been added to elasticsearch. Rather than
  // maintain an exact matrix of what is supported, We simply try to make use of all features available
  // above this version and disable them for connections to any version below. This cutoff is inclusive
  // on the ENABLE side so all new features must be available in 2.1.2 and up. Everything missing in
  // a version between 2.0 and 2.1.1 is disabled for all versions in that range.
  public static final Version MIN_VERSION_TO_ENABLE_NEW_FEATURES = new Version(2, 1, 2);

  // Version 5x or higher
  private static final Version ELASTICSEARCH_VERSION_5X = new Version(5, 0, 0);

  //Version 5.3.x or higher
  private static final Version ELASTICSEARCH_VERSION_5_3_X = new Version(5, 3, 0);

  enum TLSValidationMode {
    STRICT,
    VERIFY_CA,
    UNSECURE,
    OFF
  }

  private volatile ImmutableMap<String, WebTarget> clients;
  private Client client;
  private final List<Host> hosts;
  private final TLSValidationMode sslMode;
  private final String protocol;
  private final ElasticsearchAuthentication elasticsearchAuthentication;
  private final int readTimeoutMillis;
  private final boolean useWhitelist;

  /**
   * Rather than maintain an exact matrix of what is supported in each version,
   * we simply try to make use of all features available above a cutoff version
   * and disable them for connections to any version below.
   *
   * Flag to indicate if the current cluster has a high enough version to enable
   * new features. See comment above for strategy on partitioning the features
   * into 2 groups. This is set after a an actual connection is made to the
   * cluster
   *
   * <p>
   * The cutoff is currently configured to 2.1.2.
   */
  private boolean enableNewFeatures;

  /**
   * Flag to indicate if the current cluster has a high enough version for 5x features.  For example, boolean group by is supported for 5x cluster.
   */
  private boolean enable5vFeatures;

  /**
   * Flag to indicate if contains is supported.
   * A typo between v5.0.0 and v5.2.x causes query failure with contains
   */
  private boolean enableContains;

  /**
   * The lowest version found in the cluster.
   */
  private Version minVersionInCluster;


  public ElasticConnectionPool(
      List<Host> hosts,
      TLSValidationMode tlsMode,
      ElasticsearchAuthentication elasticsearchAuthentication,
      int readTimeoutMillis,
      boolean useWhitelist){
    this.sslMode = tlsMode;
    this.protocol = tlsMode != TLSValidationMode.OFF ? "https" : "http";
    this.hosts = ImmutableList.copyOf(hosts);
    this.elasticsearchAuthentication = elasticsearchAuthentication;
    this.readTimeoutMillis = readTimeoutMillis;
    this.useWhitelist = useWhitelist;

  }

  public void connect() throws IOException {
    ClientConfig configuration = new ClientConfig();
    configuration.property(ClientProperties.READ_TIMEOUT, readTimeoutMillis);
    AWSCredentialsProvider awsCredentialsProvider = elasticsearchAuthentication.getAwsCredentialsProvider();
    if (awsCredentialsProvider != null) {
      configuration.property(REGION_NAME, elasticsearchAuthentication.getRegionName());
      configuration.register(ElasticsearchRequestClientFilter.class);
      Binder binder = new AbstractBinder() {
        @Override
        protected void configure() {
          bind(awsCredentialsProvider).to(AWSCredentialsProvider.class);
        }
      };
      configuration.register(binder);
    }

    ClientBuilder builder = ClientBuilder.newBuilder()
        .withConfig(configuration);

    switch(sslMode) {
    case UNSECURE:
      builder.sslContext(SSLHelper.newAllTrustingSSLContext("SSL"));
      // fall-through
    case VERIFY_CA:
      builder.hostnameVerifier(SSLHelper.newAllValidHostnameVerifier());
      // fall-through
    case STRICT:
      break;

    case OFF:
      // no TLS/SSL configuration
    }

    client = builder.build();
    client.register(GZipEncoder.class);
    client.register(DeflateEncoder.class);
    client.register(EncodingFilter.class);

    if (REQUEST_LOGGER.isDebugEnabled()) {
      java.util.logging.Logger julLogger = java.util.logging.Logger.getLogger(REQUEST_LOGGER_NAME);
      client.register(new LoggingFeature(
          julLogger,
          Level.FINE,
          REQUEST_LOGGER.isTraceEnabled() ? LoggingFeature.Verbosity.PAYLOAD_TEXT : LoggingFeature.Verbosity.HEADERS_ONLY,
          65536));
    }

    final JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    provider.setMapper(ElasticMappingSet.MAPPER);
    client.register(provider);
    HttpAuthenticationFeature httpAuthenticationFeature = elasticsearchAuthentication.getHttpAuthenticationFeature();
    if (httpAuthenticationFeature != null) {
      client.register(httpAuthenticationFeature);
    }

    updateClients();
  }

  private void updateClients(){
    final ImmutableMap.Builder<String, WebTarget> builder = ImmutableMap.builder();

    @SuppressWarnings("resource")
    final DeferredException ex = new DeferredException();
    for(Host host1 : hosts){
      try {
        final List<HostAndAddress> hosts = getHostList(host1);
        final Set<String> hostSet = new HashSet<>();



        // for each host, create a separate jest client factory.
        for (HostAndAddress host : hosts) {

            // avoid adding duplicate addresses if on same machine.
            if (!hostSet.add(host.getHost())) {
              continue;
            }
            final String connection = protocol + "://" + host.getHttpAddress();
            builder.put(host.getHost(), client.target(connection));
        }

        this.clients = builder.build();

        if(ex.hasException()){
          StringBuilder sb = new StringBuilder();
          sb.append("One or more failures trying to get host list from a defined seed host. Update was ultimately succesful connecting to ");
          sb.append(host1.toCompound());
          sb.append(":\n\n");
          sb.append(getAvailableHosts());
          logger.info(sb.toString(), ex.getAndClear());
        } else {
          StringBuilder sb = new StringBuilder();
          sb.append("Retrieved Elastic host list from ");
          sb.append(host1.toCompound());
          sb.append(":\n\n");
          sb.append(getAvailableHosts());
          logger.info(sb.toString());
        }
        return;
      }catch (Exception e){
        ex.addException(new RuntimeException(String.format("Failure getting server list from seed host ", host1.toCompound()), e));
      }
    }

    throw new RuntimeException("Unable to get host lists from any host seed.", ex.getAndClear());
  }

  private String getAvailableHosts(){

    if(clients == null){
      return "";
    }

    StringBuilder sb = new StringBuilder();
    for(Entry<String, WebTarget> e : clients.entrySet()){
      sb.append('\t');
      sb.append(e.getKey());
      sb.append(" => ");
      sb.append(e.getValue());
      sb.append('\n');
    }
    return sb.toString();
  }

  public ElasticConnection getRandomConnection(){
    final List<WebTarget> webTargets = clients.values().asList();
    final int index = ThreadLocalRandom.current().nextInt(webTargets.size());
    return new ElasticConnection(webTargets.get(index));
  }

  public ElasticConnection getConnection(List<String> hosts) {
    //if whitelist then only keep the hosts in the list
    if(useWhitelist) {
      List<String> whitelistedHosts = new ArrayList<>();
      for (String host : hosts) {
        if (clients.containsKey(host)) {
          whitelistedHosts.add(host);
        }
      }

      if(whitelistedHosts.size() == 0) {
        return getRandomConnection();
      }

      hosts = whitelistedHosts;
    }

    final int index = ThreadLocalRandom.current().nextInt(hosts.size());
    final String host = hosts.get(index);
    WebTarget target = clients.get(host);
    if(target == null){
      missingHost(host);
      target = clients.get(host);
      if(target == null){
        throw UserException.connectionError()
          .message("Unable to find defined host [%s] in cluster. Available hosts: \n", host, getAvailableHosts())
          .build(logger);
      }
    }
    return new ElasticConnection(Preconditions.checkNotNull(clients.get(host), String.format("The host specified [%s] was not among the list of connected nodes.", host)));
  }

  private synchronized void missingHost(String host){
    // double checked to avoid duplicate updates.
    if(clients.containsKey(host)){
      return;
    }

    logger.info("Shard was located on {}, an Elastic host that is not yet known to Dremio. Updating known hosts.", host);
    updateClients();
  }

  public SourceCapabilities getCapabilities(){
    return new SourceCapabilities(
        new BooleanCapabilityValue(ElasticsearchStoragePlugin.ENABLE_V5_FEATURES, enable5vFeatures),
        new BooleanCapabilityValue(ElasticsearchStoragePlugin.SUPPORTS_NEW_FEATURES, enableNewFeatures),
        new BooleanCapabilityValue(SourceCapabilities.SUPPORTS_CONTAINS, enableContains)
        );
  }

  public Version getMinVersionInCluster(){
    return minVersionInCluster;
  }

  /**
   * Using an initial host, determine the list of available hosts in the cluster.
   *
   * @param initialHost Host to connect to.
   * @return A list of host names and http addresses.
   * @throws IOException
   */
  private List<HostAndAddress> getHostList(Host initialHost) throws IOException {
    final List<HostAndAddress> hosts = new ArrayList<>();

    //check if this is a valid host if whitelist is enabled
    if(useWhitelist) {
      for (Host givenHost : this.hosts) {
        hosts.add(new HostAndAddress(givenHost.hostname, givenHost.toCompound()));
      }
    }

    final Result nodesResult;

    NodesInfo nodesInfo = new NodesInfo();
    nodesResult = new ElasticConnection(client.target(protocol + "://" + initialHost)).executeAndHandleResponseCode(nodesInfo, true,
        "Cannot gather Elasticsearch nodes information. Please make sure that the user has [cluster:monitor/nodes/info] privilege.");

    minVersionInCluster = null;
    JsonObject nodes = nodesResult.getAsJsonObject().getAsJsonObject("nodes");

    List<Message> nodeVersionInformation = new ArrayList<>();
    for (Entry<String, JsonElement> entry : nodes.entrySet()) {
      final JsonObject nodeObject = entry.getValue().getAsJsonObject();
      final String host;
      if (useWhitelist) {
        host = nodeObject.get("name").getAsString();
      } else {
        host = nodeObject.get("host").getAsString();
      }
      Version hostVersion = Version.parse(nodeObject.get("version").getAsString());
      if (minVersionInCluster == null || minVersionInCluster.compareTo(hostVersion) > 0) {
        minVersionInCluster = hostVersion;
      }
      nodeVersionInformation.add(new Message(MessageLevel.INFO, entry.getKey() + ", " + host + ", Elasticsearch version: " + hostVersion));
    }
    if (minVersionInCluster == null) {
      throw UserException.connectionError().message("Unable to get Elasticsearch node version information.").build(logger);
    }

    if (!useWhitelist) {
      for (Entry<String, JsonElement> entry : nodes.entrySet()) {
        final JsonObject nodeObject = entry.getValue().getAsJsonObject();
        final String host = nodeObject.get("host").getAsString();

        // From 5.0.0v onwards, elasticsearch nodes api has changed and returns a slightly different response.
        // https://github.com/elastic/elasticsearch/pull/19218
        // So instead of using http_address, use publish_address always.
        final JsonObject httpObject = nodeObject.get("http").getAsJsonObject();
        final String httpOriginal = httpObject.get("publish_address").getAsString();

        // DX-4266  Depending on the settings, http_address can return the following, and we should handle them properly
        // <ip>:<port>
        // <hostname>/<ip>:<port>
        // This should not apply to publish_address, which we are now using above, but as a precaution, keeping the below code
        final String[] httpOriginalAddresses = httpOriginal.split("/");
        String http = null;
        if (httpOriginalAddresses != null && httpOriginalAddresses.length >= 1) {
          for (String oneAddress : httpOriginalAddresses) {
            if (oneAddress.contains(":") && oneAddress.indexOf(":") < oneAddress.length() - 1) {
              http = oneAddress;
              break;
            }
          }
        }
        if (http == null) {
          throw new RuntimeException("Could not parse the _nodes information from Elasticserach cluster, found invalid http_address " + httpOriginal);
        }

        hosts.add(new HostAndAddress(host, http));

      }
    }

    // Assert minimum version for Elasticsearch
    if (minVersionInCluster.compareTo(MIN_ELASTICSEARCH_VERSION) < 0) {
      throw new RuntimeException(
          format("Dremio only supports Elasticsearch versions above %s. Found a node with version %s",
              MIN_ELASTICSEARCH_VERSION, minVersionInCluster));
    }

    enableNewFeatures = minVersionInCluster.compareTo(MIN_VERSION_TO_ENABLE_NEW_FEATURES) >= 0;

    enable5vFeatures = minVersionInCluster.compareTo(ELASTICSEARCH_VERSION_5X) >= 0;

    enableContains = minVersionInCluster.compareTo(ELASTICSEARCH_VERSION_5_3_X) >= 0;

    return hosts;
  }

  private class HostAndAddress {
    private final String host;
    private final String httpAddress;

    public HostAndAddress(String host, String httpAddress) {
      super();
      this.host = host;
      this.httpAddress = httpAddress;
    }

    public String getHost() {
      return host;
    }

    public String getHttpAddress() {
      return httpAddress;
    }
  }

  private static void addContextAndThrow(Builder userExceptionBuilder, List<String> context) {
    if (context != null && !context.isEmpty()) {
      Preconditions.checkArgument(context.size() % 2 == 0);
      ListIterator<String> iterator = context.listIterator();
      while (iterator.hasNext()) {
        userExceptionBuilder.addContext(iterator.next(), iterator.next());
      }
    }
    throw userExceptionBuilder.build(logger);
  }

  private static class ContextInfo {
    private String name;
    private String message;
    private Object[] values;

    public ContextInfo(String name, String message, Object[] values) {
      super();
      this.name = name;
      this.message = message;
      this.values = values;
    }

  }

  private static class ContextListenerImpl implements ContextListener {

    private WebTarget target;
    private final List<ContextInfo> contexts = new ArrayList<>();

    @Override
    public ContextListener addContext(WebTarget target) {
      this.target = target;
      return this;
    }

    @Override
    public ContextListener addContext(String context, String message, Object...args) {
      contexts.add(new ContextInfo(context, message, args));
      return this;
    }

    UserException.Builder addContext(UserException.Builder builder){
      if(target != null){
        builder.addContext("Request: " + target.getUri());
      }

      for(ContextInfo i : contexts){
        builder.addContext(i.name, String.format(i.message, i.values));
      }

      return builder;
    }

  }

  private static UserException.Builder addResponseInfo(UserException.Builder builder, Response response) {
    try {
      builder.addContext("Response Status", response.getStatusInfo().getStatusCode());
      builder.addContext("Response Reason", response.getStatusInfo().getReasonPhrase());
      String string = CharStreams.toString(new InputStreamReader((InputStream) response.getEntity()));
      builder.addContext("Response Body", string);
    } catch(Exception ex){
      logger.warn("Failure while decoding exception", ex);
    }
    return builder;
  }

  private static UserException handleException(Exception e, ElasticAction2<?> action, ContextListenerImpl listener) {
    if (e instanceof ResponseProcessingException) {
      final UserException.Builder builder = UserException.dataReadError(e)
          .message("Failure consuming response from Elastic cluster during %s.", action.getAction());

      listener.addContext(builder);

      final Response response = ((ResponseProcessingException) e).getResponse();
      if(response != null) {
        addResponseInfo(builder, response);
      }
      return builder.build(logger);
    }

    if (e instanceof WebApplicationException) {
      final UserException.Builder builder;
      final Response response = ((WebApplicationException) e).getResponse();
      if (response == null) {
        builder = UserException.dataReadError(e)
            .message("Failure executing Elastic request %s.", action.getAction());

        listener.addContext(builder);
      } else {
        switch (Response.Status.fromStatusCode(response.getStatus())) {
        case NOT_FOUND: { // index not found
          builder = UserException.invalidMetadataError(e)
              .message("Failure executing Elastic request %s: HTTP 404 Not Found.", action.getAction());
          break;
        }
        default:
          builder = UserException.dataReadError(e)
              .message("Failure executing Elastic request %s: HTTP %d.", action.getAction(), response.getStatus());
          break;
        }

        listener.addContext(builder);
        addResponseInfo(builder, response);
      }

      return builder.build(logger);
    }

    return listener.addContext(UserException.dataReadError(e)
        .message("Failure executing Elastic request %s.", action.getAction()))
        .build(logger);
  }

  private static class AsyncCallback<T> implements InvocationCallback<T> {

    private final SettableFuture<T> future;

    public AsyncCallback(SettableFuture<T> future) {
      super();
      this.future = future;
    }

    @Override
    public void completed(T response) {
      future.set(response);
    }

    @Override
    public void failed(Throwable throwable) {
      future.setException(throwable);
    }

  }

  public class ElasticConnection {

    private final WebTarget target;

    public ElasticConnection(WebTarget target) {
      super();
      this.target = target;
    }

    public <T> CheckedFuture<T, UserException> executeAsync(final ElasticAction2<T> action){
      final ContextListenerImpl listener = new ContextListenerImpl();
      // need to cast to jersey since the core javax.ws.rs Invocation doesn't support a typed submission.
      final JerseyInvocation invocation = (JerseyInvocation) action.buildRequest(target, listener);
      final SettableFuture<T> future = SettableFuture.create();
      invocation.submit(new GenericType<T>(action.getResponseClass()), new AsyncCallback<>(future));
      return Futures.makeChecked(future, new Function<Exception, UserException>(){
        @Override
        public UserException apply(Exception input) {
          if(input instanceof ExecutionException){
            input = (Exception) input.getCause();
          }
          return handleException(input, action, listener);
        }});
    }

    public <T> T execute(ElasticAction2<T> action){
      final ContextListenerImpl listener = new ContextListenerImpl();
      final Invocation invocation = action.buildRequest(target, listener);
      try {
        return invocation.invoke(action.getResponseClass());
      } catch (Exception e){
        throw handleException(e, action, listener);
      }

    }

    public Result executeAndHandleResponseCode(ElasticAction action, boolean handleResponseCode, String unauthorizedMsg, String... context) {

      Result result = null;
      final List<String> contextWithAlias = Lists.newArrayList(context);
      contextWithAlias.add("action");
      contextWithAlias.add(action.toString());
      contextWithAlias.add("base url");
      contextWithAlias.add(target.getUri().toString());
      try {
        result = action.getResult(target);
      } catch (Exception e) {
        addContextAndThrow(UserException.connectionError(e).message("Encountered a problem while executing %s. %s", action, unauthorizedMsg), contextWithAlias);
      }

      if (result == null) {
        addContextAndThrow(UserException.connectionError().message("Something went wrong. "
            + "Please make sure the source is available. %s", unauthorizedMsg), contextWithAlias);
      }

      if (!handleResponseCode) {
        return result;
      }

      if (result.success()) {
        return result;
      } else {
        logger.warn("Error encountered: " + result.getErrorMessage());

        final int responseCode = result.getResponseCode();
        if (responseCode == 401) {
          addContextAndThrow(UserException.permissionError().message("Unauthorized to connect to Elasticsearch cluster. "
              + "Please make sure that the username and the password provided are correct."), contextWithAlias);
        }
        if (responseCode == 403) {
          addContextAndThrow(UserException.permissionError().message(unauthorizedMsg), contextWithAlias);
        }
        contextWithAlias.add("error message");
        contextWithAlias.add(result.getErrorMessage());
        addContextAndThrow(
            UserException.connectionError()
                .message("Something went wrong, error code " + responseCode + ".  Please provide the correct host and credentials.")
                .addContext("responseCode", responseCode),
            contextWithAlias
        );

        // will not reach here
        return result;
      }
    }

    @Deprecated
    public WebTarget getTarget(){
      return target;
    }
  }

  @Override
  public void close(){
    if(client != null){
      client.close();
    }
  }
}
