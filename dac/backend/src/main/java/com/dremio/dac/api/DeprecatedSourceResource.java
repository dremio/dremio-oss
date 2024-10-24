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
package com.dremio.dac.api;

import static com.dremio.dac.options.UIOptions.ALLOW_HIVE_SOURCE;
import static com.dremio.exec.store.DataplanePluginOptions.NESSIE_PLUGIN_ENABLED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_ENABLED;
import static com.dremio.exec.store.jdbc.JdbcPluginOptions.JDBC_OPENSEARCH_ENABLED;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.service.errors.ServerErrorException;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SourceVerifier;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/** Deprecated resource for information about sources. */
@APIResource
@Secured
@Path("/source")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Deprecated
public class DeprecatedSourceResource {
  /** 1.5 changed _type to entityType, this class provides backwards compatibility */
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.PROPERTY,
      property = "_type",
      defaultImpl = SourceDeprecated.class)
  @JsonSubTypes({@JsonSubTypes.Type(value = SourceDeprecated.class, name = "source")})
  public static class SourceDeprecated extends Source {
    public SourceDeprecated() {}

    public SourceDeprecated(
        SourceConfig sourceConfig,
        AccelerationSettings settings,
        ConnectionReader reader,
        List<CatalogItem> children,
        @Nullable String nextPageToken,
        SourceState state) {
      super(sourceConfig, settings, reader, children, nextPageToken, state);
    }

    public SourceDeprecated(Source source) {
      setId(source.getId());
      setConfig(source.getConfig());
      setState(source.getState());
      setTag(source.getTag());
      setType(source.getType());
      setName(source.getName());
      setDescription(source.getDescription());
      setCreatedAt(source.getCreatedAt());
      setMetadataPolicy(source.getMetadataPolicy());
      setAccelerationRefreshPeriodMs(source.getAccelerationRefreshPeriodMs());
      setAccelerationNeverExpire(source.isAccelerationNeverExpire());
      setAccelerationNeverRefresh(source.isAccelerationNeverRefresh());
      setAccelerationRefreshSchedule(source.getAccelerationRefreshSchedule());
      setAccelerationActivePolicyType(source.getAccelerationActivePolicyType());
      setAccelerationGracePeriodMs(source.getAccelerationGracePeriodMs());
    }

    @JsonProperty("_type")
    public String getDeprecatedEntityType() {
      return "source";
    }
  }

  private final SourceService sourceService;
  private final OptionManager optionManager;
  private final Provider<SourceVerifier> sourceVerifierProvider;
  private final Provider<ConnectionReader> connectionReaderProvider;

  @Inject
  public DeprecatedSourceResource(
      SourceService sourceService,
      OptionManager optionManager,
      Provider<SourceVerifier> sourceVerifierProvider,
      Provider<ConnectionReader> connectionReaderProvider) {
    this.sourceService = sourceService;
    this.optionManager = optionManager;
    this.sourceVerifierProvider = sourceVerifierProvider;
    this.connectionReaderProvider = connectionReaderProvider;
  }

  @GET
  @RolesAllowed({"admin", "user"})
  public ResponseList<Source> getSources() {
    final ResponseList<Source> sources = new ResponseList<>();

    final List<SourceConfig> sourceConfigs = sourceService.getSources();
    for (SourceConfig sourceConfig : sourceConfigs) {
      Source source = fromSourceConfig(sourceConfig);

      if (sourceVerifierProvider.get().isSourceSupported(source.getType(), optionManager)) {
        sources.add(source);
      }
    }

    return sources;
  }

  @POST
  @RolesAllowed({"admin"})
  public SourceDeprecated addSource(SourceDeprecated source) {
    try {
      if (!sourceVerifierProvider.get().isSourceSupported(source.getType(), optionManager)) {
        throw new IllegalArgumentException(source.getType() + " source type is not supported.");
      }

      SourceConfig newSourceConfig = sourceService.createSource(source.toSourceConfig());

      return fromSourceConfig(newSourceConfig);
    } catch (NamespaceException | ExecutionSetupException e) {
      throw new ServerErrorException(e);
    }
  }

  @GET
  @RolesAllowed({"admin", "user"})
  @Path("/{id}")
  public SourceDeprecated getSource(@PathParam("id") String id) throws NamespaceException {
    SourceConfig sourceConfig = sourceService.getById(id);

    return fromSourceConfig(sourceConfig);
  }

  @PUT
  @RolesAllowed({"admin"})
  @Path("/{id}")
  public SourceDeprecated updateSource(@PathParam("id") String id, SourceDeprecated source) {
    SourceConfig sourceConfig;
    try {
      if (!sourceVerifierProvider.get().isSourceSupported(source.getType(), optionManager)) {
        throw new IllegalArgumentException(source.getType() + " source type is not supported.");
      }

      sourceConfig = sourceService.updateSource(id, source.toSourceConfig());

      return fromSourceConfig(sourceConfig);
    } catch (NamespaceException e) {
      throw new ServerErrorException(e);
    }
  }

  @DELETE
  @RolesAllowed("admin")
  @Path("/{id}")
  public Response deleteSource(@PathParam("id") String id) throws NamespaceException {
    SourceConfig config = sourceService.getById(id);
    sourceService.deleteSource(config);
    return Response.ok().build();
  }

  // Returns all source types in Dremio
  @GET
  @RolesAllowed({"admin", "user"})
  @Path("/type")
  public ResponseList<SourceTypeTemplate> getSourceTypes() {
    final ConnectionReader connectionReader = connectionReaderProvider.get();
    final ResponseList<SourceTypeTemplate> types = new ResponseList<>();

    for (Class<? extends ConnectionConf<?, ?>> input :
        connectionReader.getAllConnectionConfs().values()) {
      // We can't use isInternal as it's not a static method, instead we only show listable sources.
      if (isListable(input)) {
        String sourceType = input.getAnnotation(SourceType.class).value();
        if (isSourceTypeVisible(sourceType)
            && sourceVerifierProvider.get().isSourceSupported(sourceType, optionManager)) {
          types.add(SourceTypeTemplate.fromSourceClass(input, false));
        }
      }
    }

    return types;
  }

  private boolean isSourceTypeVisible(String sourceType) {
    switch (sourceType) {
      case "HIVE":
        return optionManager.getOption(ALLOW_HIVE_SOURCE);
      case "OPENSEARCH":
        return optionManager.getOption(JDBC_OPENSEARCH_ENABLED);
      case "NESSIE":
        return optionManager.getOption(NESSIE_PLUGIN_ENABLED);
      case "RESTCATALOG":
        return optionManager.getOption(RESTCATALOG_PLUGIN_ENABLED);
      default:
        return true;
    }
  }

  // Returns the specified source type with all its properties expanded
  @GET
  @RolesAllowed({"admin", "user"})
  @Path("/type/{name}")
  public SourceTypeTemplate getSourceByType(@PathParam("name") String name) {
    final Optional<Class<? extends ConnectionConf<?, ?>>> connectionConf = getConnectionConf(name);

    return connectionConf
        .filter(this::isConfigurable)
        .map(sourceClass -> SourceTypeTemplate.fromSourceClass(sourceClass, true))
        .orElseThrow(
            () -> new NotFoundException(String.format("Could not find source of type [%s]", name)));
  }

  protected Optional<Class<? extends ConnectionConf<?, ?>>> getConnectionConf(String name) {
    final ConnectionReader connectionReader = connectionReaderProvider.get();
    return Optional.ofNullable(connectionReader.getAllConnectionConfs().get(name));
  }

  protected boolean isConfigurable(Class<? extends ConnectionConf<?, ?>> clazz) {
    SourceType type = clazz.getAnnotation(SourceType.class);
    return type != null && type.configurable();
  }

  private boolean isListable(Class<? extends ConnectionConf<?, ?>> clazz) {
    SourceType type = clazz.getAnnotation(SourceType.class);
    return type != null && type.configurable() && type.listable();
  }

  @VisibleForTesting
  protected SourceDeprecated fromSourceConfig(SourceConfig sourceConfig) {
    return new SourceDeprecated(sourceService.fromSourceConfig(sourceConfig));
  }
}
