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
package com.dremio.dac.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.List;
import java.util.Optional;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.service.errors.ServerErrorException;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.annotations.VisibleForTesting;

/**
 * Resource for information about sources.
 */
@APIResource
@Secured
@Path("/source")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class SourceResource {
  private static final Logger logger = LoggerFactory.getLogger(SourceResource.class);

  /**
   * 1.5 changed _type to entityType, this class provides backwards compatibility
   */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_type", defaultImpl = SourceDeprecated.class)
  @JsonSubTypes({
    @JsonSubTypes.Type(value = SourceDeprecated.class, name = "source")
  })
  public static class SourceDeprecated extends Source {
    public SourceDeprecated() {
    }

    public SourceDeprecated(SourceConfig sourceConfig, AccelerationSettings settings, ConnectionReader reader) {
      super(sourceConfig, settings, reader);
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
      setAccelerationGracePeriodMs(source.getAccelerationGracePeriodMs());
    }

    @JsonProperty("_type")
    public String getDeprecatedEntityType() {
      return "source";
    }
  }

  private final SourceService sourceService;
  private final SabotContext sabotContext;

  @Inject
  public SourceResource(SourceService sourceService, SabotContext sabotContext) {
    this.sourceService = sourceService;
    this.sabotContext = sabotContext;
  }

  @GET
  @RolesAllowed({"admin", "user"})
  public ResponseList<Source> getSources() {
    final ResponseList<Source> sources = new ResponseList<>();

    final List<SourceConfig> sourceConfigs = sourceService.getSources();
    for (SourceConfig sourceConfig : sourceConfigs) {
      Source source = fromSourceConfig(sourceConfig);

      sources.add(source);
    }

    return sources;
  }

  @POST
  @RolesAllowed({"admin"})
  public SourceDeprecated addSource(SourceDeprecated source) {
    try {
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
      sourceConfig = sourceService.updateSource(id, source.toSourceConfig());

      return fromSourceConfig(sourceConfig);
    } catch (NamespaceException | ExecutionSetupException e) {
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
  @RolesAllowed("admin")
  @Path("/type")
  public ResponseList<SourceTypeTemplate> getSourceTypes() {
    final ConnectionReader connectionReader = sabotContext.getConnectionReaderProvider().get();
    final ResponseList<SourceTypeTemplate> types = new ResponseList<>();

    for(Class<? extends ConnectionConf<?, ?>> input : connectionReader.getAllConnectionConfs().values()) {
      // we can't use isInternal as its not a static method, instead we only list configurable sources
      if (isConfigurable(input)) {
        types.add(SourceTypeTemplate.fromSourceClass(input, false));
      }
    }

    return types;
  }

  // Returns the specified source type with all its properties expanded
  @GET
  @RolesAllowed("admin")
  @Path("/type/{name}")
  public SourceTypeTemplate getSourceByType(@PathParam("name") String name) {
    final ConnectionReader connectionReader = sabotContext.getConnectionReaderProvider().get();
    Optional<Class<? extends ConnectionConf<?, ?>>> connectionConf = Optional.ofNullable(connectionReader.getAllConnectionConfs().get(name));
    return connectionConf
        .filter(this::isConfigurable)
        .map(sourceClass -> SourceTypeTemplate.fromSourceClass(sourceClass, true))
        .orElseThrow(() -> new NotFoundException(String.format("Could not find source of type [%s]", name)));
  }

  private boolean isConfigurable(Class<? extends ConnectionConf<?, ?>> clazz) {
    SourceType type = clazz.getAnnotation(SourceType.class);
    return type != null && type.configurable();
  }

  @VisibleForTesting
  protected SourceDeprecated fromSourceConfig(SourceConfig sourceConfig) {
    return new SourceDeprecated(sourceService.fromSourceConfig(sourceConfig));
  }
}
