/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.admin;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.io.IOException;
import java.io.StringReader;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.annotations.RestResourceUsedForTesting;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Resource for getting and updating storages using Drill REST API.
 */

// TODO DX-3158 - learn how we can re-enable auth and still get regression to work
//@Secured
//@RolesAllowed({"admin", "user"})
@RestResourceUsedForTesting
@Path("/storage")
public class StorageResource {

  private StoragePluginRegistry pluginConfiguration;
  private final ObjectMapper storagePluginConfigMapper;

  @Inject
  public StorageResource(StoragePluginRegistry pluginConfiguration, SabotContext context) {
    this.storagePluginConfigMapper = context.getLpPersistence().getMapper();
    this.pluginConfiguration = pluginConfiguration;
  }

  @GET
  @Path("/{name}.json")
  @Produces(APPLICATION_JSON)
  public PluginConfigWrapper getStoragePluginJSON(@PathParam("name") String name) {
    try {
      StoragePlugin plugin = pluginConfiguration.getPlugin(name);
      if (plugin != null) {
        return new PluginConfigWrapper(name, plugin.getConfig());
      }
    } catch (Exception e) {
      throw new BadRequestException("Failure while trying to access storage config: " + name, e);
    }
    return new PluginConfigWrapper(name, null);
  }

  @POST
  @Path("/{name}.json")
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public String createOrUpdatePluginJSON(PluginConfigWrapper plugin) {
    try {
      pluginConfiguration.createOrUpdate(plugin.getName(), plugin.getConfig(), null, true);
      return "{\"message\": \"success\"}";
    } catch (ExecutionSetupException e) {
      throw new RuntimeException("Cannot create or update plugin " + plugin.getName(), e);
    }
  }

  @POST
  @Path("/{name}")
  @Consumes("application/x-www-form-urlencoded")
  @Produces(APPLICATION_JSON)
  public String createOrUpdatePlugin(@FormParam("name") String name, @FormParam("config") String storagePluginConfig) {
    try {
      StoragePluginConfig config = storagePluginConfigMapper.readValue(
          new StringReader(storagePluginConfig), StoragePluginConfig.class);
      pluginConfiguration.createOrUpdate(name, config, null, true);
      return "{\"message\": \"success\"}";
    } catch (IOException e) {
      throw new BadRequestException("Malformed storagePluginConfig " + storagePluginConfig, e);
    } catch (ExecutionSetupException e) {
      throw new RuntimeException("Cannot create or update plugin " + name, e);
    }
  }

}
