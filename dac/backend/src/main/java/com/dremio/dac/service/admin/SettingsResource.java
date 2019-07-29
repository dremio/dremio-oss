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
package com.dremio.dac.service.admin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * Resource for changing system settings
 */
@RestResource
@Path("/settings")
@Secured
@RolesAllowed({"admin"})
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class SettingsResource {


  private final SystemOptionManager options;

  @Inject
  public SettingsResource(SabotContext context) {
    this.options = context.getOptionManager();
  }

  @POST
  public SettingsWrapperObject list
    (/* Body */ SettingsRequest request) {
    Preconditions.checkNotNull(request, "request could not be null");

    Set<String> requiredSettings = request.getRequiredSettings();
    if (requiredSettings == null) {
      requiredSettings = Collections.emptySet();
    }

    List<Setting> settings = new ArrayList<>();
    if (requiredSettings.size() != 0 || request.getIncludeSetSettings()) {
      for (OptionValue optionValue : options) {
        if (requiredSettings.contains(optionValue.getName()) ||
          (request.getIncludeSetSettings() && options.isSet(optionValue.getName()))) {
          settings.add(toSetting(optionValue));
        }
      }
    }
    return new SettingsWrapperObject(settings);
  }

  /**
   * A request for list of settings
   */
  public static class SettingsRequest {
    private final Set<String> requiredSettings;
    private final boolean includeSetSettings;

    @JsonCreator
    public SettingsRequest(@JsonProperty("requiredSettings") Set<String> requiredSettings,
                           @JsonProperty("includeSetSettings") boolean includeSetSettings) {
      this.requiredSettings = requiredSettings;
      this.includeSetSettings = includeSetSettings;
    }

    public Set<String> getRequiredSettings() {
      return requiredSettings;
    }

    public boolean getIncludeSetSettings() {
      return includeSetSettings;
    }
  }

  /**
   * Exists until ui has better lists of entity handling.
   */
  public static class SettingsWrapperObject {
    private final List<Setting> settings;

    @JsonCreator
    public SettingsWrapperObject(@JsonProperty("settings") List<Setting> settings) {
      super();
      this.settings = settings;
    }

    public List<Setting> getSettings() {
      return settings;
    }

  }

  @GET
  @Path("{id}")
  public Response getSetting(@PathParam("id") String id) {
    if(!options.isValid(id)){
      return Response.status(Status.NOT_FOUND).build();
    }
    return Response.ok(toSetting(options.getOption(id))).build();
  }

  @PUT
  @Path("{id}")
  public Response setSetting(
      Setting updatedSetting,
      @PathParam("id") String id) {
    if(!options.isValid(id)){
      return Response.status(Status.NOT_FOUND).build();
    }

    OptionValue optionValue = toOptionValue(updatedSetting);
    options.setOption(optionValue);
    return Response.ok(toSetting(options.getOption(id))).build();
  }

  @DELETE
  @Path("{id}")
  public Response resetSetting(@PathParam("id") String id) {
    if(!options.isValid(id)){
      return Response.status(Status.NOT_FOUND).build();
    }

    OptionValue option = options.getOption(id);

    options.deleteOption(id, option.getType());
    return Response.ok().build();
  }

  private OptionValue toOptionValue(Setting setting){
    if (setting instanceof Setting.BooleanSetting) {
      return OptionValue.createBoolean(OptionType.SYSTEM, setting.getId(), ((Setting.BooleanSetting)setting).getValue());
    } else if(setting instanceof Setting.FloatSetting) {
      return OptionValue.createDouble(OptionType.SYSTEM, setting.getId(), ((Setting.FloatSetting)setting).getValue());
    } else if(setting instanceof Setting.IntegerSetting) {
      return OptionValue.createLong(OptionType.SYSTEM, setting.getId(), ((Setting.IntegerSetting)setting).getValue());
    } else if(setting instanceof Setting.TextSetting) {
      return OptionValue.createString(OptionType.SYSTEM, setting.getId(), ((Setting.TextSetting)setting).getValue());
    } else {
      throw new IllegalStateException("Unable to handle setting " + setting );
    }
  }

  @SuppressWarnings("rawtypes")
  private Setting toSetting(OptionValue option){

    switch(option.getKind()){
    case BOOLEAN:
      return new Setting.BooleanSetting(option.getName(), option.getBoolVal());
    case DOUBLE:
      return new Setting.FloatSetting(option.getName(), option.getFloatVal());
    case LONG:
      return new Setting.IntegerSetting(option.getName(), option.getNumVal());
    case STRING:
      return new Setting.TextSetting(option.getName(), option.getStringVal());
    default:
      throw new IllegalStateException("Unable to handle kind " + option.getKind());
    }
  }
}
