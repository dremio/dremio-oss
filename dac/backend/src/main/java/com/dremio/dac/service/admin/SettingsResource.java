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

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.resource.PowerBIResource;
import com.dremio.dac.resource.TableauResource;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

/** Resource for changing system settings */
@RestResource
@Path("/settings")
@Secured
@RolesAllowed({"admin"})
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Options
public class SettingsResource {
  private static final Set<String> CLIENT_TOOL_OPTIONS =
      ImmutableSet.of(
          TableauResource.CLIENT_TOOLS_TABLEAU.getOptionName(),
          PowerBIResource.CLIENT_TOOLS_POWERBI.getOptionName());

  private final ProjectOptionManager projectOptionManager;

  private static final Map<String, SettingValidator> VALIDATORS =
      ImmutableMap.of(
          "support.email.addr",
          (id, setting) -> {
            // this pattern is for validating multiple emails separated by commas.
            Pattern pattern =
                Pattern.compile(
                    "^([\\w+-.%]+@[\\w-.]+\\.[A-Za-z]{2,4})(\\s*,\\s*[\\w+-.%]+@[\\w-.]+\\.[A-Za-z]{2,4})*$");
            Matcher matcher = pattern.matcher(setting.getValue().toString().trim());
            if (!matcher.find()) {
              throw new IllegalArgumentException(
                  "Emails must be in correct format separated by comma.");
            }
          });

  @Inject
  public SettingsResource(ProjectOptionManager projectOptionManager) {
    this.projectOptionManager = projectOptionManager;
    initializeClientTooloptions(
        ImmutableSet.of(TableauResource.CLIENT_TOOLS_TABLEAU, PowerBIResource.CLIENT_TOOLS_POWERBI),
        projectOptionManager);
  }

  @VisibleForTesting
  public static SettingValidator getSettingValidator(String id) {
    return VALIDATORS.get(id);
  }

  @POST
  public Response list(/* Body */ SettingsRequest request) {
    Preconditions.checkNotNull(request, "request could not be null");

    Set<String> requiredSettings = request.getRequiredSettings();
    if (requiredSettings == null) {
      requiredSettings = Collections.emptySet();
    }

    List<Setting> settings = new ArrayList<>();
    if (requiredSettings.size() != 0 || request.getIncludeSetSettings()) {
      for (OptionValue optionValue : projectOptionManager) {
        if (requiredSettings.contains(optionValue.getName())
            || (request.getIncludeSetSettings()
                && projectOptionManager.isSet(optionValue.getName()))) {
          settings.add(toSetting(optionValue));
        }
      }
    }
    return Response.ok(new SettingsWrapperObject(settings)).build();
  }

  /** A request for list of settings */
  public static class SettingsRequest {
    private final Set<String> requiredSettings;
    private final boolean includeSetSettings;

    @JsonCreator
    public SettingsRequest(
        @JsonProperty("requiredSettings") Set<String> requiredSettings,
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

  /** Exists until ui has better lists of entity handling. */
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
    if (!projectOptionManager.isValid(id)) {
      return Response.status(Status.NOT_FOUND).build();
    }
    return Response.ok(toSetting(projectOptionManager.getOption(id))).build();
  }

  @PUT
  @Path("{id}")
  public Response setSetting(Setting updatedSetting, @PathParam("id") String id) {
    if (!projectOptionManager.isValid(id)) {
      return Response.status(Status.NOT_FOUND).build();
    }

    if (VALIDATORS.get(id) != null) {
      try {
        VALIDATORS.get(id).validateSetting(id, updatedSetting);
      } catch (Exception e) {
        return Response.status(Status.BAD_REQUEST)
            .entity(new GenericErrorMessage(e.getMessage(), null, null))
            .build();
      }
    }

    OptionValue optionValue = toOptionValue(updatedSetting);
    projectOptionManager.setOption(optionValue);
    return Response.ok(toSetting(projectOptionManager.getOption(id))).build();
  }

  @DELETE
  @Path("{id}")
  public Response resetSetting(@PathParam("id") String id) {
    if (!projectOptionManager.isValid(id)) {
      return Response.status(Status.NOT_FOUND).build();
    }

    // Client tool options should not be removable.
    if (CLIENT_TOOL_OPTIONS.contains(id)) {
      return Response.status(Status.BAD_REQUEST).build();
    }

    OptionValue option = projectOptionManager.getOption(id);

    projectOptionManager.deleteOption(id, option.getType());
    return Response.ok().build();
  }

  private OptionValue toOptionValue(Setting setting) {
    if (setting instanceof Setting.BooleanSetting) {
      return OptionValue.createBoolean(
          OptionType.SYSTEM, setting.getId(), ((Setting.BooleanSetting) setting).getValue());
    } else if (setting instanceof Setting.FloatSetting) {
      return OptionValue.createDouble(
          OptionType.SYSTEM, setting.getId(), ((Setting.FloatSetting) setting).getValue());
    } else if (setting instanceof Setting.IntegerSetting) {
      return OptionValue.createLong(
          OptionType.SYSTEM, setting.getId(), ((Setting.IntegerSetting) setting).getValue());
    } else if (setting instanceof Setting.TextSetting) {
      return OptionValue.createString(
          OptionType.SYSTEM, setting.getId(), ((Setting.TextSetting) setting).getValue());
    } else {
      throw new IllegalStateException("Unable to handle setting " + setting);
    }
  }

  @SuppressWarnings("rawtypes")
  private Setting toSetting(OptionValue option) {

    switch (option.getKind()) {
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

  private static void initializeClientTooloptions(
      Collection<TypeValidators.TypeValidator> options, ProjectOptionManager optionManager) {
    options.forEach(
        option -> {
          if (!optionManager.isSet(option.getOptionName())) {
            optionManager.setOption(option.getDefault());
          }
        });
  }

  @VisibleForTesting
  public interface SettingValidator {
    void validateSetting(String id, Setting setting) throws Exception;
  }
}
