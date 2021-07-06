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
package com.dremio.plugins.elastic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;

import com.dremio.plugins.elastic.ElasticActions.ContextListener;
import com.dremio.plugins.elastic.ElasticActions.ElasticAction;
import com.dremio.plugins.elastic.ElasticActions.ElasticAction2;
import com.dremio.plugins.elastic.ElasticActions.FailureResult;
import com.dremio.plugins.elastic.ElasticActions.JsonResult;
import com.dremio.plugins.elastic.ElasticActions.Result;
import com.google.common.base.Joiner;

public class ElasticTestActions {

  public static class CreateIndex extends ElasticAction {
    private final String index;
    private final int shards;
    private final int replicas;

    public CreateIndex(String index, int shards, int replicas) {
      this.index = index;
      this.shards = shards;
      this.replicas = replicas;
    }

    @Override
    Result getResult(WebTarget target) {
      String settingsString = toString();
      return new JsonResult(target.path(index).request().buildPut(Entity.json(settingsString)).invoke(byte[].class));
    }

    @Override
    public String toString() {
      String format =
          "{\n" +
          "  \"settings\": {\n" +
          "    \"analysis\": {\n" +
          "      \"normalizer\": {\n" +
          "        \"lowercase_normalizer\": {\n" +
          "          \"type\": \"custom\",\n" +
          "          \"filter\": [\n" +
          "            \"lowercase\"\n" +
          "          ]\n" +
          "        }\n" +
          "      }\n" +
          "    },\n" +
          "    \"index\": {\n" +
          "      \"number_of_shards\": %d,\n" +
          "      \"number_of_replicas\": %d\n" +
          "    }\n" +
          "  }\n" +
          "}";
      try {
        return String.format(format, shards, replicas);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public enum AliasActionType {
    ADD,
    REMOVE
  }

  public static class AliasActionDef {
    final AliasActionType actionType;
    final String index;
    final String alias;

    public AliasActionDef(AliasActionType actionType, String index, String alias) {
      this.actionType = actionType;
      this.index = index;
      this.alias = alias;
    }
  }

  public static class CreateAliases extends ElasticAction {
    List<String> aliases = new ArrayList<>();

    public CreateAliases addAlias(String index, String alias) {
      aliases.add(String.format("{ \"add\": {\"index\" : \"%s\", \"alias\": \"%s\" } }", index, alias));
      return this;
    }

    public CreateAliases addAlias(String[] indices, String alias) {
      List<String> quotedIndices = new ArrayList<>();
      for (String index : indices) {
        quotedIndices.add(String.format("\"%s\"", index));
      }
      aliases.add(String.format("{ \"add\": {\"indices\" : [%s], \"alias\": \"%s\" } }", Joiner.on(",").join(quotedIndices), alias));
      return this;
    }

    public CreateAliases addAlias(String[] indices, String alias, String filter) {
      List<String> quotedIndices = new ArrayList<>();
      for (String index : indices) {
        quotedIndices.add(String.format("\"%s\"", index));
      }
      aliases.add(String.format("{ \"add\": {\"indices\" : [%s], \"alias\": \"%s\", \"filter\": %s } }", Joiner.on(",").join(quotedIndices), alias, filter));
      return this;
    }

    public CreateAliases removeAlias(String index, String alias) {
      aliases.add(String.format("{ \"remove\": {\"index\" : \"%s\", \"alias\": \"%s\" } }", index, alias));
      return this;
    }

    public CreateAliases addAlias(String index, String alias, String filter) {
      aliases.add(String.format("{ \"add\": {\"index\" : \"%s\", \"alias\": \"%s\", \"filter\": %s } }", index, alias, filter));
      return this;
    }

    @Override
    public Result getResult(WebTarget target) {
      return new JsonResult(target.path("_aliases").request().buildPost(Entity.json(toString())).invoke(byte[].class));
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("{\n\t\"actions\" : [\n\t\t");
      builder.append(Joiner.on(",\n\t\t").join(aliases));
      builder.append("]\n}");
      return builder.toString();
    }
  }

  public static class PutMapping extends ElasticAction2<byte[]> {
    private String index;
    private String type;
    private String mapping;

    public PutMapping(String index, String type) {
      super("put mapping", byte[].class);
      this.index = index;
      this.type = type;
    }

    public PutMapping setMapping(String mapping) {
      this.mapping = mapping;
      return this;
    }

    @Override
    Invocation buildRequest(WebTarget initial, ContextListener context) {
      WebTarget target = initial.path(index).path("_mapping").path(type);
      context.addContext(target);
      return target.request().buildPut(Entity.json(mapping));
    }

    @Override
    Invocation buildRequest(WebTarget initial, ContextListener context, boolean enable7vFeatures) {
      WebTarget target = initial.path(index).path("_mapping").path(type).queryParam("include_type_name", true);
      context.addContext(target);
      return target.request().buildPut(Entity.json(mapping));
    }
  }

  public static class Bulk extends ElasticAction {
    private List<String> requests = new ArrayList<>();

    public Bulk add(String request) {
      requests.add(request);
      return this;
    }

    @Override
    public Result getResult(WebTarget target) {
      String bulkRequest = toString();
      return new JsonResult(target.path("_bulk").queryParam("refresh", true).request().buildPost(Entity.json(bulkRequest)).invoke(byte[].class));
    }

    @Override
    public String toString() {
      return Joiner.on("\n").join(requests) + "\n";
    }
  }

  public static class DeleteIndex extends ElasticAction {
    private String index;

    public DeleteIndex(String index) {
      this.index = index;
    }

    @Override
    public Result getResult(WebTarget target) {
      return new JsonResult(target.path(index).request().buildDelete().invoke(byte[].class));
    }
  }

  public static class GetMapping extends ElasticAction {
    private List<String> indexes = new ArrayList<>();
    private List<String> types = new ArrayList<>();

    public GetMapping addIndex(String index) {
      indexes.add(index);
      return this;
    }

    public GetMapping addIndexes(Collection<String> indexes) {
      this.indexes.addAll(indexes);
      return this;
    }

    public GetMapping addType(String type) {
      types.add(type);
      return this;
    }

    @Override
    public Result getResult(WebTarget target) {
      WebTarget t = target;
      target.path(Joiner.on(",").join(indexes)).path("_mapping");
      if (indexes.size() > 0) {
        t = t.path(Joiner.on(",").join(indexes));
      }
      t = t.path("_mapping");
      if (types.size() > 0) {
        t = t.path(Joiner.on(",").join(types));
      }
      try {
        return new JsonResult(t.request().buildGet().invoke(byte[].class));
      } catch (WebApplicationException e) {
        return new FailureResult(e.getResponse().getStatus(), e.getMessage());
      }
    }
  }
}
