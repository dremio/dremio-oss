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
package com.dremio.plugins.elastic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import com.dremio.plugins.elastic.ElasticActions.ElasticAction;
import com.dremio.plugins.elastic.ElasticActions.Result;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet.ClusterMetadata;
import com.google.common.base.Joiner;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ElasticActions {

  static JsonParser parser = new JsonParser();

  private static JsonObject parse(String string) {
    return parser.parse(string).getAsJsonObject();
  }

  public static abstract class ElasticAction {
    /**
     * Interface for the connection pool to generate request.
     * @param target
     * @return
     */
    abstract Result getResult(WebTarget target);
  }

  public static abstract class Result {
    public JsonObject getAsJsonObject() {
      throw new UnsupportedOperationException();
    }

    public JsonArray getAsJsonArray() {
      throw new UnsupportedOperationException();
    }

    public int getAsInt() {
      throw new UnsupportedOperationException();
    }

    public long getAsLong() {
      throw new UnsupportedOperationException();
    }

    public String getErrorMessage() {
      throw new UnsupportedOperationException();
    }

    public int getResponseCode() {
      throw new UnsupportedOperationException();
    }

    public byte[] getBytes() {
      throw new UnsupportedOperationException();
    }

    public <T> T getAsObject(){
      throw new UnsupportedOperationException();
    }

    public boolean success() {
      return true;
    }
  }

  public static class JsonResult extends Result {
    private byte[] json;

    public JsonResult(byte[] json) {
      this.json = json;
    }

    @Override
    public JsonObject getAsJsonObject() {
      return parse(new String(json));
    }

    @Override
    public byte[] getBytes() {
      return json;
    }
  }

  public static class FailureResult extends Result {
    private int status;
    private String errorMessage;

    public FailureResult(int status, String errorMessage) {
      this.status = status;
      this.errorMessage = errorMessage;
    }

    @Override
    public boolean success() {
      return false;
    }

    @Override
    public String getErrorMessage() {
      return errorMessage;
    }

    @Override
    public int getResponseCode() {
      return status;
    }
  }

  public static class NodesInfo extends ElasticAction {
    @Override
    public Result getResult(WebTarget target) {
      try {
        return new JsonResult(target.path("_nodes").request().buildGet().invoke(byte[].class));
      } catch (WebApplicationException e) {
        return new FailureResult(e.getResponse().getStatus(), e.getMessage());
      }
    }
  }

  public static class Health extends ElasticAction {
    public Result getResult(WebTarget target) {
      try {
        return new JsonResult(target.path("_cluster/health").request().buildGet().invoke(byte[].class));
      } catch (WebApplicationException e) {
        return new FailureResult(e.getResponse().getStatus(), e.getMessage());
      }
    }
  }

  public static class Search extends ElasticAction2<byte[]> {
    private String query;
    private String resource;
    private Map<String,String> parameters = new HashMap<>();


    public Search() {
      super("start search", byte[].class);
    }

    public Search setQuery(String query){
      this.query = query;
      return this;
    }

    public Search setResource(String resource){
      this.resource = resource;
      return this;
    }

    public Search setParameter(String key, String value) {
      parameters.put(key, value);
      return this;
    }

    public String getResource(){
      return resource;
    }

    @Override
    Invocation buildRequest(WebTarget initial, ContextListener context) {
      WebTarget t = initial
          .path(resource)
          .path("_search");
      for (Entry<String,String> entry : parameters.entrySet()) {
        t = t.queryParam(entry.getKey(), entry.getValue());
      }

      context.addContext(t);
      context.addContext("Query", query);
      return t.request().build("POST", Entity.json(query));
    }

  }

  public static class SearchScroll extends ElasticAction2<byte[]> {

    private String scrollId;
    private String scrollTimeout;

    public SearchScroll() {
      super("get next search result", byte[].class);
    }

    public SearchScroll setScrollId(String scrollId){
      this.scrollId = scrollId;
      return this;
    }

    @Override
    Invocation buildRequest(WebTarget initial, ContextListener context) {
      WebTarget target = initial.path("_search/scroll").queryParam("scroll", scrollTimeout);
      context.addContext("ScrollId", scrollId);
      context.addContext(target);
      return target.request().buildPost(Entity.text(scrollId));
    }

    public SearchScroll setScrollTimeout(String scrollTimeout){
      this.scrollTimeout = scrollTimeout;
      return this;
    }

  }

  public static class DeleteScroll {
    private String scrollId;

    public DeleteScroll(String scrollId) {
      this.scrollId = scrollId;
    }

    public void delete(WebTarget target, InvocationCallback<Response> callback) {
      target.path("_search/scroll").path(scrollId).request().buildDelete().submit(callback);
    }
  }

  public static class SearchShards extends ElasticAction {
    private List<String> indexes = new ArrayList<>();

    public SearchShards addIndex(String index) {
      indexes.add(index);
      return this;
    }

    public SearchShards addIndices(List<String> indices){
      indexes.addAll(indices);
      return this;
    }

    public Result getResult(WebTarget target) {
      try {
        return new JsonResult(target.path(Joiner.on(",").join(indexes)).path("_search_shards").request().buildGet().invoke(byte[].class));
      } catch (WebApplicationException e) {
        return new FailureResult(e.getResponse().getStatus(), e.getMessage());
      }
    }
  }

  public static class CountResult extends Result {
    private long count;

    public CountResult(long count) {
      this.count = count;
    }

    @Override
    public long getAsLong() {
      return count;
    }
  }

  public static class Count extends ElasticAction {
    private List<String> indexes = new ArrayList<>();
    private List<String> types = new ArrayList<>();

    public Count addIndex(String index) {
      indexes.add(index);
      return this;
    }

    public Count addType(String type) {
      types.add(type);
      return this;
    }

    public Result getResult(WebTarget target) {
      try {
        return new CountResult(parse(target.path(Joiner.on(",").join(indexes)).path(Joiner.on(",").join(types)).path("_count").request().buildGet().invoke(String.class)).get("count").getAsLong());
      } catch (WebApplicationException e) {
        return new FailureResult(e.getResponse().getStatus(), e.getMessage());
      }
    }
  }

  public static class CatAlias extends ElasticAction {
    private final String alias;
    public CatAlias(String alias) {
      this.alias = alias;
    }
    @Override
    public Result getResult(WebTarget target) {
      try {
        return new JsonResult(target.path("_alias").path(alias).request().header("Accept", "application/json").header("content-type", "application/json").buildGet().invoke(byte[].class));
      } catch (WebApplicationException e) {
        return new FailureResult(e.getResponse().getStatus(), e.getMessage());
      }
    }
  }

  public static class IndexExists extends ElasticAction {
    private String index;

    public IndexExists addIndex(String index) {
      this.index = index;
      return this;
    }

    @Override
    public Result getResult(WebTarget target) {
      final Response obj = target.path(index).request().build("HEAD").invoke();
      return new Result() {
        @Override
        public int getAsInt() {
          return obj.getStatus();
        }
        @Override
        public boolean success() {
          return isHttpSuccessful(obj.getStatus());
        }
      };
    }
  }

  private static boolean isHttpSuccessful(int httpCode) {
    return (httpCode / 100) == 2;
  }


  public abstract static class ElasticAction2<T> {

    private final String action;
    private final Class<T> responseClazz;

    public ElasticAction2(String action, Class<T> responseClazz) {
      super();
      this.action = action;
      this.responseClazz = responseClazz;
    }

    public Class<T> getResponseClass(){
      return responseClazz;
    }

    String getAction(){
      return action;
    }

    abstract Invocation buildRequest(WebTarget initial, ContextListener context);
  }

  public static class GetClusterMetadata extends ElasticAction2<ClusterMetadata> {
    private String indexName;

    public GetClusterMetadata(){
      super("retrieving cluster metadata", ClusterMetadata.class);
    }

    public GetClusterMetadata setIndex(String indexName){
      this.indexName = indexName;
      return this;
    }

    @Override
    Invocation buildRequest(WebTarget target, ContextListener context) {
      target = target.path("_cluster/state/metadata");
      if(indexName != null){
        target = target.path(indexName);
      }

      context.addContext(target);
      return target.request().buildGet();
    }

  }

  public interface ContextListener {
    ContextListener addContext(WebTarget target);
    ContextListener addContext(String context, String message, Object...args);
  }


}
