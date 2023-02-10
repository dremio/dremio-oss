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
package com.dremio.resource;

/**
 * Basic ResourceScheduling Properties
 */
public class ResourceSchedulingProperties {

  private String user;
  private String userInfo;
  private Double queryCost;
  private String clientType;
  private String queryType;
  private String routingQueue;
  private String routingTag;
  private String routingEngine;
  private String ruleSetEngine;
  private String queryLabel;

  public ResourceSchedulingProperties() {

  }

  public String getUser() {
    return user;
  }

  public ResourceSchedulingProperties setUser(String user) {
    this.user = user;
    return this;
  }

  public String getUserInfo() {
    return userInfo;
  }

  public ResourceSchedulingProperties setUserInfo(String userInfo) {
    this.userInfo = userInfo;
    return this;
  }

  public Double getQueryCost() {
    return queryCost;
  }

  public ResourceSchedulingProperties setQueryCost(Double queryCost) {
    this.queryCost = queryCost;
    return this;
  }

  public String getClientType() {
    return clientType;
  }

  public ResourceSchedulingProperties setClientType(String clientType) {
    this.clientType = clientType;
    return this;
  }

  public String getQueryType() {
    return queryType;
  }

  public ResourceSchedulingProperties setQueryType(String queryType) {
    this.queryType = queryType;
    return this;
  }

  public String getQueryLabel() {
    return queryLabel;
  }

  public ResourceSchedulingProperties setQueryLabel(String queryLabel) {
    this.queryLabel = queryLabel;
    return this;
  }

  public String getRoutingQueue() {
    return routingQueue;
  }

  public ResourceSchedulingProperties setRoutingQueue(String queueName) {
    this.routingQueue = queueName;
    return this;
  }

  public String getRoutingTag() {
    return routingTag;
  }

  public ResourceSchedulingProperties setRoutingTag(String tag) {
    this.routingTag = tag;
    return this;
  }

  public String getRoutingEngine() {
    return routingEngine;
  }

  public ResourceSchedulingProperties setRoutingEngine(String routingEngine) {
    this.routingEngine = routingEngine;
    return this;
  }

  /*
   * Engine and RoutingEngine are both same. Internally, we still use
   * routingEngine name.
   * RoutingEngine is retained for backward compatibility
   */
  public String getEngine() {
    return routingEngine;
  }

  public ResourceSchedulingProperties setEngine(String engine) {
    this.routingEngine = engine;
    return this;
  }

  public String getRuleSetEngine() {
    return ruleSetEngine;
  }

  public ResourceSchedulingProperties setRuleSetEngine(String ruleSetEngine) {
    this.ruleSetEngine = ruleSetEngine;
    return this;
  }
}
