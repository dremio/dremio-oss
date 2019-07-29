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
package com.dremio.service.namespace.capabilities;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Holds a collection of source capabilities. This is serializable and exposes a
 * set of known primitives. All classes used here are final to ensure that no
 * references are connected back to underlying sources from these objects. This
 * exists to expose capabilities of the source that are known based on either
 * configuration, nature or as reported by connecting to the source.
 */
public final class SourceCapabilities {

  public static final SourceCapabilities NONE = new SourceCapabilities(ImmutableList.<CapabilityValue<?,?>>of());

  public static final BooleanCapability REQUIRES_HARD_AFFINITY = new BooleanCapability("requires_hard_affinity", false);
  public static final BooleanCapability SUPPORTS_CONTAINS = new BooleanCapability("supports_contains_operation", false);

  // Indicates that the plugin disallows ScanCrel nodes in plans produced for it. By making the cost infinite,
  // the planner is forced to substitute a ScanCrel for a plugin-specific node.
  public static final BooleanCapability TREAT_CALCITE_SCAN_COST_AS_INFINITE = new BooleanCapability("treat_calcite_scan_cost_as_infinite", false);

  // Indicates that the plugin is capable of pushing down sub queries.
  public static final BooleanCapability SUBQUERY_PUSHDOWNABLE =
    new BooleanCapability("subquery_pushdownable", false);

  // Indicates that the plugin is capable of pushing down correlated sub queries.
  public static final BooleanCapability CORRELATED_SUBQUERY_PUSHDOWN =
    new BooleanCapability("correlated_pushdownable", true);


  private final ImmutableMap<Capability<?>, CapabilityValue<?,?>> values;

  @JsonCreator
  public SourceCapabilities(
      @JsonProperty("capabilitiesList") List<CapabilityValue<?,?>> capabilities){
    if(capabilities == null) {
      capabilities = ImmutableList.of();
    }
    ImmutableMap.Builder<Capability<?>, CapabilityValue<?,?>> builder = ImmutableMap.builder();
    for(CapabilityValue<?, ?> c : capabilities){
      builder.put(c.getCapability(), c);
    }
    this.values = builder.build();
  }

  public SourceCapabilities(CapabilityValue<?,?>... capabilities){
    ImmutableMap.Builder<Capability<?>, CapabilityValue<?,?>> builder = ImmutableMap.builder();
    for(CapabilityValue<?, ?> c : capabilities){
      builder.put(c.getCapability(), c);
    }
    this.values = builder.build();
  }

  public long getCapability(LongCapability capability){
    CapabilityValue<?, ?> value = values.get(capability);
    if(value != null && value.getCapability().equals(capability)){
      return (Long) value.getValue();
    }else{
      return capability.getDefaultValue();
    }
  }

  public boolean getCapability(BooleanCapability capability){
    CapabilityValue<?,?> value = values.get(capability);
    if(value != null && value.getCapability().equals(capability)){
      return (Boolean) value.getValue();
    }else{
      return capability.getDefaultValue();
    }
  }

  public double getCapability(DoubleCapability capability){
    CapabilityValue<?,?> value = values.get(capability);
    if(value != null && value.getCapability().equals(capability)){
      return (Double) value.getValue();
    }else{
      return capability.getDefaultValue();
    }
  }

  public String getCapability(StringCapability capability){
    CapabilityValue<?,?> value = values.get(capability);
    if(value != null && value.getCapability().equals(capability)){
      return (String) value.getValue();
    }else{
      return capability.getDefaultValue();
    }
  }

  // for serialization.
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<CapabilityValue<?,?>> getCapabilitiesList(){
    return ImmutableList.copyOf(values.values());
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof SourceCapabilities)) {
      return false;
    }
    SourceCapabilities castOther = (SourceCapabilities) other;
    return Objects.equal(values, castOther.values);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(values);
  }

}
