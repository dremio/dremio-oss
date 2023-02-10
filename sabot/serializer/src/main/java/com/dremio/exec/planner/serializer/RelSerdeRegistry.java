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
package com.dremio.exec.planner.serializer;

import java.util.Map;

import org.apache.calcite.rel.RelNode;

import com.dremio.common.scanner.persistence.ScanResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Holds a list of all the RelNodeSerde's available within a ScanResult.
 */
class RelSerdeRegistry {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RelSerdeRegistry.class);

  private Map<Class<?>, RelNodeSerde<? extends RelNode,?>> serdesFromLogical;
  private Map<String, RelNodeSerde<?, ?>> protoHolders;

  @SuppressWarnings({ "rawtypes" })
  public RelSerdeRegistry(ScanResult result) {
    ImmutableMap.Builder<Class<?>, RelNodeSerde<?, ?>> serdesFromLogical = ImmutableMap.builder();
    ImmutableMap.Builder<String, RelNodeSerde<?, ?>> protoHolders = ImmutableMap.builder();

    for(Class<? extends RelNodeSerde> s : result.getImplementations(RelNodeSerde.class)) {
      try {
       RelNodeSerde<?, ?> serde = s.newInstance();
       serdesFromLogical.put(serde.getRelClass(), serde);
       protoHolders.put("type.googleapis.com/" + serde.getDefaultInstance().getDescriptorForType().getFullName(), serde);
      } catch (InstantiationException | IllegalAccessException e) {
        logger.warn("Unable to instantiate {}", s.getName(), e);
      }
    }

    this.serdesFromLogical = serdesFromLogical.build();
    this.protoHolders = protoHolders.build();
  }

  public Iterable<String> getProtoNames(){
    return protoHolders.keySet();
  }

  @SuppressWarnings("unchecked")
  public <T extends RelNode> RelNodeSerde<T, ?> getSerdeByRelNodeClass(Class<T> clazz){
    return (RelNodeSerde<T, ?>) Preconditions.checkNotNull(serdesFromLogical.get(clazz), "Unable to find RelNodeSerde for %s.", clazz.getName());
  }

  public RelNodeSerde<?, ?> getSerdeByTypeString(String type) {
    return Preconditions.checkNotNull(protoHolders.get(type), "Unable to find RelNodeSerde for %s", type);

  }
}
