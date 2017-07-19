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
package com.dremio.service.coordinator.zk;

import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.curator.x.discovery.details.InstanceSerializer;

import com.dremio.exec.proto.CoordinationProtos.DremioServiceInstance;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;

final class ServiceInstanceHelper {
  private ServiceInstanceHelper() {}

  public static final InstanceSerializer<NodeEndpoint> SERIALIZER = new DremioServiceInstanceSerializer();

  private static class DremioServiceInstanceSerializer implements InstanceSerializer<NodeEndpoint>{

    @Override
    public byte[] serialize(ServiceInstance<NodeEndpoint> i) throws Exception {
      DremioServiceInstance.Builder b = DremioServiceInstance.newBuilder();
      b.setId(i.getId());
      b.setName(i.getName());
      b.setRegistrationTimeUTC(i.getRegistrationTimeUTC());
      b.setEndpoint(i.getPayload());
      return b.build().toByteArray();
    }

    @Override
    public ServiceInstance<NodeEndpoint> deserialize(byte[] bytes) throws Exception {
      DremioServiceInstance i = DremioServiceInstance.parseFrom(bytes);
      ServiceInstanceBuilder<NodeEndpoint> b = ServiceInstance.<NodeEndpoint>builder();
      b.id(i.getId());
      b.name(i.getName());
      b.registrationTimeUTC(i.getRegistrationTimeUTC());
      b.payload(i.getEndpoint());
      return b.build();
    }

  }
}
