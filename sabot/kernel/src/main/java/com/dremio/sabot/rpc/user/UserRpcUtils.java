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
package com.dremio.sabot.rpc.user;

import java.lang.management.ManagementFactory;

import com.dremio.common.Version;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.exec.proto.UserBitShared.RpcEndpointInfos;
import com.google.common.base.Preconditions;

/**
 * Utility class for User RPC
 *
 */
public final class UserRpcUtils {
  private UserRpcUtils() {}

  /*
   * Template for the endpoint infos.
   *
   * It speeds up things not to check application name via JMX for
   * each connection.
   */
  private static final RpcEndpointInfos INFOS_TEMPLATE =
      RpcEndpointInfos.newBuilder()
        .setApplication(ManagementFactory.getRuntimeMXBean().getName())
        .setVersion(DremioVersionInfo.getVersion())
        .setMajorVersion(DremioVersionInfo.getMajorVersion())
        .setMinorVersion(DremioVersionInfo.getMinorVersion())
        .setPatchVersion(DremioVersionInfo.getPatchVersion())
        .setBuildNumber(DremioVersionInfo.getBuildNumber())
        .setVersionQualifier(DremioVersionInfo.getQualifier())
        .buildPartial();

  /**
   * Returns a {@code RpcEndpointInfos} instance
   *
   * The instance is populated based on Dremio version informations
   * from the classpath and runtime information for the application
   * name.
   *
   * @param name the endpoint name.
   * @return a {@code RpcEndpointInfos} instance
   * @throws NullPointerException if name is null
   */
  public static RpcEndpointInfos getRpcEndpointInfos(String name) {
    RpcEndpointInfos infos = RpcEndpointInfos.newBuilder(INFOS_TEMPLATE)
        .setName(Preconditions.checkNotNull(name))
        .build();

    return infos;
  }

  /**
   * Get the version from a {@code RpcEndpointInfos} instance
   */
  public static Version getVersion(RpcEndpointInfos infos) {
    return new Version(
        infos.getVersion(),
        infos.getMajorVersion(),
        infos.getMinorVersion(),
        infos.getPatchVersion(),
        infos.getBuildNumber(),
        infos.getVersionQualifier());
  }
}
