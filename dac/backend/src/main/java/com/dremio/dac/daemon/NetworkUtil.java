/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.daemon;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;

/**
 * Utility class for helping determining local resolution.
 */
public final class NetworkUtil {

  private NetworkUtil(){}

  public static boolean addressResolvesToThisNode(String masterNode) throws UnknownHostException {
    return addressResolvesToThisNode(InetAddress.getByName(masterNode));
  }

  public static boolean addressResolvesToThisNode(InetAddress masterNode) {
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      if (interfaces == null) {
        throw new RuntimeException(
            "Failure to retrieve any network interfaces from the node.  Check that {} is a valid address and the system can correctly resolve it.");
      }

      for (NetworkInterface networkInterface : Collections.list(interfaces)) {
        Enumeration<InetAddress> inetAddressEnumeration = networkInterface.getInetAddresses();
        if (inetAddressEnumeration == null) {
          continue;
        }
        for (InetAddress address : Collections.list(inetAddressEnumeration)) {
          if (address.equals(masterNode)) {
            return true;
          }
        }
      }
    } catch (SocketException e) {
      throw new RuntimeException(
          "Failure retrieving all network interfaces from the node.  Check that " + masterNode + " is a valid address and the system can correctly resolve it.", e);
    }
    return false;
  }
}
