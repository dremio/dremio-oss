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
package com.dremio.dac.sources;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

import com.dremio.dac.proto.model.source.Host;

/**
 * Utilities related to the generated Host type
 */
public class Hosts {

  static StringBuilder appendHosts(StringBuilder sb, Iterable<Host> hostList, char delimiter) {
    Iterator<Host> iterator = hostList.iterator();
    while (iterator.hasNext()) {
      Host h = iterator.next();
      sb.append(checkNotNull(h.getHostname(), "hostname missing")).append(":").append(checkNotNull(h.getPort(), "port missing"));
      if(iterator.hasNext()) {
        sb.append(delimiter);
      }
    }
    return sb;
  }

  static String getHosts(Iterable<Host> hostList, char delimiter) {
    return appendHosts(new StringBuilder(), hostList, delimiter).toString();
  }
}
