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
package com.dremio.jdbc;

import java.util.Properties;
import org.apache.calcite.avatica.ConnectionConfigImpl;

// TODO(DRILL-3730):  Change public DremioConnectionConfig from class to
// interface.  Move implementation (including inheritance from
// net.hydromatic.avatica.ConnectionConfigImpl) from published-interface package
// com.dremio.jdbc to class in implementation package
// com.dremio.jdbc.impl.
/**
 * NOTE: DremioConnectionConfig will be changed from a class to an interface.
 *
 * <p>In the meantime, clients must not use the fact that {@code DremioConnectionConfig} currently
 * extends {@link net.hydromatic.avatica.ConnectionConfigImpl}. They must call only methods declared
 * directly in DremioConnectionConfig (or inherited Object).
 */
public class DremioConnectionConfig extends ConnectionConfigImpl {
  private final Properties props;

  public DremioConnectionConfig(Properties p) {
    super(p);
    this.props = p;
  }

  public boolean isLocal() {
    // TODO  Why doesn't this call getZookeeperConnectionString()?
    return "local".equals(props.getProperty("zk"));
  }

  // True if the URL points directly to a node
  public boolean isDirect() {
    return props.getProperty("direct") != null;
  }

  // TODO: Check: Shouldn't something validate that URL has "zk" parameter?
  public String getZookeeperConnectionString() {
    return props.getProperty("zk");
  }

  public boolean isServerPreparedStatementDisabled() {
    return Boolean.valueOf(props.getProperty("server.preparedstatement.disabled"));
  }

  public boolean isServerMetadataDisabled() {
    return Boolean.valueOf(props.getProperty("server.metadata.disabled"));
  }
}
