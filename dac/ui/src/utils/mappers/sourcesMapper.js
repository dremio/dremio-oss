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
class SourcesMapper {
  newSource(type, data) {
    const info = data;
    delete info.credentials;
    const hash = {
      'HDFS': 'HDFS',
      'MongoDB': 'MONGO',
      'S3': 'S3',
      'HBase': 'HBASE',
      'LinuxCluster': 'LinuxCluster',
      'MapRFS': 'MAPRFS',
      'MySQL': 'MYSQL',
      'NAS': 'NAS',
      'GoogleAnalytics': 'GoogleAnalytics',
      'Oracle': 'ORACLE',
      'Saleforce': 'Saleforce',
      'Elastic': 'ELASTIC',
      'Kudu': 'KUDU',
      'Redshift': 'REDSHIFT',
      'PostgreSQL': 'POSTGRES',
      'Hive' : 'HIVE',
      'SQLserver' : 'MSSQL',
      'DB2' : 'DB2'
    };
    if (info.config.hostList) {
      for (const host of info.config.hostList) {
        delete host.id;
      }
    }
    if (info.config.propertyList) {
      for (const property of info.config.propertyList) {
        delete property.id;
      }
    }
    if (info.config.authenticationTimeoutMillis) {
      info.config.authenticationTimeoutMillis = Number(info.config.authenticationTimeoutMillis);
    }
    if (info.config.subpartitionSize) {
      info.config.subpartitionSize = Number(info.config.subpartitionSize);
    }
    return {...info, type: hash[type] || type};
  }
}

const sourcesMapper = new SourcesMapper();

export default sourcesMapper;
