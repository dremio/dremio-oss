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
import { Component, PropTypes } from 'react';
import SelectSourceTypeView from './SelectSourceTypeView';

export default class SelectSourceType extends Component {
  static propTypes = {
    routeParams: PropTypes.object,
    children: PropTypes.node,
    onSelectSource: PropTypes.func
  };

  constructor(props) {
    super(props);

    this.popularConnections = [
      {label: 'Amazon Redshift', sourceType: 'Redshift', beta: true },
      {label: 'Amazon S3', sourceType: 'S3', beta: true },
      {label: 'Elasticsearch', sourceType: 'Elastic' },
      {label: 'HBase', sourceType: 'HBase', beta: true },
      {label: 'HDFS', sourceType: 'HDFS' },
      {label: 'Hive', sourceType: 'Hive' },
      // {label: 'IBM DB2', sourceType: 'DB2' },
      {label: 'MapR-FS', sourceType: 'MapRFS' },
      {label: 'Microsoft SQL Server', sourceType: 'SQLserver' },
      {label: 'MongoDB', sourceType: 'MongoDB', beta: true },
      {label: 'MySQL', sourceType: 'MySQL', beta: true },
      {label: 'NAS', sourceType: 'NAS' },
      {label: 'Oracle', sourceType: 'Oracle' },
      {label: 'PostgreSQL', sourceType: 'PostgreSQL', beta: true }
      // {label: 'LinuxCluster', sourceType: 'LinuxCluster' },
      // {label: 'GoogleAnalytics', sourceType: 'GooogleAnalytics' },
    ];
    this.disabledConnections = [
      {label: 'Cassandra', sourceType: 'Cassandra' },
      {label: 'Salesforce', sourceType: 'Salesforce' },
      {label: 'Solr', sourceType: 'Solr' },
      {label: 'Netezza', sourceType: 'Netezza' },
      {label: 'Teradata', sourceType: 'Teradata' }
      // {label: 'Redis', sourceType: 'Redis' },
      // {label: 'Kudu', sourceType: 'Kudu' },
      // {label: 'MapRDB', sourceType: 'MapRDB' },
      // {label: 'Phoenix', sourceType: 'Phoenix' },
      // {label: 'RecordService', sourceType: 'RecordService' },
      // {label: 'Impala', sourceType: 'Impala' },
      // {label: 'Kafka', sourceType: 'Kafka' },
      // {label: 'AzureDataLake', sourceType: 'AzureDataLake' },
      // {label: 'AzureDocumentDB', sourceType: 'AzureDocumentDB' },
      // {label: 'AmazonDynamoDB', sourceType: 'AmazonDynamoDB' },
      // {label: 'GenericREST', sourceType: 'GenericREST' }
    ];
  }


  render() {
    return (
      <SelectSourceTypeView
        popularConnections={this.popularConnections}
        disabledConnections={this.disabledConnections}
        showAutocomplete
        onSelectSource={this.props.onSelectSource}/>
    );
  }
}
