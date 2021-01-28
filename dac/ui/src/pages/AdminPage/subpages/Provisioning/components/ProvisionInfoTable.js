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
import { Component } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import ViewCheckContent from 'components/ViewCheckContent';
import TableViewer from 'components/TableViewer';
import {getTableData} from '@inject/pages/AdminPage/subpages/Provisioning/components/ProvisionInfoTableMixin';

export default class ProvisionInfoTable extends Component {
  static propTypes = {
    provision: PropTypes.instanceOf(Immutable.Map)
  };

  getTableColumns() {
    const {provision} = this.props;
    const clusterType = provision && provision.get('clusterType');

    if (clusterType === 'YARN') {
      // DX-11577 NOTE regarding widths. Previously reactable component was used to display this data. Widths below are taken from actual dom that is rendered by reactable component.
      return [
        { key: 'status', label: la('Status'), flexGrow: 2 },
        { key: 'host', label: la('Host'), flexGrow: 4 }, // fills a rest of the available space
        { key: 'memoryMB', label: la('Memory (MB)'), align: 'right', width: 131 },
        { key: 'virtualCoreCount', label: la('Virtual Cores'), align: 'right', width: 126 }
      ];
    }
    if (clusterType === 'EC2') {
      return [
        { key: 'status', label: la('Status'), flexGrow: 2 },
        { key: 'host', label: la('Host'), flexGrow: 4 },
        { key: 'instanceId', label: la('Instance ID'), width: 150 },
        { key: 'privateIp', label: la('Private ip'), width: 100 }
      ];
    }
    return [];
  }

  render() {
    const columns = this.getTableColumns();
    const tableData = getTableData(columns);

    return (
      <div style={styles.base}>
        {
          tableData.size > 0 ? <TableViewer
            tableData={tableData}
            rowHeight={42}
            columns={columns}
          /> : (
            <ViewCheckContent
              message={la('No Workers')}
              dataIsNotAvailable={tableData.size === 0}
              customStyle={styles.emptyMessageStyle}
            />
          )
        }
      </div>
    );
  }
}

const styles = {
  base: {
    width: '100%',
    height: '100%',
    position: 'relative',
    overflow: 'auto',
    padding: '0 10px'
  },
  tableHeader: {
    height: 30,
    fontWeight: '500',
    fontSize: 12,
    color: '#333333'
  },
  emptyMessageStyle: {
    paddingBottom: '20%',
    color: '#cbcbcb',
    backgroundColor: '#f8f8f8'
  }
};
