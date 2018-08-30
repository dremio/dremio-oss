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
import { Component } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import ViewCheckContent from 'components/ViewCheckContent';
import TableViewer from 'components/TableViewer';

export default class ProvisionInfoTable extends Component {
  static propTypes = {
    provision: PropTypes.instanceOf(Immutable.Map)
  };

  getTableData() {
    if (!this.props.provision) return new Immutable.List();

    const getRows = (key, status) => {
      const items = this.props.provision.getIn(['containers', key]);
      if (!items) return new Immutable.List();

      return items.map((item) => {
        const row = {
          rowClassName: '',
          data: item.getIn(['containerPropertyList']).reduce((prev, property) => {
            return {...prev, [property.get('key')]: property.get('value')};
          }, {status})
        };
        return row;
      });
    };

    // Note: 'Running' is not a term we use elsewhere in the UI
    // but in this list we can't distinguish "Active" from "Decomissioning"
    const runningData = getRows('runningList', la('Running'));
    const disconnectedData = getRows('disconnectedList', la('Provisioning or Disconnected'));

    return disconnectedData.concat(runningData);
  }

  getTableColumns() {
    // DX-11577 NOTE regarding widths. Previously reactable component was used to display this data. Widths below are taken from actual dom that is rendered by reactable component.
    return [ // todo: make this list dynamic for different provision types = i.e. just a mapping of known keys to UI strings
      {
        key: 'status',
        label: la('Status'),
        width: 87
      },
      {
        key: 'host',
        label: la('Host'),
        flexGrow: 1 // fills a rest of the available space
      },
      {
        key: 'memoryMB',
        label: la('Memory (MB)'),
        align: 'right',
        width: 131
      },
      {
        key: 'virtualCoreCount',
        label: la('Virtual Cores'),
        align: 'right',
        width: 126
      }
    ];
  }

  render() {
    const columns = this.getTableColumns();
    const tableData = this.getTableData(columns);

    return (
      <div style={styles.base}>
        {tableData.size > 0 ? <TableViewer
          tableData={tableData}
          columns={columns}
        /> :
        <ViewCheckContent
          message={la('No Workers')}
          dataIsNotAvailable={tableData.size === 0}
          customStyle={styles.emptyMessageStyle}
        />}
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
