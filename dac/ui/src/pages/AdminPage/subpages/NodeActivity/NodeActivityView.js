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
import { connect } from 'react-redux';
import Immutable  from 'immutable';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import StatefulTableViewer from 'components/StatefulTableViewer';
import NumberFormatUtils from 'utils/numberFormatUtils';
import { getViewState } from 'selectors/resources';
import Header from 'pages/AdminPage/components/Header';
import NodeTableCell from 'pages/AdminPage/subpages/NodeActivity/NodeTableCell';
import NodeActivityViewMixin from 'dyn-load/pages/AdminPage/subpages/NodeActivity/NodeActivityViewMixin';

import './NodeActivity.less'; // TODO to Vasyl, need to use Radium
import { page, pageContent } from 'uiTheme/radium/general';

export const VIEW_ID = 'NodeActivityView';

@Radium
@pureRender
@NodeActivityViewMixin
class NodeActivityView extends Component {

  static propTypes = {
    sourceNodesList: PropTypes.instanceOf(Immutable.Map),
    viewState: PropTypes.instanceOf(Immutable.Map)
  }

  getTableColumnsConfig() {
    return [{
      label: la('Node'),
      flexGrow: 1
    },
    {
      label: la('Host'),
      flexGrow: 1
    },
    {
      label: la('Port'),
      width: 100
    },
    {
      label: la('CPU'),
      width: 100
    },
    {
      label: la('Memory'),
      width: 140
    }];
  }

  getTableColumns() {
    return this.getTableColumnsConfig().map(column => ({ key: column.label, ...column}));
  }

  getNodeCellStatus(node) {
    return node.get('status');
  }

  getNodeCellTooltip(node) {
    return (this.getNodeCellStatus(node) === 'green') ? la('Active') : '';
  }

  getNodeCell(node) {
    return (
      <NodeTableCell
        name={node.get('name')}
        status={this.getNodeCellStatus(node)}
        tooltip={this.getNodeCellTooltip(node)}
      />
    );
  }

  getNodeData(columnNames, node) {
    const [name, ip, port, cpu, memory] = columnNames;
    return {
      data: {
        [name]: this.getNodeCell(node),
        [ip]: node.get('ip'),
        [port]: node.get('port'),
        [cpu]: `${NumberFormatUtils.roundNumberField(node.get('cpu'))}%`,
        [memory]: `${NumberFormatUtils.roundNumberField(node.get('memory'))}%` // todo: check comps for digits. and fix so no need for parseFloat
      }
    };
  }

  getNodes() {
    return this.props.sourceNodesList.get('nodes');
  }

  getTableData() { // todo: styling: col alignment and spacing (esp. numbers)
    const columnNames = this.getTableColumnsConfig().map(column => column.label);
    const nodes = this.getNodes();
    return nodes.map( node => this.getNodeData(columnNames, node, nodes));
  }

  render() {
    const tableData = this.getTableData();
    const columns = this.getTableColumns();

    return (
      <div id='admin-nodeActivity' style={page}>
        <Header title={la('Node Activity')}/>
        {this.getSubHeader()}
        <div style={pageContent}>
          <StatefulTableViewer
            tableData={tableData}
            columns={columns}
            viewState={this.props.viewState}
          />
        </div>
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    viewState: getViewState(state, VIEW_ID)
  };
}

export default connect(mapStateToProps)(NodeActivityView);
