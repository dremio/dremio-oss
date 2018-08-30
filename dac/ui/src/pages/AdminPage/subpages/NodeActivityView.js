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
import { connect } from 'react-redux';
import Immutable  from 'immutable';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import StatefulTableViewer from 'components/StatefulTableViewer';
import NumberFormatUtils from 'utils/numberFormatUtils';
import { getViewState } from 'selectors/resources';
import './NodeActivity.less'; // TODO to Vasyl, need to use Radium
import { page, pageContent } from 'uiTheme/radium/general';
import Header from '../components/Header';

export const VIEW_ID = 'NodeActivityView';

@Radium
@pureRender
class NodeActivityView extends Component {

  static propTypes = {
    sourceNodesList: PropTypes.instanceOf(Immutable.Map),
    viewState: PropTypes.instanceOf(Immutable.Map)
  }

  static colors = {
    green: '#84D754',
    red: '#D83236'
  }

  getNode(name, status) {
    const backgroundStyle = {background: status === 'green'
      ? NodeActivityView.colors.green
      : NodeActivityView.colors.red};

    const style = {...styles.circle, ...backgroundStyle};
    return (
      <span style={styles.node}>
        <span style={style}>{status}</span>
        <span>{name}</span>
      </span>
    );
  }

  getTableColumns() {
    return [{
      label: la('Node'),
      flexGrow: 1
    },
    {
      label: la('IP Address'),
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
    }].map(column => ({ key: column.label, ...column }));
  }

  getTableData() { // todo: styling: col alignment and spacing (esp. numbers)
    const [name, ip, port, cpu, memory] = this.getTableColumns().map(column => column.label);
    return this.props.sourceNodesList.get('nodes').map( (node) => {
      return {
        data: {
          [name]: this.getNode(node.get('name'), node.get('status')),
          [ip]: node.get('ip'),
          [port]: node.get('port'),
          [cpu]: `${NumberFormatUtils.roundNumberField(node.get('cpu'))}%`,
          [memory]: `${NumberFormatUtils.roundNumberField(node.get('memory'))}%` // todo: check comps for digits. and fix so no need for parseFloat
        }
      };
    });
  }

  render() {
    const tableData = this.getTableData();
    const columns = this.getTableColumns();

    return (
      <div id='admin-nodeActivity' style={page}>
        <Header title={la('Node Activity')}/>
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

const styles = {
  node: {
    display: 'flex',
    alignItems: 'center'
  },
  circle: {
    width: 12,
    height: 12,
    margin: '0 10px',
    borderRadius: '50%',
    textIndent: '-9999px'
  }
};
