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
import Immutable from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';
import uuid from 'uuid';

import { GREEN } from 'uiTheme/radium/colors';

const GRID_BORDER = '1px solid #e5e5e5';
export const WORKER_STATE_TO_COLOR = {
  active: GREEN,
  pending: '#bbc0c7',
  disconnected: '#fd9b3c',
  decommissioning: '#eb5050'
};

@Radium
export default class WorkersGrid extends Component {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    workersStatus: PropTypes.object
  }

  getGridItemSize(items) {
    const total = items.reduce((acc, i) => acc + i.value, 0);
    if (total > 175) {
      return 5;
    }
    if (total > 40) {
      return 9;
    }
    return 15;
  }

  getWorkerStateData() {
    const {active, pending, disconnected, decommissioning} = this.props.entity.get('workersSummary').toJS();
    return [
      { workerState: 'active', label: la('Active'), value: active },
      { workerState: 'pending', label: la('Pending'), value: pending },
      { workerState: 'disconnected', label: la('Provisioning or Disconnected'), value: disconnected },
      { workerState: 'decommissioning', label: la('Decommisioning'), value: decommissioning }
    ];
  }

  renderStatusIcon({ workerState, size = 8, styles = {} }) {
    const style = {
      ...styles,
      width: size,
      height: size,
      background: WORKER_STATE_TO_COLOR[workerState]
    };
    return <div style={style} key={uuid.v4()} />;
  }

  renderStatusItem(item, index) {
    const { workerState } = item;
    const isEven = index % 2 === 0;
    const style = {
      ...styles.statusItem,
      borderLeft: isEven ? 'transparent' : GRID_BORDER
    };
    return (
      <div key={index} style={style}>
        {this.renderStatusIcon({ workerState, styles: { marginTop: 2 }})}
        <div style={styles.statusCount}>{item.value}</div>
        <div style={styles.statusLabel}>{item.label}</div>
      </div>
    );
  }

  renderContainerStatuses() {
    return (
      <div style={styles.statusWrapper}>
        {this.getWorkerStateData().map((i, index) => this.renderStatusItem(i, index))}
      </div>
    );
  }

  renderWorkersGrid() {
    const items = this.getWorkerStateData();
    const size = this.getGridItemSize(items);
    const margin = size < 15 ? 1 : 2;
    return (
      <div className='grid-list' style={styles.workerGridContainer}>
        {items.reduce((grid, item) => {
          const gridItems = [...Array(item.value)].map(() => {
            return this.renderStatusIcon({ workerState: item.workerState, styles: {margin}});
          });
          return grid.concat(gridItems);
        }, [])}
      </div>
    );
  }

  render() {
    return (
      <div style={styles.base}>
        {this.renderContainerStatuses()}
        {this.renderWorkersGrid()}
      </div>
    );
  }
}

const styles = {
  base: {
    width: 402,
    marginBottom: 12,
    background: '#ffffff',
    border: GRID_BORDER,
    display: 'flex',
    flexDirection: 'column'
  },
  statusWrapper: {
    display: 'flex',
    flexWrap: 'wrap'
  },
  statusItem: {
    width: 200,
    padding: 8,
    borderBottom: GRID_BORDER,
    display: 'flex'
  },
  statusLabel: {
    marginLeft: 5
  },
  statusCount: {
    fontWeight: 500,
    marginLeft: 5
  },
  workerGridContainer: {
    padding: 8,
    display: 'flex',
    flexWrap: 'wrap'
  }
};
