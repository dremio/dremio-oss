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
import Immutable from 'immutable';
import { get } from 'lodash/object';

import StatefulTableViewer from '@app/components/StatefulTableViewer';
import { getViewState } from '@app/selectors/resources';

import { page, pageContent } from 'uiTheme/radium/general';
import ClusterListViewMixin from 'dyn-load/pages/AdminPage/subpages/Provisioning/ClusterListViewMixin';
import EnginesFilter from '@app/pages/AdminPage/subpages/Provisioning/components/EnginesFilter';
import { getFilteredEngines } from 'dyn-load/pages/AdminPage/subpages/Provisioning/EngineFilterHelper';
import EngineStatus from '@app/pages/AdminPage/subpages/Provisioning/components/EngineStatus';
import NumberFormatUtils from '@app/utils/numberFormatUtils';
import {
  getEntityName,
  getYarnSubProperty,
  isYarn
} from '@app/pages/AdminPage/subpages/Provisioning/provisioningUtils';
import { CLUSTER_STATE } from '@app/constants/provisioningPage/provisioningConstants';
import { DEFAULT_ENGINE_FILTER_SELECTIONS } from 'dyn-load/constants/provisioningPage/provisioningConstants';

export const VIEW_ID = 'ClusterListView';
export const STATUS_VIEW_ID = 'ClusterListViewStatus';

export const YARN_HOST_PROPERTY = 'yarn.resourcemanager.hostname';
export const YARN_NODE_TAG_PROPERTY = 'services.node-tag';

@ClusterListViewMixin
export class ClusterListView extends Component {
  static propTypes = {
    provisions: PropTypes.instanceOf(Immutable.List),
    queues: PropTypes.instanceOf(Immutable.List),
    removeProvision: PropTypes.func,
    editProvision: PropTypes.func,
    changeProvisionState: PropTypes.func,
    adjustWorkers: PropTypes.func,
    selectEngine: PropTypes.func,
    //connected
    viewState: PropTypes.instanceOf(Immutable.Map),
    statusViewState: PropTypes.instanceOf(Immutable.Map)
  };

  static defaultProps = {
    provisions: Immutable.List()
  };

  state = {
    filterState: {filters: DEFAULT_ENGINE_FILTER_SELECTIONS}
  };

  onUpdateFilterState = (filterState) => {
    this.setState({filterState});
  };

  getEngineData(engine) {
    return {
      data: {
        status: {node: () => <EngineStatus engine={engine} viewState={this.props.statusViewState}/>},
        engine: {node: () => this.getEngineName(engine)},
        size: {node: () => this.getEngineSize(engine)},
        cores: {node: () => this.getClusterCPUCores(engine)},
        memory: {node: () => this.getClusterRAM(engine)},
        ip: {node: () => this.getClusterIp(engine)},
        nodes: {node: () => this.getRunningNodes(engine)},
        action: {node: () => this.getAction(engine)}
      }
    };
  }

  getEngineName(entity) {
    const name = getEntityName(entity, YARN_NODE_TAG_PROPERTY);
    const engineId = entity.get('id');
    return <div className='link' onClick={this.props.selectEngine.bind(this, engineId)}>{name}</div>;
  }

  getEngineSize(entity) {
    return entity.getIn(['dynamicConfig', 'containerCount']);
  }

  getClusterCPUCores = (entity) => {
    return isYarn(entity) && entity.getIn(['yarnProps', 'virtualCoreCount']) || '';
  };

  getClusterRAM = (entity) => {
    const valueInMb = entity.getIn(['yarnProps', 'memoryMB']);
    const displayedValue = (valueInMb === undefined || Number.isNaN(valueInMb)) ? '-'
      : NumberFormatUtils.makeMemoryValueString(valueInMb * 1024 * 1024);
    return displayedValue;
  };

  getClusterIp = (entity) => {
    return isYarn(entity) && getYarnSubProperty(entity, YARN_HOST_PROPERTY) || '';
  };

  getRunningNodes(entity) {
    const {active, total} = entity.get('workersSummary').toJS();
    return `${active} / ${total}`;
  }

  handleStartStop = (entity) => {
    const nextState = entity.get('currentState') === CLUSTER_STATE.running ?
      CLUSTER_STATE.stopped : CLUSTER_STATE.running;
    this.props.changeProvisionState(nextState, entity, STATUS_VIEW_ID);
  };
  handleAddRemove = (entity) => {
    //show AdjustWorkersForm in a popup
    this.props.adjustWorkers(entity);
  };

  getEngines = () => {
    const engines = this.props.provisions;
    const filters = get(this.state, 'filterState.filters');
    return getFilteredEngines(engines, filters);
  };

  getTableData = () => {
    const engines = this.getEngines();
    return engines.map(engine => this.getEngineData(engine));
  };

  render() {
    // provisions are sorted in selectors/provision
    const {viewState} = this.props;
    const columns = this.getTableColumns();
    const tableData = this.getTableData();

    return (
      <div id='admin-engines' style={page}>
        <>
          <EnginesFilter
            filterState={this.state.filterState}
            onUpdateFilterState={this.onUpdateFilterState}
            style={{flexShrink: 0}}
          />
        </>
        <div style={pageContent}>
          <StatefulTableViewer
            virtualized
            viewState={viewState}
            tableData={tableData}
            columns={columns}
          />
        </div>
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    viewState: getViewState(state, VIEW_ID),
    statusViewState: getViewState(state, STATUS_VIEW_ID)
  };
}

export default connect(mapStateToProps)(ClusterListView);
