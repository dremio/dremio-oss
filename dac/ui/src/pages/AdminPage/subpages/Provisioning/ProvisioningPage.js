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
import Immutable from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';

import {
  loadProvision, removeProvision,
  openAddProvisionModal, openEditProvisionModal,
  editProvision, openAdjustWorkersModal
} from '@app/actions/resources/provisioning';
import {extraProvisingPageMapDispatchToProps } from '@inject/actions/resources/provisioning';
import { showConfirmationDialog } from '@app/actions/confirmation';
import { addNotification } from '@app/actions/notification';
import { getViewState } from '@app/selectors/resources';
import { getAllProvisions } from '@app/selectors/provision';
import { PROVISION_MANAGERS } from 'dyn-load/constants/provisioningPage/provisionManagers';
import { MSG_CLEAR_DELAY_SEC } from '@app/constants/Constants';
import Header from '@app/pages/AdminPage/components/Header';
import ViewStateWrapper from '@app/components/ViewStateWrapper';
import Button from '@app/components/Buttons/Button';
import * as ButtonTypes from '@app/components/Buttons/ButtonTypes';
import { page, pageContent } from '@app/uiTheme/radium/general';
import ApiUtils from '@app/utils/apiUtils/apiUtils';
import { SingleEngineView } from '@app/pages/AdminPage/subpages/Provisioning/components/singleEngine/SingleEngineView';
import { SingleEngineHeader } from '@app/pages/AdminPage/subpages/Provisioning/components/singleEngine/SingleEngineHeader';
import ProvisioningPageMixin from 'dyn-load/pages/AdminPage/subpages/Provisioning/ProvisioningPageMixin';
import ClusterListView from '@app/pages/AdminPage/subpages/Provisioning/ClusterListView';

const VIEW_ID = 'ProvisioningPage';
const PROVISION_POLL_INTERVAL = 3000;

@Radium
@ProvisioningPageMixin
export class ProvisioningPage extends Component {
  static propTypes = {
    viewState: PropTypes.instanceOf(Immutable.Map),
    provisions: PropTypes.instanceOf(Immutable.List),
    loadProvision: PropTypes.func,
    removeProvision: PropTypes.func,
    openAddProvisionModal: PropTypes.func,
    openEditProvisionModal: PropTypes.func,
    openAdjustWorkersModal: PropTypes.func,
    showConfirmationDialog: PropTypes.func,
    addNotification: PropTypes.func,
    editProvision: PropTypes.func
  };

  state = {
    selectedEngineId: null
  };

  pollId = 0;

  _isUnmounted = false;

  componentWillMount() {
    this.startPollingProvisionData(true);
    this.loadData();
  }

  componentWillUnmount() {
    this._isUnmounted = true;
    this.stopPollingProvisionData();
  }

  removeProvision = (entity) => {
    ApiUtils.attachFormSubmitHandlers(
      this.props.removeProvision(entity.get('id'), VIEW_ID)
    ).then(() => {
      this.props.loadProvision(null, VIEW_ID);
    }).catch(e => {
      const message = e &&  e._error && e._error.message;
      const errorMessage = message && message.get('errorMessage') || la('Failed to remove provision');
      this.props.addNotification(<span>{errorMessage}</span>, 'error', MSG_CLEAR_DELAY_SEC);
    });
  };

  handleRemoveProvision = (entity) => {
    this.props.showConfirmationDialog({
      title: la('Remove Engine'),
      text: la('Are you sure you want to remove this engine?'),
      confirmText: la('Remove'),
      confirm: () => this.removeProvision(entity)
    });
  };

  handleStopProvision = (confirmCallback) => {
    this.props.showConfirmationDialog({
      title: la('Stop Engine'),
      text: [
        la('Existing jobs will be halted.'),
        la('Are you sure you want to stop the engine?')
      ],
      cancelText: la('Don\'t Stop Engine'),
      confirmText: la('Stop Engine'),
      confirm: confirmCallback
    });
  };

  handleChangeProvisionState = (desiredState, entity, viewId) => {
    const data = {
      ...entity.toJS(),
      desiredState
    };
    delete data.workersSummary; // we add this in a decorator
    // server should be ignoring these readonly fields on write
    delete data.containers;
    delete data.currentState;
    delete data.error;
    delete data.detailedError;
    delete data.stateChangeTime;

    const commitChange = () => {
      const actionName = data.desiredState === 'STOPPED' ? 'stop' : 'start';
      const msg = la(`Request to ${actionName} the engine has been sent to the server.`);
      this.props.addNotification(<span>{msg}</span>, 'info');

      this.props.editProvision(data, viewId);
    };

    if (data.desiredState === 'STOPPED') {
      this.handleStopProvision(commitChange);
    } else {
      commitChange();
    }
  };

  handleEditProvision = (entity) => {
    this.props.openEditProvisionModal(entity.get('id'), entity.get('clusterType'));
  };

  handleAdjustWorkers = (entity) => {
    this.props.openAdjustWorkersModal(entity.get('id'));
  };

  selectEngine = (engineId) => {
    this.setState({selectedEngineId: engineId});
  };
  unselectEngine = () => {
    this.setState({selectedEngineId: null});
  };

  openAddProvisionModal = () => {
    // use cluster type to open form for this type if there is only one choice
    let clusterType = null;
    if (PROVISION_MANAGERS.length === 1) {
      clusterType = PROVISION_MANAGERS[0].clusterType;
    }
    this.openAdd(this.props, clusterType);
  };

  handleSelectClusterType = (clusterType) => {
    this.openAdd(this.props, clusterType);
  };

  stopPollingProvisionData() {
    clearTimeout(this.pollId);
    this.pollId = undefined;
  }

  startPollingProvisionData = (isFirst) => {
    const pollAgain = () => {
      if (!this._isUnmounted && (isFirst || this.pollId)) {
        this.pollId = setTimeout(this.startPollingProvisionData, PROVISION_POLL_INTERVAL);
      }
    };
    let clusterType = null;
    if (PROVISION_MANAGERS.length === 1) {
      clusterType = PROVISION_MANAGERS[0].clusterType;
    }
    this. getProvision(this.props, clusterType, VIEW_ID, pollAgain);
  };

  getSelectedEngine = (id) => {
    return id && this.props.provisions.find(engine => engine.get('id') === id);
  };

  renderHeader() {
    const { selectedEngineId } = this.state;
    const selectedEngine = this.getSelectedEngine(selectedEngineId);
    const addNewButton = <Button
      style={{ width: 100, marginTop: 5}}
      onClick={this.openAddProvisionModal}
      type={ButtonTypes.NEXT}
      text={this.getBtnLabel()}
    />;
    return (selectedEngineId) ?
      <SingleEngineHeader
        engine={selectedEngine}
        unselectEngine={this.unselectEngine}
        handleEdit={this.handleEditProvision}
        handleStartStop={this.handleChangeProvisionState}
      /> :
      <Header
        titleStyle={{fontSize: 20}}
        title={la('Engines')}
        endChildren={addNewButton}
      />;
  }

  renderProvisions(selectedEngineId, provisions, viewState) {
    const selectedEngine = this.getSelectedEngine(selectedEngineId);
    const queues = this.getQueues();
    return (
      <div style={styles.baseContent}>
        {selectedEngineId && <SingleEngineView
          engine={selectedEngine} queues={queues} viewState={viewState}
        />}
        {!selectedEngineId && <ClusterListView
          editProvision={this.handleEditProvision}
          removeProvision={this.handleRemoveProvision}
          changeProvisionState={this.handleChangeProvisionState}
          adjustWorkers={this.handleAdjustWorkers}
          selectEngine={this.selectEngine}
          provisions={provisions}
          queues={queues}
        />}
      </div>
    );
  }

  render() {
    const { viewState, provisions } = this.props;
    const { selectedEngineId } = this.state;
    // want to not flicker the UI as we poll
    const isInFirstLoad = !this.pollId;
    return (
      <div id='admin-provisioning' style={page}>
        {this.renderHeader()}
        <ViewStateWrapper
          viewState={viewState}
          style={pageContent}
          hideChildrenWhenFailed={false}
          hideSpinner={!isInFirstLoad}
        >
          {this.renderProvisions(selectedEngineId, provisions, viewState)}
        </ViewStateWrapper>
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    viewState: getViewState(state, VIEW_ID),
    provisions: getAllProvisions(state)
  };
}

export default connect(mapStateToProps, {
  loadProvision,
  removeProvision,
  openAddProvisionModal,
  openEditProvisionModal,
  openAdjustWorkersModal,
  showConfirmationDialog,
  addNotification,
  editProvision,
  ...extraProvisingPageMapDispatchToProps
})(ProvisioningPage);

const styles = {
  baseContent: {
    display: 'flex',
    flexDirection: 'column',
    height: '100%'
  }
};
