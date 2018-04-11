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
import { connect } from 'react-redux';

import { loadProvision, removeProvision, openAddProvisionModal, openEditProvisionModal, editProvision }
  from 'actions/resources/provisioning';
import { showConfirmationDialog } from 'actions/confirmation';
import { getViewState } from 'selectors/resources';
import { getAllProvisions } from 'selectors/provision';
import { PROVISION_MANAGERS } from 'constants/provisioningPage/provisionManagers';
import Header from 'pages/AdminPage/components/Header';
import ViewStateWrapper from 'components/ViewStateWrapper';
import Button from 'components/Buttons/Button';
import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import { formDescription } from 'uiTheme/radium/typography';
import { page, pageContent } from 'uiTheme/radium/general';

import ClusterListView from './ClusterListView';
import SelectClusterType from './SelectClusterType';

const VIEW_ID = 'ProvisioningPage';
const PROVISION_POLL_INTERVAL = 3000;

@Radium
export class ProvisioningPage extends Component {
  static propTypes = {
    viewState: PropTypes.instanceOf(Immutable.Map),
    provisions: PropTypes.instanceOf(Immutable.List),
    loadProvision: PropTypes.func,
    removeProvision: PropTypes.func,
    openAddProvisionModal: PropTypes.func,
    openEditProvisionModal: PropTypes.func,
    showConfirmationDialog: PropTypes.func,
    editProvision: PropTypes.func
  };

  state = {
    showProvisionList: false
  }

  pollId = 0

  _isUnmounted = false

  componentWillMount() {
    this.startPollingProvisionData(true);
  }

  componentWillUnmount() {
    this._isUnmounted = true;
    this.stopPollingProvisionData();
  }

  removeProvision = (entity) => {
    this.props.removeProvision(entity.get('id'), VIEW_ID).then(() => {
      this.props.loadProvision('yarn', VIEW_ID);
    });
  }

  handleRemoveProvision = (entity) => {
    this.props.showConfirmationDialog({
      title: la('Remove Cluster'),
      text: la('Are you sure you want to remove this cluster?'),
      confirmText: la('Remove'),
      confirm: () => this.removeProvision(entity)
    });
  }

  handleStopProvision = (confirmCallback) => {
    this.props.showConfirmationDialog({
      title: la('Stop Cluster'),
      text: [
        la('Existing jobs will be halted.'),
        la('Are you sure you want to stop the cluster?')
      ],
      cancelText: la('Don\'t Stop Cluster'),
      confirmText: la('Stop Cluster'),
      confirm: confirmCallback
    });
  }

  // todo: consolidate with handleEditProvision
  handleChangeProvisionState = (desiredState, entity, viewId) => {
    const data = {
      ...entity.toJS(),
      desiredState
    };
    delete data.workersSummary; // todo: we add this in a decorator
    // todo: server should be ignoring these readonly fields on write
    delete data.containers;
    delete data.currentState;
    delete data.error;
    delete data.detailedError;

    const commitChange = () => this.props.editProvision(data, viewId);
    if (data.desiredState === 'STOPPED') {
      this.handleStopProvision(commitChange);
    } else {
      commitChange();
    }
  }

  handleEditProvision = (entity) => {
    this.props.openEditProvisionModal(entity.get('id'), entity.get('clusterType'));
  }

  openAddProvisionModal = () => {
    this.props.openAddProvisionModal(null);
  }

  handleSelectClusterType = (clusterType) => {
    this.props.openAddProvisionModal(clusterType);
  }

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
    this.props.loadProvision('yarn', VIEW_ID).then(pollAgain, pollAgain);
  }

  renderContent(isInFirstLoad) {
    const { viewState, provisions } = this.props;
    const hasProvisions = provisions.size > 0;
    const showInitialSetup = !hasProvisions && (!viewState.get('isInProgress') || !isInFirstLoad);

    return (
      <div style={styles.baseContent}>
        {showInitialSetup && <div style={{paddingTop: 15}}>
          <h4>{la('Provisioning Options')}</h4>
          <div style={formDescription}>{la('No provisioning option set up. Select one to get started.')}</div>
          <SelectClusterType
            onSelectClusterType={this.handleSelectClusterType}
            clusters={Immutable.fromJS(PROVISION_MANAGERS)}
          />
        </div>}
        {hasProvisions && <ClusterListView
          editProvision={this.handleEditProvision}
          removeProvision={this.handleRemoveProvision}
          changeProvisionState={this.handleChangeProvisionState}
          provisions={provisions}
        />}
      </div>
    );
  }

  render() {
    const { viewState } = this.props;
    // want to not flicker the UI as we poll
    const isInFirstLoad = !this.pollId;
    const addNewButton = <Button
      style={{ width: 100, marginTop: 5}}
      onClick={this.openAddProvisionModal}
      type={ButtonTypes.NEXT}
      text={la('Add New')}
    />;
    return (
      <div id='admin-provisioning' style={page}>
        <Header title={la('Provisioning')} endChildren={addNewButton} />
        <ViewStateWrapper
          viewState={viewState}
          style={pageContent}
          hideChildrenWhenFailed={false}
          hideSpinner={!isInFirstLoad}>

          {this.renderContent(isInFirstLoad)}
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
  showConfirmationDialog,
  editProvision
})(ProvisioningPage);

const styles = {
  baseContent: {
    display: 'flex',
    flexDirection: 'column',
    height: '100%'
  }
};
