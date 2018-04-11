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
import React, {Component} from 'react';
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import Immutable from 'immutable';
import invariant from 'invariant';

import {createProvision, editProvision} from 'actions/resources/provisioning';
import {getEntity} from 'selectors/resources';
import {showConfirmationDialog} from 'actions/confirmation';
import {PROVISION_MANAGERS} from 'constants/provisioningPage/provisionManagers';
import FormUnsavedWarningHOC from 'components/Modals/FormUnsavedWarningHOC';
import SelectClusterType from 'pages/AdminPage/subpages/Provisioning/SelectClusterType';
import Modal from 'components/Modals/Modal';
import ApiUtils from 'utils/apiUtils/apiUtils';

import * as provisioningForms from '../forms/provisioning';

import './Modal.less';

const VIEW_ID = 'AddProvisionModal';

export class AddProvisionModal extends Component {
  static contextTypes = {
    router: PropTypes.object.isRequired
  };
  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    location: PropTypes.object,
    pathname: PropTypes.string,
    query: PropTypes.object,
    clusterType: PropTypes.string,
    provision: PropTypes.instanceOf(Immutable.Map),
    provisionId: PropTypes.string,
    createProvision: PropTypes.func,
    editProvision: PropTypes.func,
    showConfirmationDialog: PropTypes.func,
    updateFormDirtyState: PropTypes.func
  };

  getProvisionVersion(props) {
    return props.provision.get('version');
  }

  getProvisionManager() {
    const { clusterType } = this.props;
    return Immutable.fromJS(PROVISION_MANAGERS.find(manager => manager.clusterType === clusterType) || {});
  }

  getModalTitle() {
    const provision = this.getProvisionManager();
    const label = provision.get('label');

    return `${this.isEditMode() ? la('Edit') : la('Set Up')}${label ? ` ${label}` : ''}`; // todo: proper sub-pattern loc
  }

  handleSelectClusterType  = (clusterType) => {
    const { location } = this.props;
    // location.state gets merged into this.props by ModalsContainer
    this.context.router.push({...location, state: {...location.state, clusterType}});
  }

  promptEditProvisionRestart(values) {
    return new Promise((resolve, reject) => {
      this.props.showConfirmationDialog({
        title: la('Restart Cluster'),
        confirmText: la('Restart'),
        confirm: () => resolve(this.editProvision(values)),
        cancel: reject,
        text: [
          la('Saving these settings requires a restart of the cluster. Existing jobs will be halted.'),
          la('Are you sure you want to continue?')
        ]
      });
    });
  }

  editProvision(values) {
    return ApiUtils.attachFormSubmitHandlers(
      this.props.editProvision(values, VIEW_ID)
    ).then(() => this.props.hide(null, true));
  }

  submit = (values, isRestartRequired) => {
    if (this.isEditMode()) {
      if (isRestartRequired) {
        return this.promptEditProvisionRestart(values);
      }

      return this.editProvision(values);
    }
    return ApiUtils.attachFormSubmitHandlers(
      this.props.createProvision(values, VIEW_ID)
    ).then(() => this.props.hide(null, true));
  };

  /**
   * Returns true when provisionId passed to location state
   */
  isEditMode() {
    const { provisionId } = this.props;
    return Boolean(provisionId);
  }

  render() {
    const { isOpen, hide, updateFormDirtyState, provision, clusterType } = this.props;
    const title = this.getModalTitle();

    const clusterTypeForm = clusterType && provisioningForms[clusterType];
    invariant(!clusterType || clusterTypeForm, `clusterType (${clusterType}) not a valid provisioningForm type`);

    return (
      <Modal
        title={title}
        size='large'
        isOpen={isOpen}
        hide={hide}>
        {clusterTypeForm ? React.createElement(clusterTypeForm, {
          onFormSubmit: this.submit,
          onCancel: hide,
          style: styles.formBody,
          getConflictedValues: this.getProvisionVersion,
          provision,
          updateFormDirtyState
        }) : <SelectClusterType
          style={styles.stepOneStyle}
          onSelectClusterType={this.handleSelectClusterType}
          clusters={Immutable.fromJS(PROVISION_MANAGERS)}
          />}
      </Modal>
    );
  }
}

function mapStateToProps(state, props) {
  return {
    provision: getEntity(state, props.provisionId, 'provision') || Immutable.Map()
  };
}

export default connect(mapStateToProps, {
  createProvision,
  editProvision,
  showConfirmationDialog
})(FormUnsavedWarningHOC(AddProvisionModal));

const styles = {
  formBody: {
    width: 860,
    margin: '0 auto'
  },
  stepOneStyle: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center'
  }
};
