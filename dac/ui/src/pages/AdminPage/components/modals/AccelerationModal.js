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
import Modal from 'components/Modals/Modal';
import AccelerationController from 'components/Acceleration/AccelerationController';
import FormUnsavedWarningHOC from 'components/Modals/FormUnsavedWarningHOC';
import './Modal.less';

export class AccelerationModal extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    location: PropTypes.object,
    // connected from FormUnsavedWarningHOC
    updateFormDirtyState: PropTypes.func
  };

  render() {
    const {isOpen, hide, location} = this.props;
    const { datasetId } = location.state || {};

    return (
      <Modal
        size='large'
        title={la('Acceleration')}
        isOpen={isOpen}
        hide={hide}>
        <AccelerationController
          updateFormDirtyState={this.props.updateFormDirtyState}
          onCancel={hide}
          onDone={hide}
          datasetId={datasetId}
         />
      </Modal>
    );
  }
}

export default FormUnsavedWarningHOC(AccelerationModal);
