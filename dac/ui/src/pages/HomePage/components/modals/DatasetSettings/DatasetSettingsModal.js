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
import FormUnsavedWarningHOC from 'components/Modals/FormUnsavedWarningHOC';
import { injectIntl } from 'react-intl';

import DatasetSettings from './DatasetSettings.js';
import './../Modal.less';

@injectIntl
export class DatasetSettingsModal extends Component {

  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    entityType: PropTypes.string,
    entityId: PropTypes.string,
    fullPath: PropTypes.string,
    tab: PropTypes.string,
    // connected from FormUnsavedWarningHOC
    updateFormDirtyState: PropTypes.func,
    location: PropTypes.object,
    intl: PropTypes.object.isRequired
  };

  render() {
    const {isOpen, updateFormDirtyState, intl, ...datasetSettingsProps} = this.props;
    return (
      <Modal
        size='large'
        title={intl.formatMessage({ id: 'Dataset.Settings' })}
        isOpen={isOpen}
        hide={datasetSettingsProps.hide}>
        <DatasetSettings {...datasetSettingsProps} updateFormDirtyState={updateFormDirtyState}/>
      </Modal>
    );
  }
}

export default FormUnsavedWarningHOC(DatasetSettingsModal);
