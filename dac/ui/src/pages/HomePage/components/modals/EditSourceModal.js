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
import pureRender from 'pure-render-decorator';
import { injectIntl } from 'react-intl';
import FormUnsavedWarningHOC from 'components/Modals/FormUnsavedWarningHOC';

import Modal from 'components/Modals/Modal';
import EditSourceView from './EditSourceView';

@pureRender
@injectIntl
export class EditSourceModal extends Component {
  static contextTypes = {
    router: PropTypes.object.isRequired
  };

  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
    query: PropTypes.object,
    intl: PropTypes.object.isRequired
  };

  hide = () => {
    this.props.hide();
  }

  render() {
    const { isOpen, query, hide, updateFormDirtyState, intl } = this.props;
    return (
      <Modal
        size='medium'
        title={intl.formatMessage({ id: 'Source.EditSource' })}
        isOpen={isOpen}
        hide={hide}>
        { query.name &&
          <EditSourceView
            updateFormDirtyState={updateFormDirtyState}
            hide={this.hide}
            sourceName={query.name}
            sourceType={query.type}/>
        }
      </Modal>
    );
  }
}

export default FormUnsavedWarningHOC(EditSourceModal);
