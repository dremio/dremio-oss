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
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { injectIntl } from 'react-intl';

import Modal from 'components/Modals/Modal';

import EditColumns from './EditColumns';

@injectIntl
@pureRender
export default class EditColumnsModal extends Component {
  static propTypes = {
    hide: PropTypes.func,
    isOpen: PropTypes.bool.isRequired,
    pathname: PropTypes.string,
    intl: PropTypes.object.isRequired
  };

  static contextTypes = {
    location: PropTypes.object
  }

  constructor(props) {
    super(props);
  }

  render() {
    const { hide, isOpen, intl } = this.props;
    return (
      <Modal
        hide={hide}
        size='small'
        title={intl.formatMessage({ id: 'Dataset.EditFieldVisibility' })}
        isOpen={isOpen}>
        <EditColumns
          hide={hide}
          location={this.context.location}
        />
      </Modal>
    );
  }
}
