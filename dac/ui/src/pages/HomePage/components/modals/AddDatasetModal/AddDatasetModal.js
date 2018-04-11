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

import Modal from 'components/Modals/Modal';

import AddDatasetController from './AddDatasetController';

@pureRender
export default class AddDatasetModal extends Component {
  static contextTypes = {
    routeParams: PropTypes.object,
    location: PropTypes.object
  };

  static propTypes = {
    isOpen: PropTypes.bool.isRequired,
    hide: PropTypes.func.isRequired,
    query: PropTypes.object.isRequired,
    pathname: PropTypes.string
  };

  constructor(props) {
    super(props);
  }

  render() {
    const { isOpen, hide, pathname} = this.props;
    return (
      <div className='add-dataset-modal'>
        <Modal
          hide={hide}
          size='small'
          title='Create Copy of Existing Dataset'
          isOpen={isOpen}>
          <AddDatasetController
            location={this.context.location}
            hide={hide}
            pathname={pathname}
            routeParams={this.context.routeParams}/>
        </Modal>
      </div>
    );
  }
}


