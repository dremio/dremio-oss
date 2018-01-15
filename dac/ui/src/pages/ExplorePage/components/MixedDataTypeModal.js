/*
 * Copyright (C) 2017 Dremio Corporation
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
import Modal from 'components/Modal';
// import AddFileController from './AddFileController';


class RealOne extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div>
        <p> <b>Hello Modal!</b> </p>
      </div>
    );
  }
}

export default class MixedDataTypeModal extends Component {
  static propTypes = {
    hide: PropTypes.func
  };

  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div className='add-file-modal'>
        <Modal hide={this.props.hide}>
          <RealOne> </RealOne>
        </Modal>
      </div>
    );
  }
}
