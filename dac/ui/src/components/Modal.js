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

import './Modal.less';

export default class Modal extends Component {

  static propTypes = {
    hide: PropTypes.func.isRequired,
    children: PropTypes.node
  };

  render() {
    return (
      <div className='modal-white'>
        <div className='overlay' onClick={this.props.hide}/>
        <div className='header-overlay' onClick={this.props.hide}/>
        <div className='modal-white-content' onClick={this.props.hide}>
          {this.props.children}
        </div>
      </div>
    );
  }
}
