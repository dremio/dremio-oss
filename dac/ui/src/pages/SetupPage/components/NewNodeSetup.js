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
import LinkButton from '../../../components/Buttons/LinkButton';

import AdvancedOptions from './AdvancedOptions';
import './NewNodeSetup.less';

export default class NewNodeSetup extends Component {

  constructor(props) {
    super(props);
    this.toogleAdvancedOptions = this.toogleAdvancedOptions.bind(this);
    this.state = {
      isOptionsOpened: false
    };
  }

  toogleAdvancedOptions() {
    this.setState({
      isOptionsOpened: !this.state.isOptionsOpened
    });
  }

  render() {
    const options = this.state.isOptionsOpened
      ? <AdvancedOptions />
      : <div />;


    return (
      <div className='new-node-setup setup-block'>
        <div className='input-block'>
          <input type='radio'
            id='new-node-cluster'
            name='node-type'/>
          <label htmlFor='new-node-cluster'>
            New node cluster
          </label>
        </div>
        <LinkButton to='/setup/selectdb' className='start-btn' buttonStyle='primary'>Start</LinkButton>
        <span className='advanced'
          onClick={this.toogleAdvancedOptions}>
          advanced
        </span>
        {options}
      </div>
    );
  }
}
