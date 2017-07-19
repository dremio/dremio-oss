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

import LinkButton from 'components/Buttons/LinkButton';

import './AddToExisting.less';

export default class AddToExisting extends Component {

  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div className='add-to-existing setup-block'>
        <div className='input-block'>
          <input type='radio'
            id='add-to-existing'
            name='node-type'/>
          <label htmlFor='add-to-existing'>
              Add to Existing Cluster
          </label>
        </div>
        <form>
          <div className='form-row'>
            <span className='input-text'>Existing node</span>
            <input type='text' className='longer'/>
          </div>
          <div className='form-row'>
            <span className='input-text'>User (Must be admin)</span>
            <input type='text'/>
          </div>
          <div className='form-row'>
            <span className='input-text'>Password</span>
            <input type='password'/>
          </div>
          <div className='form-row button-row'>
            <LinkButton to='/setup/selectdb' buttonStyle='primary' className='start-btn'>Start</LinkButton>
          </div>
        </form>
      </div>
    );
  }
}
