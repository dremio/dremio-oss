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

import './AmazonS3.less';

export default class AmazonS3 extends Component {

  constructor(props) {
    super(props);
  }

  render() {
    const ownerName = 'anonymous';
    return (
      <div className='amazon-s3'>
        <div className='logo'></div>
        <div className='owner'>Owner: {ownerName}</div>
        <div className='description'>
          <div className='option name'>
            <label>Name: </label>
            <input className='name-input'/>
          </div>
          <div className='option description'>
            <label>Description: </label>
            <input className='description-input'/>
          </div>
        </div>
        <div className='credentials'>
          <span>Credentials</span>
          <hr />
          <div className='input-block'>
            <div className='option'>
              <label>Access Key ID: </label>
              <input />
            </div>
            <div className='option'>
              <label>Secret Access Key: </label>
              <input className='key'/>
            </div>
          </div>
        </div>
        <div className='workspaces'>
          <span>Workspaces</span>
          <hr />
          <div className='workspaces-table'>
            <div className='table'>
              <div className='thead'>
                <div className='name th'>Name</div>
                <div className='path th'>Path</div>
              </div>
              <div className='tr root'>
                <div className='name th'>root</div>
                <div className='path th'>s3://mybucket/</div>
              </div>
              <div className='tr home'>
                <div className='name td'>root</div>
                <div className='path td'>s3://mybucket/home/$user</div>
              </div>
            </div>
          </div>
        </div>
        <LinkButton to='/' buttonStyle='primary'>Submit</LinkButton>
      </div>
    );
  }
}
