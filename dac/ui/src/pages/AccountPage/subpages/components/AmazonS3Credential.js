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
import { formLabel } from 'uiTheme/radium/typography';
import FontIcon from 'components/Icon/FontIcon';

import './AmazonS3Credential.less';  // TODO to Vasyl, need to use Radium

@pureRender
export default class AmazonS3 extends Component {
  static propTypes = {
    credential: PropTypes.object,
    onRemoveClick: PropTypes.func
  }

  constructor(props) {
    super(props);
  }

  render() {
    const {credential} = this.props;
    return (
      <div className='credential amazon'>
        <div className='header-container'>
          <h3 className='header'>{credential.name}</h3>
          <div className='remove' onClick={this.props.onRemoveClick}>
            <FontIcon type='CanceledGray' style={{verticalAlign: 'middle', marginRight: 5}}/>
            {la('Remove')}
          </div>
        </div>
        <div className='body-container'>
          <div className='property property-accessKeyId'>
            <label style={formLabel}>{la('Access Key ID')}</label>
            <input id='accessKeyId' type='text' defaultValue={credential.properties.accessKeyId}  />
          </div>
          <div className='property property-secretAccessKey'>
            <label style={formLabel}>{la('Secret Access Key')}</label>
            <input id='secretAccessKey' type='password' defaultValue={credential.properties.secretAccessKey} />
          </div>
        </div>
      </div>
    );
  }
}
