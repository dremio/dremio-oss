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
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { Component } from 'react';
import './MongoDBCredential.less'; // TODO to Vasyl, need to use Radium

@pureRender
export default class MongoDB extends Component {
  static propTypes = {
    credential: PropTypes.object,
    onRemoveClick: PropTypes.func
  };

  constructor(props) {
    super(props);
  }
  render() {
    const {credential} = this.props;
    return (
      <div className='credential mongodb'>
        <div className='header-container'>
          <div className='header'>{credential.name}</div>
          <div className='remove action' onClick={this.props.onRemoveClick} >remove</div>
        </div>
        <div className='body-container'>
          <div className='property property-username'>
            <label>Username</label>
            <input id='username' type='text' defaultValue={credential.properties.username} />
          </div>
          <div className='property property-password'>
            <label>Password</label>
            <input id='password' type='password' defaultValue={credential.properties.password} />
          </div>
        </div>
      </div>
    );
  }
}
