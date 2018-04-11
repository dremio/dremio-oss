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
import Radium from 'radium';
import pureRender from 'pure-render-decorator';
import Immutable from 'immutable';

@Radium
@pureRender
export default class UsersV2 extends Component {

  constructor(props) {
    super(props);
    this.state = {
      activeBtn: 'internal'
    };
  }

  showRadioBtns() {
    const radioBtns = Immutable.fromJS([
      {
        id: 'internal',
        text: la('Internal (Authenticate users through email invilations)')
      }, {
        id: 'os',
        text: la('OS')
      },
      {
        id: 'LDAP',
        text: la('LDAP')
      }, {
        id: 'Authentication',
        text: la('Google Authentication')
      }
    ]);

    return radioBtns.map((radio) => {
      return  (
        <div className='item-block' key={radio.get('id')}>
          <input
            id={radio.get('id')}
            defaultChecked={radio.get('id') === this.state.activeBtn}
            name='credentials'
            onChange={this.toggleRadioBtns.bind(this, radio.get('id'))}
            className='item-input'
            type='radio' />
          <label htmlFor={radio.get('id')} className='item-label'>
            {radio.get('text')}
          </label>
        </div>
      );
    });
  }

  toggleRadioBtns(id) {
    this.setState({
      activeBtn: id
    });
  }

  render() {
    return (
      <div>
        <div className='admin-header' style={style.adminHeader}>
          <h3>{la('Authentication')}</h3>
        </div>
        <div style={style.wrapper}>
          <span style={style.title}>Authentication Type</span>
          {this.showRadioBtns()}
        </div>
      </div>
    );
  }
}

const style = {
  title: {
    fontWeight: 'bold'
  },
  wrapper: {
    width: '100%',
    marginTop: 10,
    paddingTop: 20
  },
  adminHeader: {
    display: 'flex',
    alignItems: 'center',
    borderBottom: '1px solid rgba(0,0,0,.1)',
    padding: '10px 0'
  }
};
