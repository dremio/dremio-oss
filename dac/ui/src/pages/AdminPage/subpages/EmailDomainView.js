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
import Radium from 'radium';

import Header from '../components/Header';
import './EmailDomain.less';

@Radium
@pureRender
class EmailDomainView extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    /* TODO to Vasyl, we have possibility to check couple of radio buttons, I guess is's wrong */
    return (
      <div id='admin-email'>
        <Header title={la('Email Domain and Server')}/>
        <h2>Email Domain</h2>
        <div className='admin-email email-domain'>
          <input type='checkbox' />
          <label>Restrict user email addresses to a specific domain (e.g. company.com)</label>
          <div className='admin-email input-text'>
            <label>Domain</label>
            <input type='text' placeholder='dremio.test' ></input>
          </div>
        </div>
        <div className='separator short' />
        <h2>Email Server</h2>
        <div className='property server-email'>
          <div className='server radio-one'>
            <input type='radio' />
            <t>Use Dremio`s email service (requires internet connection)</t>
          </div>
          <div className='property radio-two'>
            <input type='radio' />
            <t>Use a SMTP server:</t>
          </div>
          <div className='property smtp'>
            <label>SMTP server</label>
            <input type='text' />
          </div>
          <div className='property port'>
            <label>Port</label>
            <input type='text' placeholder='25' />
          </div>
          <div className='property username'>
            <label>Username</label>
            <input type='text' />
          </div>
          <div className='property password'>
            <label>Password</label>
            <input type='text' />
          </div>
          <div className='property tls'>
            <input type='checkbox' />
            <label>Use TLS</label>
          </div>
          <div className='property address'>
            <label>From Address</label>
            <input type='text' placeholder='Dre Mio <dremio@dremio.test>' />
          </div>
        </div>
        <div className='separator' />
        <div className='email'>
          <button className='email-blu'>Save</button>
          <button className='email-grey'>Cancel</button>
        </div>
      </div>
    );
    /* TODO to Vasyl, need to add action on buttons */
  }
}

export default EmailDomainView;
