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

import './Jdbcodbc.less';

export default class Jdbcodbc extends Component {
  constructor(props) {
    super(props);
  }


  render() {
    return (
      <div id='account-jdbcodbc'>
        <h2>JDBC/ODBC Access</h2>
        <div className='separator' />
        <div className='description'>
          These drivers have been tailored to your account credentials and your OS.
          For more information or assistance, please visit our <a href={ this }>help page</a>.
        </div>
        <div className='pageitems'>
          <h3>ODBC Driver</h3>
          <div className='description'>
            We recommend this driver for you. If you require another version,
            please visit our <a href={ this }>help page</a>.
          </div>
          <div className='action'>Dremio OBDC Driver for OSX</div>
          <h3>ODBC Driver</h3>
          <div className='description'>
            We recommend this driver for you.
          </div>
          <div className='action'>Dremio JDBC Driver</div>
          <h3>BI Tool Connections</h3>
          <div className='description'>
            Follow the instructions to connect your BI tool via the ODBC or JDBC driver.
          </div>
          <div className='action'>Tableau</div>
          <div className='action'>Excel 2010+</div>
          <div className='action'>Power BI</div>
        </div>
      </div>
    );
  }
}
