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
import Immutable from 'immutable';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import {title, contextCard, cardTitle, contextAttrs, attrLabel, attrValue} from 'uiTheme/radium/rightContext';

@pureRender
@Radium
export default class Folder extends Component {

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map)
  };

  static contextTypes = {
    username: PropTypes.string
  }

  constructor(props) {
    super(props);
  }

  render() {
    const {entity} = this.props;
    return <div>
      <h4 style={title}>About this Space</h4>
      <div style={contextCard}>
        <div style={cardTitle}>Details</div>
        <ul style={contextAttrs}>
          <li style={entity.get('ctime') ? {display: 'block'} : {display: 'none'}}>
            <span style={attrLabel}>Created At:</span>
            <span style={attrValue}>{entity.get('ctime')}</span>
          </li>
        </ul>
        <div className='description'>
          {entity.get('description')}
        </div>
      </div>

    </div>;
  }
}
