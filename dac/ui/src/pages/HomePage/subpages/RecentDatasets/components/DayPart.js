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
import CopyButton from 'components/Buttons/CopyButton';
import moment from 'moment';

import LinkButton from 'components/Buttons/LinkButton';
import { inline } from 'uiTheme/radium/buttons';

import './DayPart.less';

export default class DayPart extends Component {

  static propTypes = {
    data: PropTypes.array.isRequired,
    name: PropTypes.string.isRequired,
    removeFromHistory: PropTypes.func.isRequired
  };

  constructor(props) {
    super(props);
  }

  getItems() {
    return this.props.data.map((item) => {
      return (
        <div key={item.id} className='day-row'>
          <div className='name'>
            {item.name}
            <CopyButton text={item.name || ''} />
          </div>
          <div className='lastOpened'>{moment(item.lastOpened).format('MM/DD/YYYY')}</div>
          <div className='actions'>
            <button
              className='remove'
              onClick={this.props.removeFromHistory.bind(this, item.id)} style={inline}>
              Remove from History
            </button>
            <LinkButton buttonStyle='inline' to={this.getExploreUrl(item.name)}>Open</LinkButton>
            <LinkButton buttonStyle='inline' to={this.getExploreUrl(item.name)}>Query</LinkButton>
          </div>
        </div>
      );
    });
  }

  getExploreUrl(name) {
    const params = name.split('/');
    return '/spaces/' + params[0] + '/' + params[params.length - 1];
  }

  render() {
    const date = this.props.name === 'yesterday' || this.props.name === 'today'
      ? this.props.name
      : moment(+this.props.name).format('MMMM DD, YYYY');
    return (
      <div className='day-part'>
        <div className='title'>{date}</div>
        <div className='day-table'>
          <div className='day-thead'>
            <div className='name th'>Name</div>
            <div className='lastOpened th'>Last Opened</div>
            <div className='actions th'>Actions</div>
          </div>
          {this.getItems()}
        </div>
      </div>
    );
  }
}
