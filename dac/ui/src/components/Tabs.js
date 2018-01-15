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
import React, { Component } from 'react';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

@pureRender
export default class Tabs extends Component {

  static propTypes = {
    children: PropTypes.node.isRequired,
    activeTab: PropTypes.string,
    style: PropTypes.object
  };

  render() {
    const { children, activeTab, style } = this.props;
    return (
      <div className='tab-wrapper' style={style}>
        {React.Children.map(children, (item) => {
          if (activeTab === item.props.tabId) {
            return item;
          }
        })}
      </div>
    );
  }
}
