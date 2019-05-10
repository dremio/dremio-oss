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
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import classNames from 'classnames';

@Radium
export default class TabsNavigationItem extends Component {
  static propTypes = {
    activeTab: PropTypes.string,
    onClick: PropTypes.func,
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    children: PropTypes.node
  }
  getStylesForTab(tabName) {
    const { activeTab } = this.props;
    const style = [styles.tab];
    if (activeTab === tabName) {
      style.push(styles.tabActive);
    }
    return style;
  }
  render() {
    const { item, onClick, activeTab, children } = this.props;
    const tabName = item.get('name');
    const key = `tab-${tabName}`;
    return (
      <div
        className={classNames({'tab-link': true, 'active': activeTab === tabName})}
        style={this.getStylesForTab(tabName)}
        key={key}
        onClick={onClick}
        data-qa={key}
      >
        {children}
      </div>
    );
  }
}

const styles = {
  tab: {
    padding: '12px 10px 8px',
    textAlign: 'center',
    cursor: 'pointer',
    borderBottom: '3px solid transparent',
    ':hover': {
      'background': '#f3f3f3'
    }
  },
  tabActive: {
    borderBottom: '3px solid #77818f'
  }
};
