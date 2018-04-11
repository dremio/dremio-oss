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
import React, { Component } from 'react';
import classNames from 'classnames/bind';
import Radium from 'radium';

import PropTypes from 'prop-types';

import FontIcon from 'components/Icon/FontIcon';

@Radium
class RightTreeTabHeader extends Component {
  render() {
    const classes = classNames('tab', this.props.className, {'tab-active':this.props.active});
    const icons = {
      Settings: 'Settings',
      Activity: 'Activity'
    };
    return (
      <li
        className={classes}
        onClick={this.props.onClick.bind(this, this.props.tabId)}
        style={[styles.tab]}>
        <FontIcon type={icons[this.props.tabId]}/>
        <span className='title'>{this.props.title}</span>
      </li>
    );
  }
}

RightTreeTabHeader.propTypes = {
  tabId: PropTypes.string.isRequired,
  title: PropTypes.string.isRequired,
  className: PropTypes.string,
  active: PropTypes.bool.isRequired,
  onClick: PropTypes.func.isRequired
};

export default class RightTreeTabs extends Component {

  static propTypes = {
    toggleVisibility: PropTypes.func,
    children: PropTypes.node.isRequired,
    className: PropTypes.string
  };

  constructor(props) {
    super(props);
    this.handleSelect = this.handleSelect.bind(this);
    this.state = {activeTab: 'Settings'};
  }

  getHeaderItems() {
    return React.Children.map(this.props.children, (child) => {
      return (
        <RightTreeTabHeader
          tabId={child.props.tabId}
          title={child.props.title}
          className={child.props.className}
          active={child.props.tabId === this.state.activeTab}
          onClick={this.handleSelect}
          key={child.props.title}
        />
      );
    });
  }

  handleSelect(key) {
    this.setState({activeTab: key});
  }

  render() {
    const activeTab = React.Children.map(this.props.children, (child) => {
      return child.props.tabId === this.state.activeTab ? child : undefined;
    }).find((child) => child);
    return (
      <div className='right-tree-tabs' style={{ width: 250 }}>
        <ul
          className='tab-list'
          style={styles.tabList}>
          {this.getHeaderItems()}
          <li
            onClick={this.props.toggleVisibility}
            style={styles.closeLink}>
            <FontIcon type='Collapse' />
          </li>
        </ul>
        {activeTab}
      </div>
    );
  }
}

const styles = {
  tabList: {
    margin: 0,
    padding: 0,
    height: 38,
    overflow: 'hidden',
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    background: '#ccc'
  },
  tab: {
    width: 105,
    height: 38,
    padding: 5,
    display: 'flex',
    alignItems: 'center',
    cursor: 'pointer',
    borderRight: '1px solid rgba(0,0,0,.1)'
  },
  closeLink: {
    display: 'flex',
    alignItems: 'center',
    cursor: 'pointer',
    marginRight: 14
  }
};
