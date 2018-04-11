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
import Immutable  from 'immutable';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import { formDefault } from 'uiTheme/radium/typography';
import { SECONDARY_BORDER } from 'uiTheme/radium/colors';

@pureRender
@Radium
export default class TabControl extends Component {
  static propTypes = {
    tabs: PropTypes.instanceOf(Immutable.Map),
    onTabChange: PropTypes.func,
    tabBaseStyle: PropTypes.object,
    tabSelectedStyle: PropTypes.object,
    style: PropTypes.object
  }

  constructor(props) {
    super(props);
    this.state = {
      selectedTab: this.props.tabs.keySeq().first()
    };
  }

  changeSelectedTab(tab) {
    if (tab !== this.state.selectedTab) {
      this.setState({ selectedTab: tab });
    }
    if (this.props.onTabChange) {
      this.props.onTabChange(tab);
    }
  }

  renderTabs() {
    const tabsView = this.props.tabs.keySeq().map( (tab) => {
      const selected = tab === this.state.selectedTab;
      return (
        <div
          key={tab}
          className='tab-item'
          onClick={ () => this.changeSelectedTab(tab) }
          style={[styles.tabBase, this.props.tabBaseStyle && this.props.tabBaseStyle,
            selected && styles.tabSelected,
            (selected && this.props.tabSelectedStyle) && this.props.tabSelectedStyle]}>
          {tab}
        </div>
      );
    });
    return <div style={styles.tabsWrap}>{tabsView}</div>;
  }

  render() {
    return (
      <div style={[styles.tabRoot, this.props.style]}>
        {this.renderTabs()}
        {this.props.tabs.get(this.state.selectedTab)}
      </div>
    );
  }
}

const styles = {
  tabRoot: {
    display: 'flex',
    flexDirection: 'column'
  },
  tabsWrap: {
    flexShrink: 0,
    paddingBottom: 12,
    paddingTop: 12
  },
  tabBase: {
    display: 'inline-block',
    color: 'black',
    padding: '5px 10px',
    cursor: 'pointer',
    textAlign: 'center',
    borderRadius: 2,
    ...formDefault,
    ':hover': {
      textDecoration: 'underline'
    }
  },
  tabSelected: {
    backgroundColor: SECONDARY_BORDER,
    cursor: 'default',
    ':hover': {
      textDecoration: 'none'
    }
  }
};
