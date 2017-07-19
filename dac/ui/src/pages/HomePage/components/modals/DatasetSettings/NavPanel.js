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
import {Component, PropTypes} from 'react';
import pureRender from 'pure-render-decorator';
import Radium from 'radium';
import Immutable from 'immutable';

import { FLEX_COL_START_START } from 'uiTheme/radium/flexStyle';
import { body } from 'uiTheme/radium/typography';
import { PALE_BLUE } from 'uiTheme/radium/colors';

// todo: abstract out style and move this to generic components

@pureRender
@Radium
export default class NavPanel extends Component {
  static propTypes = {
    changeTab: PropTypes.func.isRequired,
    activeTab: PropTypes.string,
    tabs: PropTypes.instanceOf(Immutable.OrderedMap)
  };

  render() {
    if (this.props.tabs.count() <= 1) {
      return null;
    }

    const children = this.props.tabs.map((text, key) => {
      return <div
        data-qa={key}
        key={key}
        onClick={this.props.changeTab.bind(this, key)}
        style={[styles.btn, this.props.activeTab === key && styles.active]}>{text}</div>;
    }).toArray();

    return <div data-qa='nav-panel' style={styles.nav}>{children}</div>;
  }
}


const styles = {
  nav: {
    ...FLEX_COL_START_START,
    backgroundColor: PALE_BLUE,
    width: 200,
    padding: 5,
    flexShrink: 0
  },
  btn: {
    marginLeft: 10,
    marginRight: 10,
    width: 170,
    ...body,
    padding: '5px 5px',
    border: '1px solid rgba(0,0,0,0)',
    height: 25,
    cursor: 'pointer',
    ':hover': {
      border: '1px solid rgba(0,0,0,0.05)'
    }
  },
  active: {
    border: '1px solid rgba(0,0,0,0)',
    backgroundColor: 'rgba(0,0,0,0.05)',
    ':hover': {
      border: '1px solid transparent'
    }
  }
};
