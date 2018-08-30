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
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import Art from 'components/Art';

import { tabLabel, tabIcon } from '@app/uiTheme/less/layout.less';
import { nav, navBtn, navBtnActive } from './NavPanel.less';


@pureRender
export default class NavPanel extends Component {
  static propTypes = {
    changeTab: PropTypes.func.isRequired,
    activeTab: PropTypes.string,
    tabs: PropTypes.instanceOf(Immutable.OrderedMap)
  };

  renderTabLabel = ({
    text,
    icon = null
  }) => {
    return (
      <span className={tabLabel}>
        <span>{text}</span>
        {icon &&
        <span className={tabIcon}>
          <Art
            src={icon.name}
            alt={icon.alt || ''}
            style={icon.style || ''}/>
        </span>
        }
      </span>
    );
  };

  render() {
    if (this.props.tabs.count() <= 1) {
      return null;
    }

    const children = this.props.tabs.map((tab, key) => {
      const labelConfig = (typeof tab === 'string') ? {text: tab} : tab;
      return <div
        data-qa={key}
        key={key}
        onClick={this.props.changeTab.bind(this, key)}
        className={this.props.activeTab === key ? navBtnActive : navBtn}>
        {this.renderTabLabel(labelConfig)}
      </div>;
    }).toArray();

    return <div data-qa='nav-panel' className={nav}>{children}</div>;
  }
}
