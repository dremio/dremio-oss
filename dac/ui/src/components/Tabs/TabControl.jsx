/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { PureComponent } from "react";
import Immutable from "immutable";
import PropTypes from "prop-types";

import * as classes from "./TabControl.module.less";
import clsx from "clsx";

class TabControl extends PureComponent {
  static propTypes = {
    tabs: PropTypes.instanceOf(Immutable.Map),
    onTabChange: PropTypes.func,
    tabBaseStyle: PropTypes.object,
    tabSelectedStyle: PropTypes.object,
    style: PropTypes.object,
  };

  constructor(props) {
    super(props);
    this.state = {
      selectedTab: this.props.tabs.keySeq().first(),
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
    const tabsView = this.props.tabs.keySeq().map((tab) => {
      const selected = tab === this.state.selectedTab;
      return (
        <div
          key={tab}
          onClick={() => this.changeSelectedTab(tab)}
          className={clsx(
            "tab-item",
            classes["tabBase"],
            classes["tabBaseStyle"],
            {
              [classes["tabSelected"]]: selected,
              [classes["tabSelectedStyle"]]: selected,
            }
          )}
        >
          {tab}
        </div>
      );
    });
    return <div className={clsx(classes["tabsWrap"])}>{tabsView}</div>;
  }

  render() {
    return (
      <div className={clsx(classes["tabRoot"])} style={this.props.style}>
        {this.renderTabs()}
        {this.props.tabs.get(this.state.selectedTab)}
      </div>
    );
  }
}

export default TabControl;
