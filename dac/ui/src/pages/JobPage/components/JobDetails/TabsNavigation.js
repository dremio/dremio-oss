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
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { injectIntl } from 'react-intl';

import TabsNavigationItem from './TabsNavigationItem';

@injectIntl
@Radium
@PureRender
export default class TabsNavigation extends Component {
  static propTypes = {
    activeTab: PropTypes.string.isRequired,
    changeTab: PropTypes.func,
    style: PropTypes.object,
    attemptDetails: PropTypes.instanceOf(Immutable.List),
    showJobProfile: PropTypes.func,
    intl: PropTypes.object.isRequired
  }

  constructor(props) {
    super(props);

    this.tabHash = Immutable.fromJS([
      {name: 'overview', label: props.intl.formatMessage({ id: 'Common.Overview' })},
      {name: 'details', label: props.intl.formatMessage({ id: 'Common.Details' })},
      {name: 'acceleration', label: props.intl.formatMessage({ id: 'Acceleration.Acceleration' })},
      {name: 'profiles', label: props.intl.formatMessage({ id: 'Job.Profiles' })},
      {name: 'profile', label: props.intl.formatMessage({ id: 'Job.Profile' })}
    ]);
  }

  render() {
    const { activeTab, style, attemptDetails } = this.props;
    const isHaveSingleProfile = attemptDetails && attemptDetails.size < 2;
    const tabs = this.tabHash.map((item) => {
      const name = item.get('name');
      if (isHaveSingleProfile && name === 'profile') {
        const profileUrl = attemptDetails.getIn([0, 'profileUrl']);
        return (
          <TabsNavigationItem
            key={name}
            item={item}
            activeTab={activeTab}
            onClick={() => this.props.showJobProfile(profileUrl)}>

            {item.get('label')}
          </TabsNavigationItem>
        );
      }
      if (isHaveSingleProfile && name === 'profiles'
          || !isHaveSingleProfile && name === 'profile') {
        return null;
      }
      return (
        <TabsNavigationItem
          key={name}
          item={item}
          activeTab={activeTab}
          onClick={() => this.props.changeTab(name)}>

          {item.get('label')}
        </TabsNavigationItem>
      );
    });
    return (
      <div className='tabs-holder' style={[styles.base, style]}>
        {tabs}
      </div>
    );
  }
}

const styles = {
  base: {
    height: 38,
    borderBottom: '1px solid #f3f3f3',
    display: 'flex'
  }
};
