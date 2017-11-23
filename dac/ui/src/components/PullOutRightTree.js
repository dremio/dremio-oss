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
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import RightTreeTabs from 'pages/HomePage/components/RightTreeTabs';
import RightTreeTab from 'pages/HomePage/components/RightTreeTab';

import { formDescription } from 'uiTheme/radium/typography';
import rightContext from './RightContext';

import './PullOutRightTree.less';

@Radium
class PullOutRightTree extends Component {
  static contextTypes = {
    location: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired
  };

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    entityType: PropTypes.string,
    resourceActivity: PropTypes.instanceOf(Immutable.List),
    toggleVisibility: PropTypes.func.isRequired
  };

  static defaultProps = {
    resourceActivity: new Immutable.List()
  };

  constructor(props) {
    super(props);
  }

  renderActivity() {
    const resourceActivity = this.props.resourceActivity;
    return resourceActivity.map((item, index) => this.renderActivityItem(item, index));
  }

  renderActivityItem(activity, index = 0) {
    const userName = activity.get('userName');
    const when = activity.get('when');
    const userAvatar = activity.get('userAvatar');
    const notes = this.getNotesLists(activity.get('notes'));
    const avatar = userAvatar ?
      <img style={styles.avatarImg} src={userAvatar} /> :
      <div className='activity-default-avatar'><i className='fa fa-user'></i></div>;
    return (
      <div className='home-about-activity-list-item' key={index} style={styles.listItem}>
        <div className='home-about-activity-author'>
          <div className='home-about-activity-author-avatar pull-left'
            style={styles.avatar}>
            {avatar}
          </div>
          <div className='home-about-activity-summary pull-left'>
            <div style={styles.topInfo}>
              <h5 className='home-about-activity-author-name' style={[styles.floatLeft]}>{userName}</h5>
              <div className='home-about-activity-timestamp' style={[styles.activityTime, formDescription]}>{when}</div>
            </div>
            {notes}
          </div>
        </div>
      </div>
    );
  }

  renderSummary() {
    const {entity, entityType} = this.props;
    const component = rightContext[entityType];
    if (entity && component) {
      return React.createElement(component, {entity, entityType});
    }
  }

  render() {
    return (
      <div className='pull-out-right-tree'>
        <RightTreeTabs renderHideButton={this.renderHideButton}
          toggleVisibility={this.props.toggleVisibility}>
          <RightTreeTab tabId='Settings' title='Settings'>
            {this.renderSummary()}
          </RightTreeTab>
        </RightTreeTabs>
      </div>
    );
  }
}

const styles = {
  listItem: {
    overflow: 'hidden',
    padding: '10px 0',
    borderBottom: '1px solid rgba(0,0,0,.05)'
  },
  topInfo: {
    overflow: 'hidden',
    marginBottom: 5,
    width: 177
  },
  floatLeft: {
    float: 'left'
  },
  activityTime: {
    float: 'right'
  },
  notes: {
    clear: 'right'
  },
  avatar: {
    width: 36,
    height: 36,
    marginRight: 10,
    float: 'left'
  },
  avatarImg: {
    width: 36,
    height: 36
  }
};

export default PullOutRightTree;
