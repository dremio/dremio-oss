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
import { connect } from 'react-redux';
import { injectIntl } from 'react-intl';
import { getExploreState } from '@app/selectors/explore';

import Art from 'components/Art';

import { RECOMMENDED_JOIN, CUSTOM_JOIN } from 'constants/explorePage/joinTabs';
import { setJoinTab, clearJoinDataset } from 'actions/explore/join';

import { PALE_NAVY } from 'uiTheme/radium/colors';

@injectIntl
@Radium
export class JoinHeader extends Component {
  static propTypes = {
    viewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    hasRecommendations: PropTypes.bool,
    isRecommendationsInProgress: PropTypes.bool,
    closeIconHandler: PropTypes.func.isRequired,
    closeIcon: PropTypes.bool,
    separator: PropTypes.string,
    text: PropTypes.string,
    setJoinTab: PropTypes.func,
    joinTab: PropTypes.string,
    clearJoinDataset: PropTypes.func,
    intl: PropTypes.object.isRequired
  };

  static contextTypes = {
    router: PropTypes.object
  };

  constructor(props) {
    super(props);
    this.tabs = [
      {
        name: 'Recommended Join',
        id: RECOMMENDED_JOIN
      },
      {
        name: 'Custom Join',
        id: CUSTOM_JOIN
      }
    ];
  }

  getCloseIcon() {
    const handler = this.props.closeIconHandler;
    const icon = this.props.closeIcon
      ? <Art
        src={'XBig.svg'}
        alt={this.props.intl.formatMessage({ id: 'Common.Close' })}
        onClick={handler}
        style={styles.icon} />
      : null;
    return icon;
  }

  getSeparator() {
    const {separator} = this.props;
    return separator
      ? separator
      : ': ';
  }

  setActiveTab(id) {
    this.props.clearJoinDataset();
    this.props.setJoinTab(id);
  }

  isActiveTab(id) {
    return id === this.props.joinTab;
  }

  renderTabs() {
    return this.tabs.map(tab => {
      const { hasRecommendations, viewState } = this.props;
      const isHovered = Radium.getState(this.state, tab.id, ':hover');
      const disabledStyle = (tab.id === RECOMMENDED_JOIN && !hasRecommendations) || viewState.get('isInProgress')
       ? styles.disabled
       : {};
      const activeTabStyle = (isHovered || this.isActiveTab(tab.id)) && !viewState.get('isInProgress')
        ? styles.activeTab
        : {};

      return (
        <h5
          className='transform-tab'
          data-qa={tab.name}
          style={[styles.tab, {color: '#000000'}, disabledStyle, activeTabStyle]}
          key={tab.id}
          onClick={this.setActiveTab.bind(this, tab.id)}>
          <span>{tab.name}</span>
        </h5>
      );
    });
  }

  render() {
    return (
      <div className='raw-wizard-header' style={[styles.base]}>
        <div style={[styles.content]}>
          {this.props.text}{this.getSeparator()}
          {this.renderTabs()}
        </div>
        {this.getCloseIcon()}
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    height: 38,
    justifyContent: 'space-between',
    backgroundColor: PALE_NAVY
  },
  tab: {
    display: 'flex',
    height: 37,
    marginLeft: -5,
    position: 'relative',
    justifyContent: 'center',
    padding: '10px 20px',
    cursor: 'pointer',
    ':hover': {
      color: '#000000'
    }
  },
  activeTab: {
    borderBottom: '3px solid #77818F'
  },
  'content': {
    'display': 'flex',
    'marginLeft': 0,
    'alignItems': 'center',
    'fontSize': 15,
    'fontWeight': 600
  },
  icon: {
    float: 'right',
    margin: '8px 10px 0 0',
    position: 'relative',
    width: 24,
    height: 24,
    fontSize: 18,
    cursor: 'pointer'
  },
  disabled: {
    color: 'gray',
    pointerEvents: 'none'
  }
};

function mapStateToProps(state, ownProps) {
  return {
    joinTab: getExploreState(state).join.get('joinTab')
  };
}
export default connect(mapStateToProps, {setJoinTab, clearJoinDataset})(JoinHeader);
