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
import { connect } from 'react-redux';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { Link } from 'react-router';

import { MAP, TEXT, LIST } from 'constants/DataTypes';

import exploreUtils from 'utils/explore/exploreUtils';
import FontIcon from 'components/Icon/FontIcon';
import { h5 } from 'uiTheme/radium/typography';
import { PALE_NAVY, GREY, BLACK } from 'uiTheme/radium/colors';

export const EXTRACT_TAB = 'extract';
export const SPLIT_TAB = 'split';
export const REPLACE_TAB = 'replace';
export const KEEP_ONLY_TAB = 'keeponly';
export const EXCLUDE_TAB = 'exclude';

@PureRender
@Radium
export class TransformHeader extends Component {
  static propTypes = {
    closeIconHandler: PropTypes.func.isRequired,
    closeIcon: PropTypes.bool,
    separator: PropTypes.string,
    text: PropTypes.string,
    tryToRefreshTableData: PropTypes.func,

    // connected
    location: PropTypes.object,
    transform: PropTypes.instanceOf(Immutable.Map)
  };

  constructor(props) {
    super(props);

    this.tabs = [
      {
        name: 'Replace',
        id: REPLACE_TAB
      },
      {
        name: 'Extract',
        id: EXTRACT_TAB
      },
      {
        name: 'Split',
        id: SPLIT_TAB
      },
      {
        name: 'Keep Only',
        id: KEEP_ONLY_TAB
      },
      {
        name: 'Exclude',
        id: EXCLUDE_TAB
      }
    ];
  }

  getCloseIcon() {
    const handler = this.props.closeIconHandler;
    const icon = this.props.closeIcon
      ? <FontIcon type={'XBig'} theme={styles.iconTheme} onClick={handler}/>
      : null;
    return icon;
  }

  getSeparator() {
    return this.props.separator ? this.props.separator : ': ';
  }

  isActiveTab(id) {
    return id === this.props.transform.get('transformType');
  }

  isTabEnabled(id) {
    const columnType = this.props.transform.get('columnType');

    if (columnType === LIST || columnType === MAP) {
      return id === EXTRACT_TAB;
    }

    return columnType !== TEXT
      ? id !== EXTRACT_TAB && id !== SPLIT_TAB
      : true;
  }

  renderTabs() {
    const { location, transform } = this.props;

    return this.tabs.map(tab => {
      const isActive = this.isActiveTab(tab.id);
      const isEnabled = this.isTabEnabled(tab.id);
      const isHovered = isEnabled && Radium.getState(this.state, tab.id, ':hover');

      const lineThatShowThatActive = (isHovered || this.isActiveTab(tab.id))
        ? <div style={styles.activeTab}/>
        : null;

      const linkStyle = {
        ...styles.tab,
        color: isEnabled ? BLACK : GREY
      };

      if (!isEnabled) {
        return (
          <h5>
            <span className='transform-tab' style={[linkStyle, { cursor: 'default' }]} key={tab.id}>
              {tab.name}
            </span>
          </h5>
        );
      }

      return (
        <h5>
          <Link className={'transform-tab' + (isActive ? ' active-transform-tab' : '')}
            style={linkStyle}
            key={tab.id}
            to={{
              ...location,
              state: transform.set('transformType', tab.id).toJS()
            }}>
            {tab.name}
            {lineThatShowThatActive}
          </Link>
        </h5>
      );
    });
  }

  render() {
    return (
      <div>
        <div className='raw-wizard-header' style={[styles.base]}>
          <div style={[styles.content]}>
            {this.props.text}{this.getSeparator()}
            {this.renderTabs()}
          </div>
          {this.getCloseIcon()}
        </div>
      </div>
    );
  }
}


function mapStateToProps(state) {
  const location = state.routing.locationBeforeTransitions;
  return {
    location,
    transform: exploreUtils.getTransformState(location)
  };
}

export default connect(mapStateToProps)(TransformHeader);

const styles = {
  'base': {
    display: 'flex',
    height: 38,
    justifyContent: 'space-between',
    backgroundColor: PALE_NAVY
  },
  tab: {
    display: 'flex',
    height: 37,
    position: 'relative',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '0 10px',
    color: '#000000',
    cursor: 'pointer'
  },
  activeTab: {
    backgroundColor: '#77818F',
    position: 'absolute',
    bottom: -1,
    width: 'calc(100% - 10px)',
    height: 3,
    left: 5
  },
  'content': {
    'display': 'flex',
    'marginLeft': 0,
    'alignItems': 'center',
    'fontSize': 15,
    'fontWeight': 600
  },
  'iconTheme': {
    'Icon': {
      float: 'right',
      margin: '5px 5px 0 0',
      position: 'relative',
      width: 24,
      height: 24,
      'fontSize': 18,
      'cursor': 'pointer'
    }
  }
};
