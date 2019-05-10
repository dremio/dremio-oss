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
import { Component, Fragment, createRef } from 'react';
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { Link } from 'react-router';

import { Tooltip } from '@app/components/Tooltip';
import { HISTORY_ITEM_COLOR, ORANGE } from 'uiTheme/radium/colors';
import { h5White /*, bodySmallWhite*/ } from 'uiTheme/radium/typography';
import { TIME_DOT_DIAMETER } from 'uiTheme/radium/sizes';
import EllipsedText from 'components/EllipsedText';
@Radium
export class TimeDot extends Component {
  static propTypes = {
    historyItem: PropTypes.instanceOf(Immutable.Map).isRequired,
    tipVersion: PropTypes.string.isRequired,
    activeVersion: PropTypes.string.isRequired,
    hideDelay: PropTypes.number,
    location: PropTypes.object.isRequired
  };

  static defaultProps = {
    hideDelay: 50,
    historyItem: Immutable.Map()
  };

  state = {
    open: false
  };

  targetRef = createRef();

  getTooltipTarget = () => this.state.open ? this.targetRef.current : null;

  getLinkLocation() {
    const { tipVersion, historyItem, location } = this.props;
    const query = location && location.query || {};
    if (query.version === historyItem.get('datasetVersion')) {
      return null;
    }
    return {
      pathname: location.pathname,
      query: {
        ...((query.mode ? {mode: query.mode} : {})),
        tipVersion,
        version: historyItem.get('datasetVersion')
      }
    };
  }

  handleMouseLeave = () => {
    this.hideTimeout = setTimeout(() => {
      this.setState({
        open: false
      });
    }, this.props.hideDelay);
  }

  componentWillUnmount() {
    clearTimeout(this.hideTimeout);
  }

  handleMouseEnter = (e) => {
    clearTimeout(this.hideTimeout);
    this.setState({
      open: true
    });
  }

  renderCompletedContent = (wrap = true) => {
    const { historyItem } = this.props;
    const owner = historyItem.get('owner');
    const node = <div style={[styles.popoverTitle]}>
      <EllipsedText style={{...h5White, ...styles.textDesc}} text={historyItem.get('transformDescription')}/>
      <span style={[h5White]}>{owner ? owner : ''}</span>
    </div>;
    return wrap ? <div>{node}</div> : node;
  }

  renderContent() {
    const { historyItem, activeVersion } = this.props;

    const activeStyle = activeVersion === historyItem.get('datasetVersion')
      ? styles.activeStyle
      : {};
    // Talked to Jeff. It is ok to not show a spinner for loading history items. May be we will return
    // this functionality later. The issue with this is that we cancel job listener if we navigate to
    // a new version. If a user alter and preview query to quickly, some history items could stuck
    // in progress mode
    const circle = <div style={[styles.circle, activeStyle]}/>;
    return <div>
      <div
        key='dot'
        data-qa='time-dot-target'
      >
        {circle}
      </div>
    </div>;
  }

  render() {
    const linkLocation = this.getLinkLocation();
    const commonProps = {
      onMouseEnter: this.handleMouseEnter,
      onMouseLeave: this.handleMouseLeave,
      ref: this.targetRef
    };
    const dotEl = linkLocation ? (
      <Link
        className='time-dot'
        style={styles.base}
        to={linkLocation}
        {...commonProps}
      >
        {this.renderContent()}
      </Link>
    ) : (
      <span className='time-dot' style={styles.base}
        {...commonProps}
      >
        {this.renderContent()}
      </span>
    );

    return <Fragment>
      {dotEl}
      <Tooltip
        container={document.body}
        placement='left'
        target={this.getTooltipTarget}
        tooltipInnerStyle={styles.popover}
        dataQa='time-dot-popover'
      >
        {this.renderCompletedContent()}
      </Tooltip>
    </Fragment>;
  }
}

export default TimeDot;

const styles = {
  base: {
    flexShrink: 0,
    width: 30,
    height: TIME_DOT_DIAMETER,
    marginTop: 10,
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center'
  },
  textDesc: {
    width: 170,
    textDecoration: 'none',
    flexGrow: 1,
    paddingRight: 10,
    whiteSpace: 'inherit'
  },
  popoverTitle: {
    display: 'flex',
    justifyContent: 'space-between'
  },
  pointer: {
    cursor: 'pointer'
  },
  circle: {
    backgroundColor: HISTORY_ITEM_COLOR,
    width: TIME_DOT_DIAMETER,
    height: TIME_DOT_DIAMETER,
    borderRadius: TIME_DOT_DIAMETER / 2
  },
  activeStyle: {
    backgroundColor: ORANGE
  },
  popover: {
    minHeight: 46,
    maxHeight: 298, // to cut last visible line in half in case of overflow
    overflow: 'hidden',
    width: 344
  }
};
