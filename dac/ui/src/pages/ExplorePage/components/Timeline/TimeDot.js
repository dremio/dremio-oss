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
import { Component } from 'react';
import ReactDOM from 'react-dom';
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { Overlay } from 'react-overlays';
import { Link } from 'react-router';

// import FontIcon from 'components/Icon/FontIcon';
import { EXPLORE_PROGRESS_STATES } from 'constants/Constants';
import Spinner from 'components/Spinner';

import { HISTORY_ITEM_COLOR, ORANGE, NAVY } from 'uiTheme/radium/colors';
import { h5White /*, bodySmallWhite*/ } from 'uiTheme/radium/typography';
import { TIME_DOT_DIAMETER } from 'uiTheme/radium/sizes';
import EllipsedText from 'components/EllipsedText';

@Radium
export default class TimeDot extends Component {
  static propTypes = {
    historyItem: PropTypes.instanceOf(Immutable.Map).isRequired,
    tipVersion: PropTypes.string.isRequired,
    activeVersion: PropTypes.string.isRequired,
    hideDelay: PropTypes.number,
    location: PropTypes.object.isRequired,
    datasetPathname: PropTypes.string.isRequired
  };

  static defaultProps = {
    hideDelay: 50,
    historyItem: Immutable.Map()
  };

  constructor(props) {
    super(props);

    this.canClose = true;

    this.state = {
      open: false
    };

    this.popoverHash = {
      'DONE': this.renderCompletedContent,
      'COMPLETED': this.renderCompletedContent,
      'CANCELED': this.renderCompletedContent,
      'ENQUEUED': this.renderCompletedContent,
      'STARTED': this.renderProgressContent,
      'RUNNING': this.renderProgressContent,
      'NOT_SUBMITTED': this.renderCompletedContent,
      'NOT STARTED': this.renderProgressContent
    };
  }

  canClose = false;

  getLinkLocation() {
    const { tipVersion, historyItem, datasetPathname, location } = this.props;
    const query = location && location.query || {};
    if (query.version === historyItem.get('datasetVersion')) {
      return null;
    }
    return {
      pathname: datasetPathname,
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

  handleMouseEnter = () => {
    if (this.hideTimeout) {
      clearTimeout(this.hideTimeout);
    }
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

  renderProgressContent = () => {
    return (
      <div>
        {this.renderCompletedContent(false)}
        <div style={[styles.progress]}>
          <Spinner containerStyle={styles.progressIcon.Container} iconStyle={styles.progressIcon.Icon}/>
        </div>
        {/*DX-8023 <div style={[styles.popoverFooterWrap]}>
          <span style={[bodySmallWhite, styles.cancel]}>
            <FontIcon type='CanceledGray' theme={styles.cancelIcon}/>
            <span style={styles.cancelText}>Cancel Job</span>
          </span>
        </div>*/}
      </div>
    );
  }

  renderContent() {
    const { historyItem, activeVersion } = this.props;

    const activeStyle = activeVersion === historyItem.get('datasetVersion')
      ? styles.activeStyle
      : {};
    const circle = EXPLORE_PROGRESS_STATES.indexOf(historyItem.get('state')) !== -1
      ? <Spinner containerStyle={styles.spinner.Container} iconStyle={styles.spinner.Icon}/>
      : <div style={[styles.circle, activeStyle]}/>;

    const popoverContent = this.popoverHash[historyItem.get('state')] &&
        this.popoverHash[historyItem.get('state')]();
    return <div>
      <div
        data-qa='time-dot-target'
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
        style={styles.wrapStyle}
        ref='target'>
        {circle}
      </div>
      <Overlay
        show={this.state.open}
        container={document.body}
        placement='left'
        target={() => ReactDOM.findDOMNode(this.refs.target)}>
        <div style={styles.popoverWrap}>
          <div style={styles.triangle}/>
          <div
            data-qa='time-dot-popover'
            style={styles.popover}
            onMouseEnter={this.handleMouseEnter}
            onMouseLeave={this.handleMouseLeave}>
            {popoverContent}
          </div>
        </div>
      </Overlay>
    </div>;
  }

  render() {
    const linkLocation = this.getLinkLocation();
    if (linkLocation) {
      return <Link
        className='time-dot'
        style={styles.base}
        to={linkLocation}
      >
        {this.renderContent()}
      </Link>;
    }
    return <span className='time-dot' style={styles.base}>{this.renderContent()}</span>;
  }
}

const styles = {
  base: {
    flexShrink: 0,
    width: 30,
    height: TIME_DOT_DIAMETER,
    marginTop: 10,
    position: 'relative',
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center'
  },
  wrapStyle: {
    minWidth: 30,
    minHeight: 15,
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center'
  },
  textDesc: {
    width: 170,
    textDecoration: 'none',
    flexGrow: 1,
    paddingRight: 10
  },
  progress: {
    display: 'flex',
    alignItems: 'center',
    height: 20
  },
  cancelText: {
    position: 'relative',
    top: 1
  },
  cancelIcon: {
    Container: {
      display: 'inline-flex',
      alignItems: 'center',
      left: -4,
      position: 'relative'
    },
    Icon: {
      width: 24,
      height: 24
    }
  },
  progressIcon: {
    Container: {
      display: 'inline-flex',
      alignItems: 'center',
      left: -5,
      position: 'relative'
    },
    Icon: {
      width: 25,
      height: 25
    }
  },
  popoverTitle: {
    display: 'flex',
    justifyContent: 'space-between'
  },
  cancel: {
    display: 'inline-flex',
    alignItems: 'center',
    cursor: 'pointer'
  },
  spinner: {
    Icon: {
      width: 15,
      height: 15,
      position: 'relative',
      cursor: 'pointer',
      borderRadius: 20,
      backgroundSize: '23px 23px',
      backgroundPosition: 'center'
    },
    Container: {
      width: 30,
      height: 30,
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center'
    }
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
  popoverWrap: {
    minWidth: 345,
    maxWidth: 350,
    backgroundColor: NAVY,
    overflow: 'visible',
    boxShadow: '2px 2px 5px 0px rgba(0,0,0,0.05)',
    borderRadius: 2,
    position: 'absolute'
  },
  triangle: {
    width: 0,
    height: 0,
    borderStyle: 'solid',
    borderWidth: '5px 0 5px 6px',
    borderColor: `transparent transparent transparent ${NAVY}`,
    right: -5,
    top: 33,
    position: 'absolute',
    backgroundColor: 'transparent'
  },
  activeStyle: {
    backgroundColor: ORANGE
  },
  popoverFooterWrap: {
    display: 'flex',
    alignItems: 'center',
    height: 23,
    justifyContent: 'space-between'
  },
  popover: {
    padding: 10,
    height: 76,
    width: 344
  }
};
