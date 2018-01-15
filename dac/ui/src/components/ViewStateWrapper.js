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
import { connect } from 'react-redux';
import Immutable from 'immutable';
import Radium from 'radium';

import PropTypes from 'prop-types';

import Spinner from 'components/Spinner';
import Message from 'components/Message';

import { dismissViewStateError } from 'actions/resources';

import { overlay } from 'uiTheme/radium/overlay';

const TIME_TP_WAIT_BEFORE_SPINNER = 500;

@Radium
export class ViewStateWrapper extends Component {
  static propTypes = {
    viewState: PropTypes.instanceOf(Immutable.Map), // todo: might it be easier to accept a view_id?
    children: PropTypes.node,
    hideSpinner: PropTypes.bool,
    spinnerDelay: PropTypes.number,
    hideChildrenWhenInProgress: PropTypes.bool,
    hideChildrenWhenFailed: PropTypes.bool,
    style: PropTypes.object,
    showMessage: PropTypes.bool,
    spinnerStyle: PropTypes.object,
    progressMessage: PropTypes.string,
    dismissViewStateError: PropTypes.func,
    onDismissError: PropTypes.func,
    messageIsDismissable: PropTypes.bool
  };

  static defaultProps = {
    showMessage: true,
    spinnerDelay: TIME_TP_WAIT_BEFORE_SPINNER,
    viewState: Immutable.fromJS({}),
    hideChildrenWhenFailed: true,
    messageIsDismissable: true
  };

  constructor(props) {
    super(props);
    this.state = {
      shouldWeSeeSpinner: false
    };
    if (props.viewState.get('isInProgress')) {
      this.checkTimer();
    }
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.viewState.get('isInProgress')) {
      this.checkTimer();
    } else {
      clearTimeout(this.timer);
      this.setState({
        shouldWeSeeSpinner: false
      });
    }
  }

  checkTimer() {
    clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.setState({
        shouldWeSeeSpinner: true
      });
    }, this.props.spinnerDelay);
  }

  renderChildren() {
    const {children, viewState, hideChildrenWhenInProgress, hideChildrenWhenFailed} = this.props;
    if ((viewState.get('isAutoPeekFailed') || !viewState.get('isFailed') || !hideChildrenWhenFailed)
      && (!viewState.get('isInProgress') || !hideChildrenWhenInProgress)) {
      return children;
    }
  }

  renderStatus() {
    const {
      viewState,
      spinnerStyle,
      hideChildrenWhenInProgress,
      hideChildrenWhenFailed,
      showMessage,
      progressMessage,
      onDismissError
    } = this.props;
    if (viewState.get('isInProgress') && !this.props.hideSpinner) {
      if (this.state.shouldWeSeeSpinner || hideChildrenWhenInProgress) {
        return <div style={overlay} className='view-state-wrapper-overlay'>
          <div style={spinnerStyle}>
            <Spinner/>
            {progressMessage}
          </div>
        </div>;
      }
      return <div style={overlay} className='view-state-wrapper-overlay'/>;
    }

    const handleDismiss = () => {
      this.props.dismissViewStateError(viewState.get('viewId'));
      onDismissError && onDismissError();
    };

    if ((viewState.get('isFailed') || viewState.get('isWarning')) && showMessage) {
      const messageType = viewState.get('isWarning') ? 'warning' : 'error';
      const message = viewState.getIn(['error', 'message']);
      return <Message
        onDismiss={handleDismiss}
        dismissed={viewState.getIn(['error', 'dismissed'])}
        messageId={viewState.getIn(['error', 'id'])}
        message={message}
        messageType={messageType}
        isDismissable={this.props.messageIsDismissable}
        style={!hideChildrenWhenFailed ? styles.messageOverChildren : undefined}
      />;
    }
  }

  render() {
    const { style } = this.props;

    return (
      <div className='view-state-wrapper' style={[styles.base, style]}>
        {this.renderChildren()}
        {this.renderStatus()}
      </div>
    );
  }
}

export default connect(null, { dismissViewStateError })(ViewStateWrapper);

const styles = {
  base: {
    height: '100%',
    position: 'relative'
  },
  messageOverChildren: {
    position: 'absolute',
    top: 0,
    width: '100%'
  }
};
