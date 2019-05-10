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
import { connect } from 'react-redux';
import Immutable from 'immutable';
import Radium from 'radium';

import PropTypes from 'prop-types';
import ImmutablePropTypes from 'react-immutable-proptypes';

import Spinner from 'components/Spinner';
import Message from 'components/Message';

import { dismissViewStateError } from 'actions/resources';

import { overlay } from 'uiTheme/radium/overlay';
import classNames from 'classnames';

const TIME_TP_WAIT_BEFORE_SPINNER = 500;

export const viewStatePropType = ImmutablePropTypes.contains({
  isInProgress: PropTypes.bool,
  isFailed: PropTypes.bool,
  isWarning: PropTypes.bool,
  error: PropTypes.shape({
    message: PropTypes.node
    //details,
    //id
    //dismissed: false
  })
});

@Radium
export class ViewStateWrapper extends Component {
  static propTypes = {
    viewState: viewStatePropType,
    children: PropTypes.node,
    hideSpinner: PropTypes.bool,
    spinnerDelay: PropTypes.number,
    hideChildrenWhenInProgress: PropTypes.bool,
    hideChildrenWhenFailed: PropTypes.bool,
    style: PropTypes.object,
    messageStyle: PropTypes.object, // styles that are applied to a error message
    showMessage: PropTypes.bool,
    spinnerStyle: PropTypes.object,
    progressMessage: PropTypes.string,
    dismissViewStateError: PropTypes.func,
    onDismissError: PropTypes.func,
    messageIsDismissable: PropTypes.bool,
    className: PropTypes.string,
    // is used only for ExploreTable to not bock column headers on loading
    overlayStyle: PropTypes.object,
    dataQa: PropTypes.string
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

  componentWillUnmount() {
    clearTimeout(this.timer);
  }

  componentWillReceiveProps(nextProps) {
    const inProgress = nextProps.viewState.get('isInProgress');
    if (inProgress) {
      if (inProgress !== this.props.viewState.get('isInProgress')) {
        this.checkTimer();
      }
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
      onDismissError,
      messageStyle,
      overlayStyle,
      dataQa
    } = this.props;
    const resultOverlayStyle = { ...overlay, ...overlayStyle };
    const commonProps = {
      style: resultOverlayStyle,
      'data-qa': dataQa,
      className: 'view-state-wrapper-overlay'
    };
    if (viewState.get('isInProgress') && !this.props.hideSpinner) {
      if (this.state.shouldWeSeeSpinner || hideChildrenWhenInProgress) {
        return <div {...commonProps}>
          <div style={spinnerStyle}>
            <Spinner/>
            {progressMessage}
          </div>
        </div>;
      }
      return <div {...commonProps} />;
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
        inFlow={hideChildrenWhenFailed}
        style={messageStyle}
      />;
    }
  }

  render() {
    const { style, className } = this.props;

    return (
      <div
        className={classNames(['view-state-wrapper', className])}
        style={[styles.base, style]}>
        {this.renderChildren()}
        {this.renderStatus()}
      </div>
    );
  }
}

export default connect(null, { dismissViewStateError })(ViewStateWrapper);

/**
 * Returns a first truthy value from {@see fieldName} field of {@see immutableMaps} list
 * @param {string} fieldName - a field name to search for a truthy value
 * @param  {...Immutable.Map} immutableMaps - a list of immutable maps
 */
// export for testing
export const findFirstTruthyValue = (fieldName, ...immutableMaps) => {
  return immutableMaps.reduce((res, currMap) => {
    return res || currMap.get(fieldName);
  }, undefined);
};
export const mergeViewStates = (...viewStates) => {
  return Immutable.fromJS(['isInProgress', 'isFailed', 'isWarning', 'error'].reduce((result, fieldName) => {
    result[fieldName] = findFirstTruthyValue(fieldName, ...viewStates);
    return result;
  }, {}));
};

const styles = {
  base: {
    height: '100%',
    position: 'relative'
  }
};
