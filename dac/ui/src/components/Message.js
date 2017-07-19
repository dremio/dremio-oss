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
import { Component, PropTypes } from 'react';
import invariant from 'invariant';
import Immutable from 'immutable';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';
import { Link } from 'react-router';

import FontIcon from 'components/Icon/FontIcon';
import { body, fixedWidthDefault } from 'uiTheme/radium/typography';

import jobsUtils from 'utils/jobsUtils';

export const RENDER_NO_DETAILS = Symbol('RENDER_NO_DETAILS');

@Radium
@pureRender
export default class Message extends Component {

  // must be a superset of the notification system `level` options
  static MESSAGE_TYPES = ['info', 'success', 'warning', 'error'];

  static defaultProps = {
    messageType: 'info',
    isDismissable: true,
    message: Immutable.Map()
  };

  static propTypes = {
    message: PropTypes.oneOfType([
      PropTypes.node,
      PropTypes.instanceOf(Immutable.Map) // errors with extra details
    ]),
    messageType: PropTypes.string,
    dismissed: PropTypes.bool,
    onDismiss: PropTypes.func,
    style: PropTypes.object,
    messageId: PropTypes.string,
    detailsStyle: PropTypes.object,
    isDismissable: PropTypes.bool
  };

  constructor(props) {
    super(props);
    const messageText = `messageType must be one of ${JSON.stringify(Message.MESSAGE_TYPES)}`;
    invariant(Message.MESSAGE_TYPES.indexOf(this.props.messageType) !== -1, messageText);
  }

  state = {
    dismissed: false,
    showMore: false
  };

  componentWillReceiveProps(nextProps) {
    if (this.props.messageId !== nextProps.messageId) {
      this.setState({ dismissed: false, showMore: false });
    }
  }

  onDismiss = () => {
    if (this.props.onDismiss) {
      if (this.props.onDismiss() === false) {
        return;
      }
    }
    this.setState({ dismissed: true });
  }

  prevent(e) {
    e.preventDefault();
    e.stopPropagation();
  }

  showMoreToggle = () => {
    this.setState((prevState) => {
      return {showMore: !prevState.showMore};
    });
  }

  renderErrorMessageText() {
    let messageText = this.props.message;
    if (messageText instanceof Immutable.Map) {
      // note: #errorMessage is legacy
      messageText = this.renderMessageForCode() || messageText.get('message') || messageText.get('errorMessage');
    }
    return messageText;
  }

  renderIcon(messageType) {
    switch (messageType) {
    case 'error':
      return <FontIcon type='Error' style={styles.icon}/>;
    case 'warning':
      return <FontIcon type='Warning' style={styles.icon}/>;
    case 'info':
      return <FontIcon type='InfoCircleNoninteractive' style={styles.icon}/>;
    case 'success':
      return <FontIcon type='OK' style={styles.icon}/>;
    default:
      return null;
    }
  }

  renderDetails() {
    const message = this.props.message;
    if (!(message instanceof Immutable.Map)) {
      return;
    }

    let details = [];

    // If there's a messageForCode, show the #message in the details.
    if (this.renderMessageForCode()) {
      details.push(message.get('message'));
    }

    details.push(message.get('moreInfo'));

    const codeDetails = this.renderDetailsForCode();
    if (codeDetails === RENDER_NO_DETAILS) {
      return;
    }
    details.push(codeDetails);

    let stackTrace = message.get('stackTrace');
    if (stackTrace) {
      if (stackTrace.join) { // handle arrays (todo: why not always use strings? does anything send arrays anymore?)
        stackTrace = stackTrace.join('\n');
      }
      details.push(<div style={[styles.stackTrace, fixedWidthDefault]}>{stackTrace}</div>);
    }

    details = details.filter(Boolean);

    if (!details.length) return;

    const separatedStyle = {paddingTop: 10, marginTop: 10, borderTop: '1px solid hsla(0, 0%, 0%, 0.2)'};
    details = details.map((e, i) => <div style={i ? separatedStyle : {}}>{e}</div>);

    return <div children={details} style={[styles.details, body, this.props.detailsStyle]} />;
  }

  renderMessageForCode() { // this fcn assumes we have already checked that we have an ImmutableMap
    const message = this.props.message;
    const code = message.get('code');
    if (!code) return;

    switch (code) {
    case 'PIPELINE_FAILURE':
      return <span>{la('There was an error in the Reflection pipeline.')}</span>;
    case 'MATERIALIZATION_FAILURE': {
      const messageForCode = la('There was an error building a Reflection');
      // todo: #materializationFailure should become generic #details
      const url = jobsUtils.navigationURLForJobId(message.getIn(['materializationFailure', 'jobId']));
      return <span>
        {messageForCode} (<Link to={url}>{la('show job')}</Link>).
      </span>; // todo: better loc
    }
    case 'DROP_FAILURE':
      return <span>{la('There was an error dropping a Reflection.')}</span>;
    default:
      break;
    }
  }

  renderDetailsForCode() { // this fcn assumes we have already checked that we have an ImmutableMap
    const message = this.props.message;
    const code = message.get('code');
    if (!code) return;

    switch (code) {
    case 'MATERIALIZATION_FAILURE':
      return RENDER_NO_DETAILS; // job link has all the needed info
    default:
      break;
    }
  }

  renderShowMore() {
    return <span
      onTouchTap={this.showMoreToggle}
      onMouseUp={this.prevent}
      style={styles.showMoreLink}>

      {this.state.showMore ? la('show less') : la('show more')}
    </span>;
  }

  render() {
    const { messageType, style } = this.props;

    if (this.props.dismissed || this.props.dismissed === undefined && this.state.dismissed) {
      return null;
    }

    const details = this.renderDetails();

    return (
      <div className={`message ${messageType}`} style={[styles.wrap, style]}>
        <div style={[styles.base, styles[messageType], body]} ref='messagePanel'>
          {this.renderIcon(messageType)}
          <span className='message-content' style={styles.messageText} onMouseUp={this.prevent}>
            {this.renderErrorMessageText()}
          </span>
          {details && this.renderShowMore()}
          {this.props.isDismissable
            && <div style={styles.close}>
              <FontIcon type='XSmall' onClick={this.onDismiss} style={styles.dismissBtn}/>
            </div>
          }
        </div>
        {this.state.showMore && details}
      </div>
    );
  }
}

const styles = {
  wrap: {
    width: '100%'
  },
  base: {
    display: 'flex',
    alignItems: 'center',
    flexWrap: 'nowrap',
    padding: '5px',
    width: '100%',
    borderBottom: '1px rgba(0,0,0,0.05) solid',
    borderRadius: 1,
    'MozUserSelect': 'text',
    'WebkitUserSelect': 'text',
    'UserSelect': 'text',
    maxHeight: 300,
    position: 'relative',
    overflowY: 'auto',
    zIndex: 3 // needs to be above disabled table overlay
  },
  messageText: {
    flexGrow: 1
  },
  stackTrace: {
    whiteSpace: 'pre',
    maxWidth: 400
  },
  showMoreLink: {
    cursor: 'pointer',
    marginRight: 30,
    marginLeft: 10,
    textDecoration: 'underline',
    flexShrink: 0
  },
  details: {
    padding: '10px 30px',
    maxHeight: 200,
    width: '100%',
    overflowX: 'auto',
    backgroundColor: '#FEEDED'
  },
  close: {
    justifyContent: 'center',
    display: 'flex',
    alignItems: 'center',
    height: 24,
    width: 24,
    padding: 5
  },
  icon: {
    marginRight: 5,
    height: 24
  },
  msgWrap: {
    lineHeight: '24px',
    wordWrap: 'break-word',
    display: 'inline-block',
    width: '100%'
  },
  info: {
    'backgroundColor': '#E4F2F7'
  },

  success: {
    'backgroundColor': '#EEF7E2'
  },

  warning: {
    'backgroundColor': '#FFF3E8'
  },

  error: {
    'backgroundColor': '#FCD9D9'
  },

  'dismissBtn': {
    cursor: 'pointer'
  }
};
