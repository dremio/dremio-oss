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
import ReactDOM from 'react-dom';
import Radium from 'radium';
import PropTypes from 'prop-types';
import RedBox, { RedBoxError } from 'redbox-react';
import { hideAppError } from '@app/actions/prodError';
import { getAppError } from '@app/reducers';

import fileABug from '../utils/fileABug';
import FontIcon from '../components/Icon/FontIcon';

@Radium
export class CustomRedBox extends RedBox {
  static propTypes = {
    onDismiss: PropTypes.func.isRequired, // note: React provides its RSOD in dev mode, so we can't actually dissmiss there
    error: PropTypes.instanceOf(Error).isRequired
  }

  handleFileABugClick = () => {
    fileABug(this.props.error);
  }

  renderRedBoxError() {
    ReactDOM.render(<div>
      <RedBoxError rel='' error={this.props.error} style={{message:{whiteSpace: 'pre'}}}/>
      <div style={styles.topRight}>
        <FontIcon type='XBigWhite' onClick={this.props.onDismiss} style={styles.dismissButton}/>
      </div>
      <div style={styles.bottomRight}>
        <button style={styles.fileBugButton} onClick={this.handleFileABugClick}>{la('File a Bug')}</button>
      </div>
    </div>, this.el);
  }
}


const mapStateToProps = state => ({
  error: getAppError(state)
});

const mapDispatchToProps = ({
  onDismiss: hideAppError
});

export class DevErrorView extends Component {

  static propTypes = {
    error: PropTypes.object,
    onDismiss: PropTypes.func.isRequired
  }

  render() {
    if (this.props.error) {
      return <CustomRedBox error={this.props.error} onDismiss={this.props.onDismiss}/>;
    }
    return null;
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(DevErrorView);

const styles = {
  topRight: {
    position: 'absolute',
    top: 0,
    right: 0,
    zIndex: 2147483647,
    padding: 20
  },
  bottomRight: {
    position: 'fixed',
    bottom: 0,
    right: 0,
    zIndex: 2147483647,
    padding: 20
  },
  fileBugButton: {
    fontSize: '20px',
    padding: 20,
    borderRadius: 10,
    background: '#eee',
    marginRight: 20
  },
  dismissButton: {
    cursor: 'pointer',
    transform: 'scale(1.5, 1.5)',
    ':active': {
      transform: 'scale(1.2, 1.2)'
    }
  }
};
