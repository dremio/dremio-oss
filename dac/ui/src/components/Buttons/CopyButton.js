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
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import CopyToClipboard from 'react-copy-to-clipboard';
import FontIcon from 'components/Icon/FontIcon';
import { addNotification } from 'actions/notification';
import './CopyButton.less';

export class CopyButton extends Component {
  static propTypes = {
    text: PropTypes.string.isRequired,
    title: PropTypes.string,
    style: PropTypes.object,
    addNotification: PropTypes.func.isRequired
  }

  static defaultProps = {
    title: 'Copy' // TODO: loc
  }

  handleCopy = () => {
    // todo: loc
    const message = <span>Copied <i>{this.props.text}</i>.</span>;
    this.props.addNotification(message, 'success', 2);
  }

  render() {
    return (
      <CopyToClipboard text={this.props.text} onCopy={this.handleCopy}>
        <span title={this.props.title} style={{display: 'inline-block', transform: 'translateY(2px)', ...this.props.style}}>
          <FontIcon class='copy-button' type='Clipboard'/>
        </span>
      </CopyToClipboard>
    );
  }
}

export default connect(null, {
  addNotification
})(CopyButton);
