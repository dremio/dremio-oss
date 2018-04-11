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
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { injectIntl } from 'react-intl';
import CopyToClipboard from 'react-copy-to-clipboard';
import { addNotification } from 'actions/notification';
import Art from 'components/Art';

@injectIntl
export class CopyButton extends Component {
  static propTypes = {
    text: PropTypes.string.isRequired,
    title: PropTypes.string,
    style: PropTypes.object,
    addNotification: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired
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
        <span title={this.props.title} aria-label={this.props.title} style={{...styles.wrap, ...this.props.style}}>
          <Art
            src='Clipboard.svg'
            alt={this.props.intl.formatMessage({ id: 'Common.CopyPath' })}
            className='copy-button'
            style={styles.icon} />
        </span>
      </CopyToClipboard>
    );
  }
}

const styles = {
  icon: {
    cursor: 'pointer',
    width: 14,
    height: 14
  },
  wrap: {
    display: 'inline-block',
    transform: 'translateY(2px)'
  }
};

export default connect(null, {
  addNotification
})(CopyButton);
