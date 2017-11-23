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
import Radium from 'radium';
import PropTypes from 'prop-types';
import ExpandIcon from 'components/ExpandIcon';

@Radium
export default class JobErrorLog extends Component {
  static propTypes = {
    error: PropTypes.string
  }

  constructor(props) {
    super(props);

    this.expandIcon = this.expandIcon.bind(this);

    this.state = { expanded: false };
  }

  expandIcon() {
    this.setState({
      expanded: !this.state.expanded
    });
  }

  render() {
    const { error } = this.props;
    const { expanded } = this.state;
    const expandedStyle = expanded ? { maxHeight: 'none' } : {};
    return (
      <div style={styles.base}>
        <div style={[styles.messageContent, expandedStyle]}>
          <span>{error}</span>
        </div>
        <div onClick={this.expandIcon} style={styles.expandPanel}>
          <ExpandIcon expanded={expanded} />
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    backgroundColor: '#FEEAEA',
    margin: 10,
    borderBottom: '1px solid rgba(0, 0, 0, 0.0470588)',
    borderRadius: 1
  },
  messageContent: {
    maxHeight: 142,
    padding: 5,
    whiteSpace: 'pre',
    overflowY: 'auto',
    'MozUserSelect': 'text',
    'WebkitUserSelect': 'text',
    'UserSelect': 'text',
    position: 'relative',
    fontFamily: 'Menlo, monospace',
    fontWeight: 400,
    fontSize: 12,
    color: 'rgb(51, 51, 51)',
    lineHeight: 1.5,
    wordWrap: 'break-word',
    width: '100%'
  },
  expandPanel: {
    display: 'flex',
    cursor: 'pointer',
    backgroundColor: '#f5e2e2',
    justifyContent: 'center',
    borderTop: '1px solid rgba(0, 0, 0, 0.0470588)',
    height: 25,
    width: '100%'
  }
};
