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

import { lightLink } from 'uiTheme/radium/typography';

export default class AdvancedOptionsExpandable extends Component {
  static propTypes = {
    children: PropTypes.node.isRequired
  }

  state = {
    expanded: false
  }

  handleClick = () => {
    this.setState((state) => ({expanded: !state.expanded}));
  }

  render() {
    const { expanded } = this.state;
    return (
      <div>
        <a style={styles.link} onClick={this.handleClick}>
          {!expanded ? la('Show advanced options…') : la('Hide advanced options…')}
        </a>
        <div style={{...styles.contents, ...(expanded ? styles.expanded : styles.collapsed)}}>
          { this.props.children }
        </div>
      </div>
    );
  }
}

const styles = {
  contents: {
    transition: 'max-height 0.2s ease-in, overflow 0.2s 0.21s'
  },
  expanded: {
    maxHeight: 1000,
    overflow: 'visible',
    paddingTop: 10
  },
  collapsed: {
    maxHeight: 0,
    overflow: 'hidden'
  },
  link: {
    ...lightLink,
    display: 'block'
  }
};
