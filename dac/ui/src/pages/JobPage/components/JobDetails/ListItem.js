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
import Radium from 'radium';

import PropTypes from 'prop-types';

@Radium
export default class ListItem extends Component {
  static propTypes = {
    label: PropTypes.string,
    children: PropTypes.node,
    style: PropTypes.object
  }
  render() {
    const { label, children, style } = this.props;
    return (
      <li style={[styles.base, style]}>
        {label && <span style={styles.label}>{label}:</span>}
        {children}
      </li>
    );
  }
}

const styles = {
  base: {
    margin: '5px 0',
    display: 'flex',
    alignItems: 'center'
  },
  label: {
    float: 'left',
    minWidth: 150,
    color: '#999'
  }
};
