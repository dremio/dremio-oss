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

import { formDefault } from 'uiTheme/radium/typography';

@Radium
export default class MenuLabel extends Component {
  static propTypes = {
    children: PropTypes.node
  };

  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div style={styles.base}>
        {this.props.children}
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    alignItems: 'center',
    paddingLeft: 8,
    height: 24,
    ...formDefault,
    color: '#999',
    fontStyle: 'italic'
  }
};
