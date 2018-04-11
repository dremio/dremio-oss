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
export default class DividerHr extends Component {
  static propTypes = {
    style: PropTypes.object
  };

  constructor(props) {
    super(props);
  }

  render() {
    return (
      <hr style={[styles.base, this.props.style]} />
    );
  }
}

const styles = {
  base: {
    padding: 0,
    height: 1,
    border: 0,
    margin: '5px 0',
    width: '100%',
    background: '#e4e4e4'
  }
};
