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
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import { TEAL } from 'uiTheme/radium/colors';

@pureRender
class Meter extends Component {
  static propTypes = {
    value: PropTypes.number,
    max: PropTypes.number,
    color: PropTypes.string,
    background: PropTypes.string,
    style: PropTypes.object
  }

  static defaultProps = {
    value: 0,
    max: 1
  }

  render() {
    const { color, background, style, value, max } = this.props;
    const barScale = value / max;
    const barStyle = {
      width: `${barScale * 100}%`,
      backgroundColor: color || TEAL,
      ...styles.bar,
      minWidth: barScale === 0 ? 0 : 1
    };
    const barBackground = background || 'none';

    return (
      <div ref='meter' style={{ ...styles.base, background: barBackground, ...style }}>
        <div style={barStyle}/>
      </div>
    );
  }
}

const styles = {
  base: {
    width: '100%',
    height: 8
  },
  bar: {
    height: '100%',
    maxWidth: '100%'
  }
};

export default Meter;
