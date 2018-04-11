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
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import FontIcon from './Icon/FontIcon';

const DEFAULT_ICON_SIZE = 30;

@Radium
@pureRender
class Spinner extends Component {
  static propTypes = {
    iconStyle: PropTypes.object,
    containerStyle: PropTypes.object,
    message: PropTypes.node,
    style: PropTypes.object
  }

  render() {
    const iconStyle = {...styles.iconStyle, ...this.props.iconStyle};
    const containerStyle = {...styles.containerStyle, ...this.props.containerStyle};
    return (
      <div style={[styles.base, this.props.style]}>
        <FontIcon
          type='Loader spinner'
          theme={{Icon: iconStyle, Container: containerStyle}}
        />
        {this.props.message && <span style={styles.message}>{this.props.message}</span>}
      </div>
    );
  }
}

const styles = {
  base: {
    left: 0,
    position: 'absolute',
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    width: '100%',
    height: '100%',
    zIndex: 99
  },
  containerStyle: {},
  iconStyle: {
    width: DEFAULT_ICON_SIZE,
    height: DEFAULT_ICON_SIZE
  },
  message: {
    color: 'inherit'
  }
};

export default Spinner;
