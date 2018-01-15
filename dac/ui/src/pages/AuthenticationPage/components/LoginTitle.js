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
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import FontIcon from 'components/Icon/FontIcon';

@Radium
@pureRender
class LoginTitle extends Component {
  static propTypes = {
    subTitle: PropTypes.node,
    style: PropTypes.object
  };

  render() {
    return (
      <div id='login-title' style={[styles.base, this.props.style]}>
        <div style={[styles.mainTitle]}>
          <FontIcon type='NarwhalLogoWithNameLight' theme={styles.theme}/>
        </div>
        <h1 style={styles.subtitle}>
          {this.props.subTitle}
        </h1>
      </div>
    );
  }
}

const styles = {
  base: {
    marginBottom: 40
  },
  subtitle: {
    color: '#43B8C9',
    fontSize: 27
  },
  mainTitle: {
    display: 'flex',
    alignItems: 'center'
  },
  theme: {
    Icon: {
      width: 240,
      height: 75
    }
  }
};

export default LoginTitle;
