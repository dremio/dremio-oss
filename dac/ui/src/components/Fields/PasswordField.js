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

import FontIcon from 'components/Icon/FontIcon';
import TextField from './TextField';

@Radium
export default class PasswordField extends Component {

  static propTypes = {
    initialFocus: PropTypes.bool,
    error: PropTypes.string,
    onChange: PropTypes.func,
    touched: PropTypes.bool,
    disabled: PropTypes.bool,
    default: PropTypes.string,
    style: PropTypes.object,
    value: PropTypes.string
  };

  state = {
    showPass: false
  };

  togglePassView = () => {
    this.setState((state) => ({
      showPass: !state.showPass
    }));
  }

  render() {
    const type = this.state.showPass ? 'text' : 'password';
    return (
      <div className='field' style={{ marginRight: 10 }}>
        <TextField {...this.props} type={type} style={{ marginRight: 0 }}/>
        {this.props.value && <FontIcon
          type='fa-eye'
          style={styles.eyeIcon}
          onClick={this.togglePassView}/>
        }
      </div>
    );
  }
}

const styles = {
  eyeIcon: {
    position: 'absolute',
    cursor: 'pointer',
    right: 10,
    top: 22
  }
};
