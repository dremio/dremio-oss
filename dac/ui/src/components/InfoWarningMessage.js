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

import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import { PALE_BLUE } from 'uiTheme/radium/colors';
import Button from 'components/Buttons/Button';

const styles = {
  innerText: {
    top: 0
  },
  button: {
    width: 120,
    height: 26,
    margin: '15px 0 0',
    lineHeight: '24px'
  },
  quoteWrapper: {
    position: 'relative',
    display: 'flex',
    margin: '20px 0 0 15px'
  },
  quoteBlock: {
    position: 'relative',
    border: '1px solid rgba(0,0,0,.1)',
    borderRadius: 2,
    background: PALE_BLUE,
    maxWidth: 370,
    padding: 10
  }
};

@Radium
class InfoWarningMessage extends Component {
  static propTypes = {
    text: PropTypes.string,
    info: PropTypes.string,
    buttonLabel: PropTypes.string,
    onClick: PropTypes.func
  };

  render() {
    return (
      <div style={styles.quoteWrapper}>
        <div className='quote-block' style={styles.quoteBlock}>
          <div>{this.props.text}</div>
          <div style={this.props.info ? {marginTop: 8} : {}}>{this.props.info}</div>
          <Button
            type={ButtonTypes.CUSTOM}
            innerTextStyle={styles.innerText}
            text={this.props.buttonLabel}
            styles={[styles.button]}
            onClick={this.props.onClick}/>
        </div>
      </div>
    );
  }
}

export default InfoWarningMessage;
