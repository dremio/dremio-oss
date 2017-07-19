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
import { Component, PropTypes } from 'react';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import FontIcon from 'components/Icon/FontIcon';

import { PALE_NAVY } from 'uiTheme/radium/colors';
import { h5 } from 'uiTheme/radium/typography';

@PureRender
@Radium
class RawHeader extends Component {
  static propTypes = {
    closeIconHandler: PropTypes.func,
    closeIcon: PropTypes.bool,
    separator: PropTypes.string,
    text: PropTypes.string,
    subSteps: PropTypes.string
  }

  constructor(props) {
    super(props);
    this.defaultCloseIconClickHandler = this.defaultCloseIconClickHandler.bind(this);
  }

  getCloseIcon() {
    const handler = this.props.closeIconHandler
      ? this.props.closeIconHandler
      : this.defaultCloseIconClickHandler;
    const icon = this.props.closeIcon
      ? <FontIcon type='XBig' theme={style.iconTheme} onClick={handler}/>
      : null;
    return icon;
  }

  getSeparator() {
    const {separator} = this.props;
    return separator
      ? separator
      : ': ';
  }

  defaultCloseIconClickHandler() {
    console.info('close icon clicked');
  }

  subSteps() {
    const {subSteps} = this.props;
    return subSteps
      ? subSteps
      : null;
  }

  render() {
    return (
      <div className='raw-wizard-header' style={[style.base]}>
        <div style={[style.content, h5 ]}>
          {this.props.text}{this.getSeparator()}{this.subSteps()}
        </div>
        {this.getCloseIcon()}
      </div>
    );
  }
}

const style = {
  'base': {
    'display': 'flex',
    'height': 38,
    'justifyContent': 'space-between',
    backgroundColor: PALE_NAVY
  },
  'content': {
    'display': 'flex',
    'marginLeft': 15,
    'alignItems': 'center',
    'fontSize': 15,
    'fontWeight': 600
  },
  'iconTheme': {
    'Container': {
      display: 'flex',
      alignItems: 'center',
      marginRight: 15,
      width: 24,
      height: 38
    },
    'Icon': {
      'cursor': 'pointer'
    }
  }
};

export default RawHeader;
