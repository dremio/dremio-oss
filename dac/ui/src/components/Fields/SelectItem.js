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

import { formDefault, unavailable } from 'uiTheme/radium/typography';
import { PALE_BLUE, PALE_NAVY } from 'uiTheme/radium/colors';

export const CONTENT_WIDTH = 'calc(100% - 25px)';

@Radium
@pureRender
export default class SelectItem extends Component {
  static propTypes = {
    label: PropTypes.node.isRequired,
    style: PropTypes.object,
    value: PropTypes.any,
    disabled: PropTypes.bool,
    onTouchTap: PropTypes.func,
    selected: PropTypes.bool
  }

  handleTouchTap = (event) => {
    event.preventDefault();

    if (this.props.onTouchTap) {
      this.props.onTouchTap(event);
    }
  }

  render() {
    const { disabled, style, label, selected } = this.props;
    const labelStyle = disabled ? unavailable : formDefault;
    const selectedStyle = selected ? {backgroundColor: PALE_NAVY} : {};
    return (
      <div
        onTouchTap={this.handleTouchTap}
        style={[styles.base, style, selectedStyle]}
        className='field'
        data-qa={label}>
        <div style={[labelStyle, styles.label]}>
          {label}
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    height: 25,
    padding: '0 10px',
    lineHeight: '25px',
    cursor: 'pointer',
    width: '100%',
    ':hover': {
      backgroundColor: PALE_BLUE
    }
  },
  label: {
    width: CONTENT_WIDTH,
    whiteSpace: 'nowrap'
  }
};
