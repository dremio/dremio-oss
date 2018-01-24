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

import { BLUE } from 'uiTheme/radium/colors';
import { checkboxFocus, fieldDisabled } from 'uiTheme/radium/forms';
import { formDefault } from 'uiTheme/radium/typography';

export const checkboxPropTypes = {
  label: PropTypes.node,
  dataQa: PropTypes.string,
  labelBefore: PropTypes.bool,
  inputType: PropTypes.string,
  checked: PropTypes.bool,
  disabled: PropTypes.bool,
  renderDummyInput: PropTypes.func,
  dummyInputStyle: PropTypes.object,
  style: PropTypes.object
};

@Radium
@pureRender
export default class Checkbox extends Component {

  static propTypes = checkboxPropTypes;

  static defaultProps = {
    inputType: 'checkbox'
  }

  renderDummyCheckbox(isChecked, style) {
    return <div style={[styles.dummy, style, isChecked ? styles.checked : null]}
      data-qa={this.props.dataQa}>
      {isChecked ? '✔' : '\u00A0'}
    </div>;
  }

  render() {
    // retrieve the focus state of the container and style the dummy appropriately.
    const focus = Radium.getState(this.state, 'container', ':focus');

    const {style, label, dummyInputStyle, inputType, labelBefore, ...props} = this.props;
    const labelSpan = <span style={styles.labelContent}>{label}</span>;

    const focusStyle = focus ? checkboxFocus : {};
    const finalDummyInputStyle = {...dummyInputStyle, ...focusStyle};

    return (
      <label className='field' key='container' style={[styles.label, props.disabled && styles.labelDisabled, style]}>
        {labelBefore && labelSpan}
        {this.props.renderDummyInput ?
          this.props.renderDummyInput(props.checked, finalDummyInputStyle) :
          this.renderDummyCheckbox(props.checked, finalDummyInputStyle)
        }
        <input type={inputType} style={{position: 'absolute', left: -10000}} {...props}/>
        {!labelBefore && labelSpan}
      </label>
    );
  }
}

const styles = {
  label: {
    ...formDefault,
    cursor: 'pointer', // todo: use css to make all <label>s cursor:pointer?
    display: 'inline-flex',
    position: 'relative',
    alignItems: 'center', // This looks off in FF<=50 (DX-8124) - but resolving messes up the latest versions of browsers
    ':focus': {}  // need empty object so that radium listens to focus events
  },
  labelDisabled: { // todo: DRY with button
    ...fieldDisabled,
    cursor: 'default'
    // todo: look into adding left/right padding - but need to do it for the not-disabled case too
  },
  dummy: {
    flexShrink: 0,
    textAlign: 'center',
    fontSize: 9,
    width: 14,
    height: 14,
    padding: 1,
    margin: '0 5px',
    border: '1px solid #bbb',
    borderRadius: 1,
    background: '#fff',
    verticalAlign: 'text-bottom'
  },
  checked: {
    border: `1px solid ${BLUE}`,
    color: BLUE
  },
  labelContent: {
    lineHeight: '24px',
    display: 'flex',
    minWidth: 0 // flex gives children min width auto. Override so parent label can control size.
  }
};
