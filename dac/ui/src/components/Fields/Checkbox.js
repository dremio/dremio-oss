/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { PureComponent } from 'react';

import PropTypes from 'prop-types';

import classNames from 'classnames';
import TooltipEnabledLabel from 'components/TooltipEnabledLabel';
import {
  onoffBtn,
  onoffDot,
  base,
  labelContent,
  disabled as disabledCls,
  dummy
} from './Checkbox.less';

export const checkboxPropTypes = {
  label: PropTypes.node,
  dataQa: PropTypes.string,
  labelBefore: PropTypes.bool,
  inputType: PropTypes.string,
  checked: PropTypes.bool,
  disabled: PropTypes.bool,
  inverted: PropTypes.bool,
  renderDummyInput: PropTypes.func,
  dummyInputStyle: PropTypes.object,
  style: PropTypes.object,
  className: PropTypes.string,
  initialValue: PropTypes.any,
  autofill: PropTypes.any,
  onUpdate: PropTypes.any,
  valid: PropTypes.any,
  invalid: PropTypes.any,
  dirty: PropTypes.any,
  pristine: PropTypes.any,
  error: PropTypes.any,
  active: PropTypes.any,
  touched: PropTypes.any,
  visited: PropTypes.any,
  autofilled: PropTypes.any,
  isOnOffSwitch: PropTypes.bool,
  toolTip: PropTypes.string,
  toolTipPosition: PropTypes.string
};

export default class Checkbox extends PureComponent {

  static propTypes = checkboxPropTypes;

  static defaultProps = {
    inputType: 'checkbox'
  };

  renderOnOffSwitch(checked, label, labelBefore) {
    const extraStyle = {};
    if (label && labelBefore) {
      extraStyle.marginLeft = 6;
    } else if (label) { //label after
      extraStyle.marginRight = 6;
    }
    if (checked) {
      return <div className={onoffBtn} style={{...styles.switchOnBtn, ...extraStyle}}>
        On <div className={onoffDot} style={styles.onDot}/>
      </div>;
    } else {
      return <div className={onoffBtn} style={{...styles.switchOffBtn, ...extraStyle}}>
        <div className={onoffDot} style={styles.offDot}/>
        Off
      </div>;
    }
  }

  renderLabel(label) {
    return <span className={labelContent}>{label}</span>;
  }

  renderDummyCheckbox(isChecked, style) {
    return <div className={classNames(dummy, isChecked && 'checked')} style={style}
      data-qa={this.props.dataQa || 'dummyCheckbox'}>
      {isChecked ? <i className='fa fa-check'/> : '\u00A0'}
    </div>;
  }

  render() {
    const {
      style, label, dummyInputStyle, isOnOffSwitch,
      inputType, labelBefore,
      className, inverted, renderDummyInput,
      dataQa, initialValue, autofill, onUpdate, valid, invalid, dirty, pristine, error, active, touched, visited, autofilled, // eslint-disable-line @typescript-eslint/no-unused-vars
      toolTip, toolTipPosition,
      ...props
    } = this.props;
    const dummyCheckState = (inverted) ? !props.checked : props.checked;

    // <input .../> should be before dummy input to '~' css selector work
    return (
      <TooltipEnabledLabel className={classNames(['field', base, this.props.disabled && disabledCls, className])} key='container'
        style={style} labelBefore={labelBefore} label={label} labelContentClass={labelContent}
        tooltip={toolTip} toolTipPosition={toolTipPosition}>
        <input disabled={this.props.disabled} type={inputType} style={{position: 'absolute', left: -10000}} {...props}/>
        {renderDummyInput && renderDummyInput(props.checked, dummyInputStyle)}
        {isOnOffSwitch && this.renderOnOffSwitch(props.checked, label, labelBefore)}
        {!renderDummyInput && !isOnOffSwitch && this.renderDummyCheckbox(dummyCheckState, dummyInputStyle)}
      </TooltipEnabledLabel>
    );
  }
}

const styles = {
  switchOnBtn: {
    color: 'white',
    backgroundColor: '#43B8C9',
    paddingLeft: 10
  },
  switchOffBtn: {
    color: '#fff',
    backgroundColor: '#D0D0D0',
    flexDirection: 'row-reverse'
  },
  onDot: {
    backgroundColor: '#fff',
    right: '6px'
  },
  offDot: {
    backgroundColor: '#fff',
    left: '6px'
  }
};
