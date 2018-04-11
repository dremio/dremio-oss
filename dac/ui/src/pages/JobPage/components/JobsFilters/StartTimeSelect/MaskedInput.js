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
import Immutable  from 'immutable';
import Radium from 'radium';
import ReactDOM from 'react-dom';
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import SmartInput from './SmartInput';

// TODO support of different formats, not only numbers
export const FORMAT_HASH = Immutable.fromJS({
  'd': (char) => /[0-9]/.test(char)
});

/**
 * @class MaskedInput - component of input that support formats like 'dd:dd' and so on
 * @description
 * this component consists of a some amount of inputs(editable areas) and spans(not-editable areas)
 * Inputs check user input for correct format(Smart Input are used)
 */
@PureRender
@Radium
class MaskedInput extends Component {

  static propTypes = {
    mask: PropTypes.string.isRequired,
    value: PropTypes.string.isRequired,
    placeholder: PropTypes.string.isRequired,
    onBlur: PropTypes.func,
    onFocus: PropTypes.func,
    onChange: PropTypes.func
  };

  constructor(props) {
    super(props);
    this.formatCharList = FORMAT_HASH.keySeq().toList();
    this.state = {...this.mapPropsToState(props), focusedInput: null};
    this.applyChanges = this.applyChanges.bind(this);
    this._changeInnerInputFocus = this._changeInnerInputFocus.bind(this);
    this.onBlur = this.onBlur.bind(this);
    this._onInnerInputBlur = this._onInnerInputBlur.bind(this);
    this._onInnerInputFocus = this._onInnerInputFocus.bind(this);
    this._onInnerInputChange = this._onInnerInputChange.bind(this);
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.value !== this.props.value ) {
      this.setState(this.mapPropsToState(nextProps));
    }
  }

  onBlur(inputValue) {
    if (this.state.focusedInput === null) {
      this.props.onBlur(inputValue);
    }
  }

  getInputValue() {
    const valueArray = this.state.inputsList.map( (item) => {
      const value = item.get('value');
      return `${value}${item.get('separator')}`;
    });
    return valueArray.join('');
  }

  /**
   *
   * @param index{Number} - index of current input
   * @private
   * @description
   * method that trig focus on next input in inputList
   */
  _changeInnerInputFocus(index) {
    const input = ReactDOM.findDOMNode(this.refs[`si.${index + 1}`]);
    if (input) {
      input.focus();
    }
  }

  _onInnerInputBlur(index, showPlaceholder, inputValue) {
    this.setState({
      focusedInput: null
    });
    const callback = this.callBackFactory(this.onBlur);
    this.applyChanges(index, showPlaceholder, inputValue, callback);
  }

  _onInnerInputChange(index, inputValue) {
    const {onChange} = this.props;
    const callback = this.callBackFactory(onChange, false);
    this.applyChanges(index, false, inputValue, callback);
  }

  _onInnerInputFocus(index, showPlaceholder, inputValue) {
    const {onFocus} = this.props;
    this.setState({
      focusedInput: index
    });
    const callback = this.callBackFactory(onFocus);
    this.applyChanges(index, showPlaceholder, inputValue, callback);
  }

  /**
   * @description
   * @param index{Number} - index of inner input in inputList
   * @param showPlaceholder{Boolean} - flag that whether to show placeholder or not
   * @param inputValue{String} - value of inner input
   * @param callBack{Function} - callback function that will be trigged by setState function
   */
  applyChanges(index, showPlaceholder, inputValue, callBack) {
    const inputsList = this.state.inputsList.setIn(
      [index, 'value'], inputValue
    ).setIn(
      [index, 'showPlaceholder'], showPlaceholder
    );
    this.setState({
      inputsList
    }, callBack);
  }

  /**
   * @param functionFromProps - function from props that in this case have higher priority
   * @param optionalFunction - function with lower priority
   * @returns {Function} - callback function that are used in this.setState callback
   */
  callBackFactory( functionFromProps, optionalFunction) {
    return () => {
      const inputValue = this.getInputValue();
      setTimeout(() => {
        if (functionFromProps) {
          functionFromProps(inputValue);
        }
        if (optionalFunction) {
          optionalFunction();
        }
      }, 0);
    };
  }

  mapPropsToState(props) {
    const {mask, placeholder, value} = props;
    const separatorArray = mask.split('').filter((item) => this.formatCharList.indexOf(item) === -1);
    // regExp for split by separators(might be several)
    const sepReg = new RegExp(`[${separatorArray.join()}]`);
    const placeholdersList = placeholder.split(sepReg);
    const valueList = value.split(sepReg);
    const inputsList = new Immutable.List(mask.split(sepReg).map((item, index) => {
      const separator = separatorArray[index] || '';
      return new Immutable.Map({
        mask: item,
        placeholder: placeholdersList[index],
        value: valueList[index],
        showPlaceholder: placeholdersList[index] === valueList[index],
        separator
      });
    }));
    return {
      inputsList
    };
  }

  render() {
    const {inputsList} = this.state;
    // look for false value of showPlaceholder
    const showPlaceholder = inputsList.every((item) => {
      return item.get('showPlaceholder');
    });
    const inputs = inputsList.map((item, index) => {
      return (
        <span key = {`si.${item.get('mask')}.${item.get('placeholder')}.${index}`}>
          <SmartInput
            mask = {item.get('mask')}
            index = {index}
            ref = {`si.${index}`}
            inputValue = {item.get('value')}
            placeholder = {item.get('placeholder')}
            onFocus = {this._onInnerInputFocus}
            changeInputFocus = {this._changeInnerInputFocus}
            onBlur = {this._onInnerInputBlur}
            onChange = {this._onInnerInputChange}
            showPlaceholder = {showPlaceholder}/>
          {item.get('separator')}
        </span>);
    });
    const styles = [style.base];
    if (showPlaceholder) {
      styles.push(style.placeholder);
    }
    return (
      <div className='masked-input' style={styles}>
        {inputs}
      </div>
    );
  }
}

const style = {
  'base': {
    'margin': 2,
    'backgroundColor': 'white'
  },
  'placeholder':{
    'color': '#c4c4c4'
  }
};

export default MaskedInput;
