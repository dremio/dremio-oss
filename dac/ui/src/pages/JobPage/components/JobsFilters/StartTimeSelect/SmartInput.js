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
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { bodySmall } from 'uiTheme/radium/typography';

import {FORMAT_HASH} from './MaskedInput';

const SYMBOL_WIDTH = 7;

@PureRender
@Radium
class SmartInput extends Component {
  static propTypes = {
    index: PropTypes.number.isRequired,
    inputValue: PropTypes.string.isRequired,
    mask: PropTypes.string.isRequired,
    placeholder: PropTypes.string.isRequired,
    onFocus: PropTypes.func.isRequired,
    changeInputFocus: PropTypes.func.isRequired,
    onBlur: PropTypes.func.isRequired,
    onChange: PropTypes.func.isRequired,
    showPlaceholder: PropTypes.bool.isRequired
  }

  constructor(props) {
    super(props);
    this.onChange = this.onChange.bind(this);
    this.onFocus = this.onFocus.bind(this);
    this.onBlur = this.onBlur.bind(this);
  }

  onBlur(e) {
    const { placeholder, onBlur, index, inputValue} = this.props;
    const showPlaceholder = !e.target.value || e.target.value === placeholder;
    const newInputValue = showPlaceholder
      ? placeholder
      : inputValue;
    onBlur(index, showPlaceholder, newInputValue);
  }

  onChange(e) {
    const {onChange, index, mask} = this.props;
    const inputArray = e.target.value.slice((-1) * mask.length).split('');
    const inputValue = inputArray.filter((item, ind) => {
      return FORMAT_HASH.get(mask[ind])(item);
    }).join('');
    onChange(index, inputValue, inputValue.length === mask.length);
  }

  onFocus() {
    const showPlaceholder = false;
    const { onFocus, index} = this.props;
    onFocus(index, showPlaceholder, '');
  }

  render() {
    const {mask, showPlaceholder, inputValue} = this.props;
    const width = inputValue.length * SYMBOL_WIDTH || mask.length * SYMBOL_WIDTH;
    const styles = [style.base, {width}, bodySmall];
    if (showPlaceholder) {
      styles.push(style.placeholder);
    }
    return (
      <input
        style = {styles}
        value = {inputValue}
        tabIndex='-1'
        onBlur = {this.onBlur}
        onFocus = {this.onFocus}
        onChange = {this.onChange}/>
    );
  }
}

const style = {
  'base': {
    'border': 'none',
    'backgroundColor': 'inherit',
    'width': 20,
    ':focus':{
      'outline': 'none'
    }
  }
};

export default SmartInput;
