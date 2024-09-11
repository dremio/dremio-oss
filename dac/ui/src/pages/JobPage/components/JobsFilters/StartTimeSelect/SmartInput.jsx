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
import { PureComponent, forwardRef } from "react";
import PropTypes from "prop-types";

import { FORMAT_HASH } from "./MaskedInput";

import * as classes from "./SmartInput.module.less";

const SYMBOL_WIDTH = 9;

class SmartInput extends PureComponent {
  static propTypes = {
    index: PropTypes.number.isRequired,
    inputValue: PropTypes.string.isRequired,
    mask: PropTypes.string.isRequired,
    placeholder: PropTypes.string.isRequired,
    onFocus: PropTypes.func.isRequired,
    changeInputFocus: PropTypes.func.isRequired,
    onBlur: PropTypes.func.isRequired,
    onChange: PropTypes.func.isRequired,
    showPlaceholder: PropTypes.bool.isRequired,
    innerRef: PropTypes.any,
    disabled: PropTypes.bool,
  };

  constructor(props) {
    super(props);
    this.onChange = this.onChange.bind(this);
    this.onFocus = this.onFocus.bind(this);
    this.onBlur = this.onBlur.bind(this);
  }

  onBlur(e) {
    const { placeholder, onBlur, index, inputValue } = this.props;
    const showPlaceholder = !e.target.value || e.target.value === placeholder;
    const newInputValue = showPlaceholder ? placeholder : inputValue;
    onBlur(index, showPlaceholder, newInputValue);
  }

  onChange(e) {
    const { onChange, index, mask } = this.props;
    const inputArray = e.target.value.slice(-1 * mask.length).split("");
    const inputValue = inputArray
      .filter((item, ind) => {
        return FORMAT_HASH.get(mask[ind])(item);
      })
      .join("");
    onChange(index, inputValue, inputValue.length === mask.length);
  }

  onFocus() {
    const showPlaceholder = false;
    const { onFocus, index } = this.props;
    onFocus(index, showPlaceholder, "");
  }

  render() {
    const { mask, inputValue, disabled = false } = this.props;
    const width =
      inputValue.length * SYMBOL_WIDTH || mask.length * SYMBOL_WIDTH;
    const disabledClass = disabled ? classes["disabled"] : "";
    return (
      <input
        ref={this.props.innerRef}
        className={`${classes["smart-input"]} ${disabledClass}`}
        style={{ width }}
        value={inputValue}
        onBlur={this.onBlur}
        onFocus={this.onFocus}
        onChange={this.onChange}
        disabled={disabled}
      />
    );
  }
}
export default forwardRef((props, ref) => (
  <SmartInput innerRef={ref} {...props} />
));
