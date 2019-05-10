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
import { PureComponent } from 'react';

import PropTypes from 'prop-types';
import { get } from 'lodash/object';

import { SelectView } from '@app/components/Fields/SelectView';
import { formDefault } from 'uiTheme/radium/typography';
import classNames from 'classnames';
import SelectItem from './SelectItem';
import { button, label as labelCls, list as listCls } from './Select.less';

export default class Select extends PureComponent {
  static propTypes = {
    items: PropTypes.array, // items of {option?, label, dataQa}
    value: PropTypes.any,
    handle: PropTypes.func, // todo: safe to delete this? - don't see it used in this file (used by parent?)
    disabled: PropTypes.any, // todo: add a #readonly/readOnly(?) and switch existing uses of #disabled as appropriate)
    style: PropTypes.object,
    className: PropTypes.string,
    listClass: PropTypes.string,
    customLabelStyle: PropTypes.object,
    dataQa: PropTypes.string,
    onChange: PropTypes.func,
    comparator: PropTypes.func,
    //todo change default option to a value
    valueField: PropTypes.string, // a field name value item holds value. 'option' is a default
    itemRenderer: PropTypes.func, // function(item) {}
    selectedValueRenderer: PropTypes.func, // renders a selected value in main button content
    itemClass: PropTypes.string
  };

  static defaultProps = {
    items: [],
    comparator: (a, b) => a === b,
    valueField: 'option'
  };

  state = { anchorEl: null }

  getButtonLabel(value) {
    const {
      items,
      selectedValueRenderer
    } = this.props;

    const current = items.find(item => {
      return this.props.comparator(value, this.getValue(item));
    });
    if (!current) {
      return '';
    }
    if (selectedValueRenderer) {
      return selectedValueRenderer(current);
    }
    return this.getDisplayValue(current).label;
  }

  handleChange = (closeDDFn, e, value) => {
    closeDDFn();
    this.props.onChange && this.props.onChange(value);
  }

  getValue(item) {
    return get(item, this.props.valueField, item.label);
  }

  getDisplayValue(item) {
    const { itemRenderer } = this.props;
    const label = get(item, 'label', this.getValue(item));
    const labelInfo = {
      dataQa: item.dataQa || (typeof label === 'string' ? label : ''),
      label
    };
    if (itemRenderer) {
      labelInfo.label = itemRenderer(item);
    }
    return labelInfo;
  }

  renderItems(selectedValue, closeDDFn) {
    const { items, itemClass } = this.props;
    return items.map((item, index) => {
      const val = this.getValue(item);
      const selected = this.props.comparator(selectedValue, val);

      return (
        <SelectItem
          key={index}
          selected={selected}
          value={val}
          disabled={item.disabled}
          className={itemClass}
          onClick={this.handleChange.bind(this, closeDDFn)}
          {...this.getDisplayValue(item)}/>
      );
    });
  }

  render() {
    const {
      disabled,
      style,
      dataQa,
      customLabelStyle,
      className,
      listClass,
      value
    } = this.props;

    const defaultOption = this.props.items.length ? this.getValue(this.props.items[0]) : undefined;
    const selectedValue = value === undefined || value === null ? defaultOption : value;
    const buttonLabel = this.getButtonLabel(selectedValue);

    // TODO: can't easily remove textOverflow in favor of <EllipsedText> because the content comes in externally
    return (
      <SelectView
        content={<span className={labelCls} style={{...customLabelStyle, ...formDefault}}>{buttonLabel}</span>}
        disabled={disabled}
        className={classNames(['field', button, className])}
        style={style}
        listClass={classNames([listCls, listClass])}
        dataQa={dataQa}
        rootAttrs={{ 'aria-role': 'listbox' }}
      >
        {
          ({ closeDD }) => this.renderItems(selectedValue, closeDD)
        }
      </SelectView>
    );
  }
}
