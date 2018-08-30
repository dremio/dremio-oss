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
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import { Popover, PopoverAnimationVertical } from 'material-ui/Popover';
import FontIcon from 'components/Icon/FontIcon';
import Menu from 'material-ui/Menu';
import { formDefault } from 'uiTheme/radium/typography';
import classNames from 'classnames';
import SelectItem from './SelectItem';
import { button, disabled as disabledCls, icon as iconCls, label as labelCls, list as listCls } from './Select.less';

const MAX_HEIGHT = 250;

@pureRender
export default class Select extends Component {
  static propTypes = {
    items: PropTypes.array, // items of {option?, label}
    value: PropTypes.any,
    handle: PropTypes.func, // todo: safe to delete this? - don't see it used in this file (used by parent?)
    disabled: PropTypes.any, // todo: add a #readonly/readOnly(?) and switch existing uses of #disabled as appropriate)
    style: PropTypes.object,
    className: PropTypes.string,
    iconStyle: PropTypes.object,
    listStyle: PropTypes.object,
    menuStyle: PropTypes.object,
    customLabelStyle: PropTypes.object,
    dataQa: PropTypes.string,
    onChange: PropTypes.func,
    comparator: PropTypes.func,
    itemRenderer: PropTypes.func, // function(item, label) {}
    itemClass: PropTypes.string
  };

  static defaultProps = {
    items: [],
    comparator: (a, b) => a === b
  };

  state = { open: false }

  getButtonLabel(value) {
    const current = this.props.items.find(item => {
      return this.props.comparator(value, this.valueForItem(item));
    });
    return current ? this.labelForItem(current).label : '';
  }

  handleChange = (e, value) => {
    this.handleRequestClose();
    this.props.onChange && this.props.onChange(value);
  }

  valueForItem(item) {
    // todo: why do we call this "option" externally and "value" internally?
    return getIfIn(item, 'option', () => getIfIn(item, 'label', () => item));
  }

  labelForItem(item) {
    const {
      itemRenderer
    } = this.props;
    const label = getIfIn(item, 'label', () => this.valueForItem(item));
    const labelInfo = {
      dataQA: typeof label === 'string' ? label : '',
      label
    };
    if (itemRenderer) {
      labelInfo.label = itemRenderer(item, label);
    }
    return labelInfo;
  }

  handleTouchTap = (event) => {
    event.preventDefault();
    if (!this.props.disabled) {
      this.setState({
        open: !this.state.open,
        anchorEl: event.currentTarget
      });
    }
  }

  handleRequestClose = () => {
    this.setState({ open: false });
  }

  renderItems(selectedValue) {
    const { items, itemClass } = this.props;
    return items.map((item, index) => {
      const val = this.valueForItem(item);
      const selected = this.props.comparator(selectedValue, val);

      return (
        <SelectItem
          key={index}
          selected={selected}
          value={val}
          disabled={item.disabled}
          className={itemClass}
          {...this.labelForItem(item)}/>
      );
    });
  }

  renderIcon() {
    const { iconStyle } = this.props;
    return (<FontIcon type='Arrow-Down-Small' iconClass={iconCls} style={iconStyle}/>);
  }

  render() {
    const {
      disabled,
      style,
      menuStyle,
      listStyle,
      dataQa,
      customLabelStyle,
      className
    } = this.props;
    const defaultOption = this.props.items.length ? this.valueForItem(this.props.items[0]) : undefined;
    const selectedValue = getIfIn(this.props, 'value', () => defaultOption);
    const buttonLabel = this.getButtonLabel(selectedValue);

    // TODO: can't easily remove textOverflow in favor of <EllipsedText> because the content comes in externally
    return (
      <button
        type='button'
        data-qa={dataQa}
        onClick={this.handleTouchTap}
        className={classNames(['field', button, className, disabled && disabledCls])}
        style={style}>
        <span className={labelCls} style={{...customLabelStyle, ...formDefault}}>{buttonLabel}</span>
        {this.renderIcon()}
        <Popover
          open={this.state.open}
          anchorEl={this.state.anchorEl}
          anchorOrigin={{horizontal: 'left', vertical: 'bottom'}}
          targetOrigin={{horizontal: 'left', vertical: 'top'}}
          onRequestClose={this.handleRequestClose}
          animation={PopoverAnimationVertical}>
          <Menu
            autoWidth={false}
            style={menuStyle}
            data-qa='convertTo'
            onChange={this.handleChange}
            maxHeight={MAX_HEIGHT}
            className={listCls}
            listStyle={{...listStyle}}>
            {this.renderItems(selectedValue)}
          </Menu>
        </Popover>
      </button>
    );
  }
}

// Allow Selects to have options which are falsy
function getIfIn(obj, key, fallbackFcn) { // use fcn so no eval if not needed like ||
  if (key in obj) {
    return obj[key];
  }
  return fallbackFcn && fallbackFcn();
}
