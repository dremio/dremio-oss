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
import pureRender from 'pure-render-decorator';

import {Popover, PopoverAnimationVertical} from 'material-ui/Popover';
import FontIcon from 'components/Icon/FontIcon';
import Menu from 'material-ui/Menu';
import { formDefault } from 'uiTheme/radium/typography';
import { fieldDisabled } from 'uiTheme/radium/forms';
import { SECONDARY_BORDER } from 'uiTheme/radium/colors';
import SelectItem, { CONTENT_WIDTH } from './SelectItem';

const MAX_HEIGHT = 250;
const WIDTH = 200;
const DROPDOWN_COLOR = '#ececec';

@Radium
@pureRender
export default class Select extends Component {
  static propTypes = {
    items: PropTypes.array, // items of {option?, label}
    value: PropTypes.any,
    handle: PropTypes.func, // todo: safe to delete this? - don't see it used in this file (used by parent?)
    disabled: PropTypes.any, // todo: add a #readonly/readOnly(?) and switch existing uses of #disabled as appropriate)
    style: PropTypes.object,
    iconStyle: PropTypes.object,
    listStyle: PropTypes.object,
    menuStyle: PropTypes.object,
    buttonStyle: PropTypes.object,
    customLabelStyle: PropTypes.object,
    dataQa: PropTypes.string,
    onChange: PropTypes.func,
    comparator: PropTypes.func
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
    return current ? this.labelForItem(current) : '';
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
    return getIfIn(item, 'label', () => this.valueForItem(item));
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
    const { items } = this.props;
    return items.map((item, index) => {
      const val = this.valueForItem(item);
      const selected = this.props.comparator(selectedValue, val);

      return (
        <SelectItem
          key={index}
          selected={selected}
          value={val}
          disabled={item.disabled}
          label={this.labelForItem(item)}/>
      );
    });
  }

  renderIcon() {
    const { iconStyle } = this.props;
    return (<FontIcon type='Arrow-Down-Small' style={[styles.iconStyle, iconStyle]}/>);
  }

  render() {
    const { disabled, style, menuStyle, listStyle, buttonStyle, dataQa, customLabelStyle } = this.props;
    const defaultOption = this.props.items.length ? this.valueForItem(this.props.items[0]) : undefined;
    const selectedValue = getIfIn(this.props, 'value', () => defaultOption);
    const buttonLabel = this.getButtonLabel(selectedValue);
    const baseStyle = [styles.base, style, disabled && fieldDisabled];
    // TODO: can't easily remove textOverflow in favor of <EllipsedText> because the content comes in externally
    return (
      <div>
        <div style={baseStyle} data-qa={dataQa}>
          <button
            onClick={this.handleTouchTap}
            style={[styles.button, buttonStyle]}>
            <span style={[styles.label, customLabelStyle, formDefault]}>{buttonLabel}</span>
            {this.renderIcon()}
          </button>
        </div>
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
            listStyle={{...styles.listStyle, ...listStyle}}>
            {this.renderItems(selectedValue)}
          </Menu>
        </Popover>
      </div>
    );
  }
}

const styles = {
  base: {
    width: WIDTH
  },
  iconStyle: {
    paddingTop: 2,
    float: 'right',
    height: '100%'
  },
  button: {
    borderRadius: 2,
    padding: '0 0 0 10px',
    borderBottomWidth: 1,
    borderRightWidth: 0,
    borderLeftWidth: 0,
    borderTopWidth: 0,
    outline: 'none',
    height: 28,
    lineHeight: '28px',
    width: '100%',
    backgroundColor: DROPDOWN_COLOR,
    borderStyle: 'solid',
    borderColor: SECONDARY_BORDER
  },
  listStyle: {
    width: WIDTH,
    paddingTop: 5,
    paddingBottom: 5
  },
  label: {
    width: CONTENT_WIDTH,
    whiteSpace: 'nowrap',
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    float: 'left'
  }
};


// Allow Selects to have options which are falsy
function getIfIn(obj, key, fallbackFcn) { // use fcn so no eval if not needed like ||
  if (key in obj) {
    return obj[key];
  }
  return fallbackFcn && fallbackFcn();
}
