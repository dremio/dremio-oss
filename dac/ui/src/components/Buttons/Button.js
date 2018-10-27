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
import classNames from 'classnames';
import Immutable  from 'immutable';
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import FontIcon from 'components/Icon/FontIcon';
import { PALE_NAVY } from 'uiTheme/radium/colors';

/**
 * @file - that define all Button Types.
 */
import * as ButtonTypes from './ButtonTypes';

const PRIMARY_CLR = '#43B8C9';
const SECONDARY_CLR = 'rgba(0,0,0,0.04)';
/**
 * @const - this constant define default value for text variable for different button types.
 */
const DEFAULT_TEXT_HASH = new Immutable.Map({ // todo: loc
  [ButtonTypes.CUSTOM]: '',
  [ButtonTypes.NEXT || ButtonTypes.PRIMARY]: 'Next',
  [ButtonTypes.BACK || ButtonTypes.SECONDARY]: 'Back',
  [ButtonTypes.CANCEL || ButtonTypes.SECONDARY]: 'Cancel'
});

/**
 * @description
 * Button class that define standard button.
 * If you want to bind clicker to this component just assign this function to onClick property.
 */
@PureRender
@Radium
class Button extends Component {

  static propTypes = {
    className: PropTypes.string,
    disable: PropTypes.bool,
    disableSubmit: PropTypes.bool,
    text: PropTypes.string,
    type: PropTypes.oneOf(ButtonTypes.TYPES_ARRAY).isRequired,
    onClick: PropTypes.oneOfType([PropTypes.func, PropTypes.bool]),
    onClickDrop: PropTypes.func,
    iconStyle: PropTypes.object,
    innerTextStyle: PropTypes.object,
    onMouseDown: PropTypes.func,
    style: PropTypes.object,
    styles: PropTypes.oneOfType([PropTypes.array, PropTypes.object]),
    icon: PropTypes.string,
    items: PropTypes.array
  }

  constructor(props) {
    super(props);
    this.onClick = this.onClick.bind(this);
    this.closeDropdown = this.closeDropdown.bind(this);
    this.selectItemDropDown = this.selectItemDropDown.bind(this);
    this.toggleDropdown = this.toggleDropdown.bind(this);
    this.isClickOnChildOfParent = this.isClickOnChildOfParent.bind(this);
    this.state = {
      dropdown: false,
      updateBtnName: props.items && props.items[0] ? props.items[0].name : null
    };
  }

  componentWillUnmount() {
    $(document).off('click', this.clickListener);
  }

  onClick(evt) {
    const { items, onClick, disable } = this.props;
    if (onClick && !disable) {
      onClick(items ? this.state.updateBtnName : evt);
    }
  }

  closeDropdown() {
    this.setState({dropdown: false});
  }

  isClickOnChildOfParent(target, classNamesPrevented) {
    for (let i = 0; i < classNamesPrevented.length; i++) {
      if (target.className.indexOf(classNamesPrevented[i]) !== -1) {
        this.counter = 0;
        return true;
      }
    }
    this.counter++;
    const isNextTarget = !target || !target.parentElement;
    if (this.counter > 8 || isNextTarget) {
      this.counter = 0;
      return false;
    }
    return this.isClickOnChildOfParent(target.parentElement, classNamesPrevented);
  }

  selectItemDropDown(name, id) {
    this.setState({updateBtnName: name});
    if (this.props.onClickDrop) {
      this.props.onClickDrop(id);
    }
    this.closeDropdown();
  }

  showDropdown(items) {
    if (this.state.dropdown) {
      return this.renderDropdown(items);
    }
  }

  toggleDropdown() {
    this.setState({dropdown: !this.state.dropdown});
  }

  renderBtn() {
    const { text, type, icon, items, disableSubmit, disable } = this.props;
    const customStyle = this.props.styles ? this.props.styles : {};
    const commonStyle = type === ButtonTypes.NEXT || type === ButtonTypes.PRIMARY
      ? [styles.primary, customStyle]
      : [styles.secondary, customStyle];
    const standartBtn = type === ButtonTypes.NEXT || type === ButtonTypes.PRIMARY
      ? [commonStyle, {':hover': {backgroundColor: '#68C6D3'}}, customStyle]
      : [commonStyle, {':hover': {backgroundColor: 'rgba(0,0,0,0.02)'}}, customStyle];
    const iconBtn = icon
      ? (
        <FontIcon
          type={icon}
          theme={{...styles.icon, ...this.props.iconStyle}}/>
      )
      : null;
    return !items
      ? (
        <button
          data-qa={text}
          type={disableSubmit ? 'button' : 'submit'}
          style={[styles.wrapper, standartBtn, disable && {opacity: 0.7, pointerEvents: 'none'}, this.props.style]}
          className={classNames(this.props.className, icon)}
          onSubmit={this.onSubmit}
          onMouseDown={this.props.onMouseDown}
          onClick={this.onClick}>
          {iconBtn}
          <span style={[styles.innerText, this.props.innerTextStyle]}>
            {text || DEFAULT_TEXT_HASH.get(type)}
          </span>
        </button>
      )
      : this.renderBtnWithDropDown(items, commonStyle, iconBtn);
  }

  renderBtnWithDropDown(items, styleBtn, iconBtn) {
    const { updateBtnName } = this.state;
    const { icon, type } = this.props;
    const color = type === ButtonTypes.NEXT || type === ButtonTypes.PRIMARY
      ? {backgroundColor: 'rgba(255,255,255,0.25)'}
      : {backgroundColor: '#6A7781'};
    return (
      <div
        style={[styles.wrapper, this.props.styles, styleBtn]}
        className={icon || `Arrow-Down-Small${updateBtnName}`}
        key={icon}>
        <div style={[styleBtn, styles.wrapRight]} onClick={this.onClick}>
          {iconBtn}
          <div style={styles.text}>{updateBtnName}</div>
          <div style={[styles.delimiter, color]}></div>
        </div>
        <FontIcon
          type='Arrow-Down-Small' theme={styles.arrowIcon}
          onClick={this.toggleDropdown} className={updateBtnName}/>
        {this.showDropdown(items)}
      </div>
    );
  }

  renderDropdown(items) {
    const WIDTH_BTN = 100;
    const position = {marginLeft: 0 - WIDTH_BTN, top: 30};
    const itemsDrop = items.map((item) => {
      return (
        <span
          key={item.name} style={styles.itemDrop}
          onClick={this.selectItemDropDown.bind(this, item.name, item || {})}>
          {item.name}
        </span>
      );
    });
    return (
      <div style={[styles.dropdown, position]}>
        {itemsDrop}
      </div>
    );
  }

  render() {
    const {type, text} = this.props;
    if (type === ButtonTypes.CUSTOM && !text) {
      throw new Error('Custom button must have a text!');
    }

    return this.renderBtn();
  }
}

const styles = {
  text: {
    paddingRight: 3,
    marginLeft: 8
  },

  content: {
    ':hover': {
      backgroundColor: 'rgba(0,0,0,0.02)'
    }
  },

  wrapper: {
    borderRight: 'none',
    borderLeft: 'none',
    borderTop: 'none',
    position: 'relative',
    minWidth: 100,
    height: 28,
    borderRadius: 2,
    marginBottom: 5,
    fontSize: 11,
    outline: 0,
    cursor: 'pointer',
    lineHeight: '38px'
  },

  wrapRight: {
    marginTop: 0,
    borderRadius: 2,
    flexGrow: 2,
    height: 27,
    position: 'relative',
    zIndex: 20,
    display: 'inline-flex',
    flexDirection: 'row',
    flexWrap: 'nowrap',
    justifyContent: 'flex-end',
    alignItems: 'center',
    borderBottom: 'none',
    ':hover': {
      backgroundColor: 'rgba(0,0,0,0.02)'
    }
  },

  innerText: {
    width: '100%',
    top: -6,
    position: 'relative'
  },

  secondary: {
    color: '#505050',
    backgroundColor: SECONDARY_CLR,
    borderBottom: '1px solid rgba(0,0,0,0.05)'
  },

  primary: {
    backgroundColor: PRIMARY_CLR,
    borderBottom: '1px solid #3399A8',
    color: '#FFFFFF'
  },

  icon: {
    'Icon': {
      height: 22,
      width: 22,
      opacity: 0.6,
      marginTop: 3
    },
    'Container': {
      height: 28,
      width: 24,
      marginLeft: 3,
      marginRight: 10
    }
  },

  delimiter: {
    height: 20,
    width: 1,
    marginTop: 4,
    marginBottom: 4,
    backgroundColor: '#6A7781',
    opacity: 0.6,
    borderRadius: 2,
    marginLeft: 10
  },

  arrowIcon: {
    'Container': {
      height: 27,
      width: 26,
      borderRadius: 2,
      position: 'relative',
      flexBasis: 26,
      ':hover': {
        backgroundColor: 'rgba(0,0,0,0.02)'
      }
    },
    'Icon': {
      width: 26,
      height: 27,
      position: 'absolute',
      top: 1,
      right: 1
    }
  },

  dropdown: {
    minWidth: 100,
    borderRadius: 2,
    position: 'absolute',
    backgroundColor: '#F5FCFF',
    padding: 5,
    display: 'flex',
    flexDirection: 'column',
    zIndex: 99999
  },

  itemDrop: {
    padding: 5,
    cursor: 'pointer',
    color: 'black',
    ':hover': {
      backgroundColor: PALE_NAVY,
      opacity: 0.5,
      color: 'black'
    }
  }
};

export default Button;
