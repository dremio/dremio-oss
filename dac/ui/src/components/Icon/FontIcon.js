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
import React, { Component } from 'react';
import Immutable  from 'immutable';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';
import classNames from 'classnames';

import PropTypes from 'prop-types';

import './FontIcon.less';

export const SEARCH = 'fa-search';
export const UNDO = 'fa-undo';
export const EDIT = 'fa-pencil';
export const STOP = 'fa-stop';
export const COLUMNS = 'fa-columns';
export const HOME = 'fa-home';
export const FOLDER = 'fa-folder-o';
export const CLOCK = 'fa-clock-o';
export const USER_IN_BORDER = 'fa-user';
export const CLOSE_X2 = 'fa-times fa-2x';
export const CARET_DOWN = 'fa-caret-down';
const CARET_UP = 'fa-caret-up';
export const CARET_RIGHT = 'fa-caret-right';
export const SPINNER = 'fa-spinner fa-spin';
const FA_ON = 'fa-circle fa-on';
const FA_OFF = 'fa-circle fa-off';
const WARNING_ICON = 'fa-exclamation-circle';
export const ANGEL_DOWN = 'fa fa-angle-down';
const FILE = 'fa fa-file-o';
const ARROW_DOWN = 'fa fa-arrow-down';
const BAR_CHART = 'fa fa-bar-chart';
const FA_TIMES = 'fa-times';
const SHARE_ALT = 'fa-share-alt';
const TRIANGLE = 'fa-exclamation-triangle';
const EYE = 'fa-eye';
const EYE_SLASH = 'fa-eye-slash';
export const PLUS_CIRCLE = 'fa-plus-circle';
const FOLDER_CONVERT = 'FolderConvert';
const FILE_CONVERT = 'FileConvert';
const TRASH = 'Trash';
const FLAME = 'Flame';
const CLIPBOARD = 'Clipboard';
const FONT_ICONS = new Set([CARET_DOWN, WARNING_ICON, CLOSE_X2, ANGEL_DOWN, HOME, CLOCK, USER_IN_BORDER,
  FILE, ARROW_DOWN, BAR_CHART, CARET_UP, FA_ON, FA_OFF, SPINNER, FA_TIMES, CARET_RIGHT, FOLDER,
  SHARE_ALT, TRIANGLE, EYE, EYE_SLASH, SEARCH, COLUMNS, PLUS_CIRCLE, STOP, UNDO, EDIT]);



/**
 * @description Theme is object that specify your styles.
 * Theme may have two properties:
 * Container - styles that will be applied to Container(that is div tag)
 * Icon - styles that will be applied to your Icon directly
 */
const DEFAULT_STYLES = {
  defaultStyle: {
    'Icon': {
      'width': 24,
      'height': 24
    },
    'Container': {
      'display': 'inline-block'
    }
  },
  [FA_ON]: {
    'Icon': {
      'color': '#7FC24E',
      'margin': '0 0 0 5px'
    },
    'Container': {
      'display': 'inline-block'
    }
  },
  [FA_OFF]: {
    'Icon': {
      'color': '#FF7E79',
      'margin': '0 0 0 5px'
    },
    'Container': {
      'display': 'inline-block'
    }
  },
  [CARET_DOWN]: {
    'Icon': {
      'color': 'black',
      'margin': '0 0 0 5px'
    },
    'Container': {
      'display': 'inline-block'
    }
  },
  [CARET_RIGHT]: {
    'Icon': {
      'color': 'black',
      'margin': '0 0 0 5px'
    },
    'Container': {
      'display': 'inline-block'
    }
  },
  [CARET_UP]: {
    'Icon': {
      'color': 'black',
      'margin': '0 0 0 5px'
    },
    'Container': {
      'display': 'inline-block'
    }
  },
  [USER_IN_BORDER]: {
    'Icon': {
      'fontSize': 10,
      'border': '1px solid #111',
      'color': '#111',
      'display': 'flex',
      'alignItems': 'flex-end',
      'width': 14,
      'height': 14,
      'float': 'left',
      'borderRadius': 2
    },
    'Container': {
    }
  },
  [FOLDER_CONVERT]: {
    'Icon': {
      'margin-top': -2,
      'width': 55,
      'height': 24
    }
  },
  [FILE_CONVERT]: {
    'Icon': {
      'margin-top': -2,
      'width': 55,
      'height': 24
    }
  },
  [TRASH]: {
    'Icon': {
      'width': 11,
      'height': 12
    }
  },
  [FLAME]: {
    'Icon': {
      width: 13,
      height: 20
    }
  },
  [CLIPBOARD]: {
    'Icon': {
      'width': 14,
      'height': 14
    }
  }
};
/**
 * @description
 * FontIcon is global icon component
 * This component consist of two parts Icon and Container in which Icon is placed.
 */

// TODO: stop using a "font icon" component to display custom things that aren't in an icon font

@Radium
@PureRender
export class FullFontIcon extends Component {
  static propTypes = {
    onClick: PropTypes.func,
    onMouseLeave: PropTypes.func,
    onMouseEnter: PropTypes.func,
    hoverType: PropTypes.string, // todo: only works with custom icons
    type: PropTypes.string,
    theme: PropTypes.object,
    style: PropTypes.object,
    iconClass: PropTypes.string,
    iconStyle: PropTypes.object,
    id: PropTypes.string,
    class: PropTypes.string
  }

  getIcon() {
    const {type, hoverType, iconStyle, iconClass} = this.props;
    const styles = this.getStylesForThemeItem('Icon');
    if (FONT_ICONS.has(type)) {
      const classes = 'fa ${type}';
      return <i className={classes} style={styles} onClick={this.onClick}/>;
    }

    let typeToShow = type;
    if (hoverType) {
      const isHovered = Radium.getState(this.state, 'iconFont', ':hover');
      if (isHovered) {
        typeToShow = hoverType;
      }
    }
    const classes = `icon-type fa ${typeToShow}`;
    return (
      <span
        className={classNames([classes, iconClass])}
        style={[hoverType ? {':hover': {}} : null, styles, iconStyle]} // need to fake out Radium
        key='iconFont'
        onMouseLeave={this.props.onMouseLeave}
        onMouseEnter={this.props.onMouseEnter}
        onClick={this.props.onClick}/>
    );
  }

  /**
   * @description
   * Method retrieve styles object for icon type from theme that is supplied and from default styles.
   * Case that default styles may not be defined for some types is included
   * Case that theme may be undefined also included
   * @param themeItem{String}
   * @returns {Object[]}
   */
  getStylesForThemeItem(themeItem) {
    const {type, theme} = this.props;

    let defaultStyle = DEFAULT_STYLES[type] && DEFAULT_STYLES[type][themeItem];
    if (!defaultStyle) {
      defaultStyle = DEFAULT_STYLES.defaultStyle[themeItem];
    }

    let customStyle;
    if (Immutable.Map.isMap(theme)) {
      console.warn('DEPRECATED: Now FontIcon supports a more convenient format of theme without Immutable.');
      customStyle = theme && theme.get(themeItem);
    } else {
      customStyle = theme && theme[themeItem];
    }

    return [defaultStyle, customStyle];
  }

  render() {
    const styles = this.getStylesForThemeItem('Container');
    const id = this.props.id || '';
    const className = this.props.class || '';
    const ret =  (
      <span
        className={`font-icon ${className}`}
        id={id} style={styles.concat(this.props.style)}>
        {this.getIcon()}
      </span>
    );
    return ret;
  }
}


/*
 * Creates and caches to optimized Icon components with precalculated props when type is only prop.
 * Otherwise it renders FullFontIcon
 */
export default class FontIcon extends Component {
  static propTypes = {
    type: PropTypes.string,
    theme: PropTypes.object,
    dataQa: PropTypes.string
  }

  static components = {};

  static getIconTypeForDataType(dataType) {
    const icon = dataType
      ? dataType.charAt(0).toUpperCase() + (dataType).substring(1).toLowerCase()
      : 'Text';
    return 'Type' + icon;
  }

  createFastIconComponent(iconClassName, iconStyle, containerStyle) {
    let className = 'fa ' + iconClassName;
    if (!FONT_ICONS.has(iconClassName)) {
      className += ' icon-type';
    }
    const defaultIconStyle = iconStyle || {width: 24, height: 24};
    const defaultContainerStyle = containerStyle || {'display': 'inline-block'};

    @PureRender
    class FastIcon extends Component {
      static propTypes = {
        theme: PropTypes.object,
        dataQa: PropTypes.string
      }

      render() {
        const { theme, dataQa } = this.props;
        return <span
          className='font-icon'
          style={theme && theme.Container || defaultContainerStyle}>
          <span data-qa={dataQa} className={className} style={theme && theme.Icon || defaultIconStyle}/>
        </span>;
      }
    }
    return FastIcon;
  }

  render() {
    const { type, theme, dataQa, ...otherProps } = this.props;
    for (const key in otherProps) {
      if (otherProps.hasOwnProperty(key)) {
        return <FullFontIcon {...this.props}/>;
      }
    }
    let component = FontIcon.components[this.props.type];
    if (!component) {
      const defaultTheme = DEFAULT_STYLES[type] || {};
      component = FontIcon.components[type] = this.createFastIconComponent(
        type,
        defaultTheme.Icon,
        defaultTheme.Container
      );
    }
    return React.createElement(component, {theme, dataQa});
  }
}
