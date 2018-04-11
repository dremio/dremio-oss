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
import ReactDOM from 'react-dom';

import { CELL_EXPANSION } from 'uiTheme/radium/colors';
import FontIcon from 'components/Icon/FontIcon';

@pureRender
export default class EllipsisIcon extends Component {
  static propTypes = {
    onClick: PropTypes.func,
    iconStyle: PropTypes.object,
    containerStyle: PropTypes.object
  };

  ellipsis = null; // eslint-disable-line react/sort-comp

  constructor(props) {
    super(props);

    this.onClick = this.onClick.bind(this);
  }
  onClick(e) {
    e.preventDefault();
    const anchor = this.ellipsis;
    this.props.onClick(anchor);
  }
  render() {
    const { iconStyle, containerStyle } = this.props;
    const theme = {
      Icon: {},
      Container: {}
    };

    theme.Icon = {...theme.Icon, ...styles.ellipsis.Icon, ...iconStyle};
    theme.Container = {...theme.Container, ...styles.ellipsis.Container, ...containerStyle};

    return (
      <div className='ellipsis-icon'>
        <FontIcon
          theme={theme}
          ref={(component) => this.ellipsis = ReactDOM.findDOMNode(component)}
          type='Ellipsis'
          onClick={this.onClick}
        />
      </div>
    );
  }
}

const styles = {
  ellipsis: {
    Container: {
      position: 'absolute',
      right: 5,
      top: 4,
      cursor: 'pointer',
      borderRadius: 100,
      backgroundColor: CELL_EXPANSION,
      zIndex: 9000
    },
    Icon: {
      height: 13,
      width: 15,
      position: 'relative',
      borderRadius: 100,
      top: '.5px'
    }
  }
};
