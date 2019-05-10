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
import React, { Component, Fragment } from 'react';
import PropTypes from 'prop-types';
import { SelectView } from '@app/components/Fields/SelectView';
import { triangleTop } from 'uiTheme/radium/overlay';

export default class HeaderDropdown extends Component {
  static propTypes = {
    dataQa: PropTypes.string,
    name: PropTypes.string.isRequired,
    menu: PropTypes.node.isRequired
  };

  static defaultProps = {
    name: ''
  };

  render() {
    const { dataQa } = this.props;

    return (
      <SelectView
        content={
          <div className='item'>
            <span className='item-wrap' style={styles.name}>{this.props.name}</span>
            <i className='fa fa-angle-down' style={styles.downArrow}/>
          </div>
        }
        hideExpandIcon // as 'content' render it's own icon
        listStyle={styles.popover}
        listRightAligned
        dataQa={dataQa}
      >
        {
          ({ closeDD }) => (
            <Fragment>
              <div style={styles.triangle}/>
              {React.cloneElement(this.props.menu, { closeMenu: closeDD })}
            </Fragment>
          )
        }
      </SelectView>
    );
  }
}

const styles = {
  name: {
    margin: '0 6px 0 0'
  },
  downArrow: {
    fontSize: '18px'
  },
  popover: {
    marginTop: 5,
    overflow: 'visible'
  },
  triangle: {
    ...triangleTop,
    right: 11
  }
};
