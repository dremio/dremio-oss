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
import PropTypes from 'prop-types';
import pureRender from 'pure-render-decorator';

import EllipsedText from 'components/EllipsedText';
import FontIcon from 'components/Icon/FontIcon';
import { h2White } from 'uiTheme/radium/typography';
import { modalPadding } from 'uiTheme/radium/modal';

@pureRender
export default class ModalHeader extends Component {

  static propTypes = {
    title: PropTypes.string,
    hideCloseButton: PropTypes.bool,
    hide: PropTypes.func
  };

  static defaultProps = {
    hideCloseButton: false
  }

  constructor(props) {
    super(props);
  }

  render() {
    const {title, hide, hideCloseButton} = this.props;
    return (
      <div className='modal-header' style={styles.base}>
        <EllipsedText style={styles.title} text={title} />
        {!hideCloseButton && <FontIcon
          type='XBigWhite'
          onClick={hide}
          theme={styles.cancelIcon}
          style={styles.cancel} />}
      </div>
    );
  }
}


const styles = {
  base: {
    ...modalPadding,
    height: 44,
    backgroundColor: '#999999',
    display: 'flex',
    justifyContent: 'space-between',
    flexShrink: 0,
    alignItems: 'center'
  },
  title: {
    ...h2White
  },
  cancel: { // todo: this likely should be a button with :hover/:focus/:active styles
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    cursor: 'pointer'
  },
  cancelIcon: {
    Icon: {
      color: '#fff'
    }
  }
};
