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
import pureRender from 'pure-render-decorator';

import FontIcon from 'components/Icon/FontIcon';
import { FLEX_NOWRAP_ROW_BETWEEN_CENTER } from 'uiTheme/radium/flexStyle';

import { formDescription } from 'uiTheme/radium/typography';

const OVERLAY_COLOR = '#3acbac';
const OVERLAY_POINTER_SIZE = 10;

@Radium
@pureRender
export default class SignupTitle extends Component {
  render() {
    return (
      <div id='signup-title' style={[styles.base]}>
        <h1 style={[styles.mainTitle]}>
          {la('Welcome to Dremio')}
        </h1>
        <div style={[styles.subtitleWrap]}>
          <FontIcon type='NarwhalLogo' theme={styles.theme} iconClass={'dremioLogo'} />
          <h3 style={[styles.subtitle]}>
            {la('We are excited to have you on board!')}<br />
            {la('The first thing you need to do is set up an administrator account.')}
          </h3>
          <div></div>{/*for flex, to center the text*/}
        </div>
        <h4 className='whiteText' style={[styles.overlay]}>
          {la('Create Admin Account')}
          <div style={[styles.overlayPointer]} />
        </h4>
      </div>
    );
  }
}

const styles = {
  base: {
    marginBottom: 20
  },
  mainTitle: {
    fontSize: 30,
    marginBottom: 20
  },
  subtitleWrap: {
    ...FLEX_NOWRAP_ROW_BETWEEN_CENTER,
    marginBottom: 20
  },
  subtitle: {
    color: formDescription.color,
    width: 370
  },
  theme: {
    Icon: {
      width: 115,
      height: 111
    }
  },
  overlay: {
    position: 'relative',
    width: '100%',
    background: OVERLAY_COLOR,
    paddingTop: 15,
    paddingBottom: 15,
    paddingLeft: 10
  },
  overlayPointer: {
    position: 'absolute',
    borderLeft: `${OVERLAY_POINTER_SIZE}px solid transparent`,
    borderRight:  `${OVERLAY_POINTER_SIZE}px solid transparent`,
    borderTop:   `${OVERLAY_POINTER_SIZE}px solid ${OVERLAY_COLOR}`,
    bottom: -OVERLAY_POINTER_SIZE,
    left: '50%',
    marginLeft: -OVERLAY_POINTER_SIZE
  }
};
