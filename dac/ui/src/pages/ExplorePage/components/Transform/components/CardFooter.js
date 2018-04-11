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
import Immutable from 'immutable';

import FontIcon from 'components/Icon/FontIcon';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import Meter from 'components/Meter';
import { TEAL, BLACK } from 'uiTheme/radium/colors';

const PROGRESS_WIDTH = 455;

@pureRender
@Radium
export default class CardFooter extends Component {
  static propTypes = {
    card: PropTypes.instanceOf(Immutable.Map),
    style: PropTypes.object
  };

  static defaultProps = {
    card: Immutable.Map()
  };

  constructor(props) {
    super(props);
  }

  renderFooter() {
    const { card, style } = this.props;
    const themeMathed = {
      Icon: { color: '#2A394A', ...styles.iconTheme },
      Container: {...styles.mathed, height: 15}
    };
    const max = card.get ? card.get('unmatchedCount') + card.get('matchedCount') : 0;
    const value = card.get ? card.get('matchedCount') : 0;
    const themeUnmathed = {
      Icon: styles.iconTheme,
      Container: {...styles.mathed, height: 15}
    };
    let progressWidth;
    if (style instanceof Array) {
      const propsWidthObject = style && style.find(item => item.width);
      progressWidth = propsWidthObject.width ? {width: propsWidthObject.width} : {width: PROGRESS_WIDTH};
    } else {
      progressWidth = style && style.width ? {width: style.width} : {width: PROGRESS_WIDTH};
    }
    return  <div className='match-panel' style={[progressWidth, styles.labels, style]}>
      <FontIcon type='fa-stop' theme={themeUnmathed}/>
      <span style={[styles.text, {marginLeft: 5}]}>{card.get && card.get('matchedCount')}</span>
      <span style={[styles.text]}>matched values</span>
      <FontIcon type='fa-stop' theme={themeMathed}/>
      <span style={[styles.text, {marginLeft: 5}]}>{card.get && card.get('unmatchedCount')}</span>
      <span style={[styles.text]}>unmatched values</span>
      <Meter
        value={value}
        max={max}
        background={BLACK}
        style={{ ...styles.progress, ...progressWidth }}
      />
    </div>;
  }

  render() {
    return (
      <div style={[styles.base, this.props.style]}>
        {this.renderFooter()}
      </div>
    );
  }
}

const styles = {
  base: {
    bottom: 0
  },
  mathed: {
    margin: '-2px 0 4px 10px',
    color: TEAL
  },
  iconTheme: {
    fontSize: 8,
    width: 8,
    height: 8
  },
  labels: {
    display: 'flex',
    flexWrap: 'wrap'
  },
  progress: {
    height: 7
  },
  text: {
    marginLeft: 5,
    marginTop: 4,
    fontSize: 11,
    color: '#999999',
    display: 'inline-block',
    height: 16
  }
};
