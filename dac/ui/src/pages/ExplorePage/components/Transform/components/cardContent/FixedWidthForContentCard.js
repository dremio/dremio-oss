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
import Radium from 'radium';
import { fixedWidthSmall } from 'uiTheme/radium/typography';
import { TEAL } from 'uiTheme/radium/colors';
const MAX_LENGTH_CONTENT = 63;
const SYMBOL_WIDTH = 6;

@pureRender
@Radium
export default class FixedWidthForContentCard extends Component {
  static propTypes = {
    example: PropTypes.object,
    index: PropTypes.number
  };

  static getExampleTextParts({text, offset, length}, width) {
    let beforeStart;
    let afterEnd;
    if (!width) {
      beforeStart = offset;
      afterEnd = offset + length;
    } else {
      const maxLength = Math.min(MAX_LENGTH_CONTENT, width / SYMBOL_WIDTH);
      const remainingSymbols = Math.max(0, maxLength - length);

      beforeStart = Math.max(0, offset - remainingSymbols / 2);
      afterEnd = Math.min(text.length, beforeStart + length + remainingSymbols);
    }

    return [
      text.slice(beforeStart, offset),
      text.slice(offset, offset + length),
      text.slice(offset + length, afterEnd)
    ].map(FixedWidthForContentCard.insertReturnKey);
  }

  static insertReturnKey(text) {
    const res = [];
    text.replace(/ /g, '\u00a0').split('\n').forEach((e, i) => {
      if (i !== 0 ) {
        res.push(<span style={{color: TEAL}}>&#9166;</span>);
      }
      res.push(e);
    });
    return res;
  }

  constructor(props) {
    super(props);
    this.state = {
      width: 0
    };
  }

  componentDidMount() {
    this.handleWidthForContentCard();
  }

  handleWidthForContentCard() {
    this.setState({width: this.refs.wrap.offsetWidth});
  }

  render() {
    const extraBorder = this.props.index === 2
        ? { borderBottom: '1px solid #ccc' }
        : {};
    const data = FixedWidthForContentCard.getExampleTextParts(
      this.props.example, this.state.width
    );
    return (
      <div className='fixed_width' ref='wrap' style={[styles.line, extraBorder, fixedWidthSmall]}>
        <span >{data[0]}</span>
        <span style={[fixedWidthSmall, styles.hightlight]}>{data[1]}</span>
        <span className='end'>{data[2]}</span>
      </div>
    );
  }
}

const styles = {
  hightlight: {
    display: 'inline-flex',
    alignItems: 'center',
    backgroundColor: '#f2e8d0',
    height: 19
  },
  line: {
    borderTop: '1px solid #ccc',
    minWidth: 430,
    maxHeight: 20,
    minHeight: 20,
    maxWidth: 430,
    overflow: 'hidden',
    display: 'flex',
    alignItems: 'center'
  }
};
