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
import Immutable from 'immutable';

import { fixedWidthSmall } from 'uiTheme/radium/typography';
import { BORDER } from 'uiTheme/radium/colors';
import Spinner from 'components/Spinner';

import FixedWidthForContentCard from './FixedWidthForContentCard';

const MAX_LENGTH_CONTENT = 63;

@pureRender
@Radium
class CardContentView extends Component {
  static getSubstring(example, start, end) {
    const result = example.get('text').substring(start, end);
    return result.replace(/ /g, '\u00a0');
  }

  static propTypes = {
    data: PropTypes.instanceOf(Immutable.List).isRequired,
    isInProgress: PropTypes.bool
  };

  static getExampleTextParts(example) {
    const { text, positionList } = example.toJS();
    if (length === undefined) {
      return [text, '', ''];
    }

    return positionList.reduce((prev, cur, i) => {
      const temp = prev.concat([
        { text: text.slice(positionList[i - 1] && positionList[i - 1].offset + 1 || prev.length, cur.offset) },
        { text: text.slice(cur.offset, cur.offset + cur.length), highlight: true }
      ]);

      if (positionList.length - 1 === i) {
        return temp.concat({ text: text.slice(cur.offset + cur.length) });
      }

      return temp;
    }, []).filter((part) => !!part.text.length).map((part) => ({
      ...part,
      text: part.text.replace(/ /g, '\u00a0')
    }));
  }

  constructor(props) {
    super(props);

    this.getContent = this.getContent.bind(this);
  }

  getContent() {
    return this.props.data.map( (example, index) => {
      if (!example || !example.toJS) {
        return null;
      }
      const ex = example.toJS();
      if (ex.text.length > MAX_LENGTH_CONTENT) {
        return <FixedWidthForContentCard example={ex} index={index}/>;
      }

      const textParts = CardContentView.getExampleTextParts(example);
      const extraBorder = index === 2
        ? { borderBottom: `1px solid ${BORDER}` }
        : {};

      return (
        <div style={[styles.line, extraBorder, fixedWidthSmall]} key={index}>
          {textParts.map((part) => {
            const style = part.highlight ? [fixedWidthSmall, styles.hightlight] : [];
            return (
              <span style={style}>{part.text}</span>
            );
          })}
        </div>
      );
    });
  }

  render() {
    const heightStyle = this.props.data.size < 3
      ? { height: this.props.data.size * 20 }
      : {};
    const content = this.props.isInProgress
      ? <Spinner style={styles.spinnerWrap}/>
      : this.getContent();
    return (
      <div style={[styles.base, {height: '80%'}]}>
        <div style={[styles.wrap, heightStyle]}>
          {content}
        </div>
      </div>
    );
  }
}

export default CardContentView;

const styles = {
  base: {
    display: 'flex',
    justifyContent: 'center',
    flexWrap: 'wrap',
    flexGrow: 1,
    overflowY: 'hidden',
    overflowX: 'hidden',
    marginTop: 10
  },
  wrap: {
    marginTop: 8
  },
  spinnerWrap: {
    left: 0,
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    height: 100
  },
  hightlight: {
    display: 'inline-flex',
    alignItems: 'center',
    backgroundColor: '#f2e8d0',
    height: 19
  },
  line: {
    borderTop: `1px solid ${BORDER}`,
    minWidth: 430,
    maxHeight: 20,
    minHeight: 20,
    maxWidth: 430,
    overflow: 'hidden',
    display: 'flex',
    alignItems: 'center'
  }
};
