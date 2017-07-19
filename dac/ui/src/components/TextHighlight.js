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
import ReactDOM from 'react-dom';
import pureRender from 'pure-render-decorator';

@pureRender
export default class TextHighlight extends Component {

  static propTypes = {
    text: PropTypes.string,
    inputValue: PropTypes.string
  };

  componentDidMount() {
    this.updateData();
  }

  componentDidUpdate() {
    this.updateData();
  }

  getMarkStr() {
    const val = this.props.inputValue || '';
    const str = this.props.text || '';
    const escape = val.replace(/[-\\^$*+?.()|[\]{}]/g, '\\$&');
    const tagStr = `<{tag} style="${styleString}">$&</{tag}>`;

    if (val.length === 0) {
      return str;
    }

    return str && str.replace(
      RegExp(escape, 'gi'),
      tagStr.replace(/{tag}/gi, 'b')
    );
  }

  updateData() {
    const el = ReactDOM.findDOMNode(this.refs.text);
    el.innerHTML = this.getMarkStr();
  }

  render() {
    return (
      <span className='textHighlighting' ref='text'></span>
    );
  }
}

const styleString = 'font-weight: bold;';
