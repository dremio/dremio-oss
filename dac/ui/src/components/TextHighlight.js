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
import { PureComponent } from 'react';

import PropTypes from 'prop-types';

export default class TextHighlight extends PureComponent {

  static propTypes = {
    text: PropTypes.string,
    inputValue: PropTypes.string
  };

  render() {
    const inputValue = this.props.inputValue || '';
    const text = this.props.text || '';

    const nodes = [];
    if (!inputValue) {
      nodes.push(text);
    } else {
      let lastNodeIsString = false;
      for (let i = 0; i < text.length; i++) {
        const sub = text.substr(i, inputValue.length);
        if (sub.toLowerCase() === inputValue.toLowerCase()) {
          i += inputValue.length - 1;
          nodes.push(<b key={i}>{sub}</b>);
          lastNodeIsString = false;
        } else if (lastNodeIsString) {
          nodes[nodes.length - 1] += text[i];
        } else {
          nodes.push(text[i]);
          lastNodeIsString = true;
        }
      }
    }

    return <span className='TextHighlight'>{nodes}</span>;
  }
}
