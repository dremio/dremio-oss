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

export default class RowsCut extends Component {
  static propTypes = {
    maxItemsCount: PropTypes.number,
    data: PropTypes.instanceOf(Immutable.List)
  };

  constructor(props) {
    super(props);
    const hiddenCount = this.props.data.size - this.props.maxItemsCount;
    const showText = `${hiddenCount} more...`;

    this.state = {
      isOpen: false,
      hiddenCount,
      text: showText
    };
  }

  checkHiddenItems() {
    this.setState({
      isOpen: !this.state.isOpen,
      text: this.state.isOpen ? `${this.state.hiddenCount} more...` : 'hide more'
    });
  }

  checkLengthData(data) {
    const maxItemsCount = this.props.maxItemsCount;
    let element = null;
    if (data.size <= maxItemsCount) {
      element = data.map(function(item) {
        return <div key={item}>{item}</div>;
      });
    } else {
      return (
        element =
          <span className='holder' className={this.state.isOpen ? 'open-more' : 'close-more'}>
            <span className='visible-items'>
              {data.map(function(item, index) {
                if (index <= maxItemsCount - 1) {
                  return (
                    <div key={item}>
                      {item}
                    </div>
                  );
                }
              })}
            </span>
            <span
              className='show-more-btn'
              onClick={this.checkHiddenItems.bind(this)}>
              {this.state.text}
            </span>

            <span className={this.state.isOpen ? 'show' : 'hide'}>
              {data.map(function(item, index) {
                if (index > maxItemsCount - 1) {
                  return (
                    <div key={item}>
                      {item}
                    </div>
                  );
                }
              })}
            </span>
          </span>
      );
    }
    return element;
  }

  render() {
    return (
      <div>{this.checkLengthData(this.props.data)}</div>
    );
  }
}
