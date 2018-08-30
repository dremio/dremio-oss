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
import React, { Component } from 'react';
import spring from 'react-motion/lib/spring';
import Radium from 'radium';
import { injectIntl } from 'react-intl';

import PropTypes from 'prop-types';

import Art from 'components/Art';
import { formatMessage } from '../../utils/locale';

@injectIntl
@Radium
export class AddButton extends Component {
  static propTypes = {
    addItem: PropTypes.func,
    style: PropTypes.object,
    children: PropTypes.node,
    intl: PropTypes.object.isRequired
  };

  render() {
    const {addItem, style, children} = this.props;
    const isHovered = Radium.getState(this.state, 'addItem', ':hover');
    const combinedStyle = {':hover': {}, ...styles.addButton, ...style}; // need Radium fakeout
    return <a key='addItem' className='add-item' onClick={addItem} style={combinedStyle}>
      <Art
        src={`${isHovered ? 'AddHover' : 'Add'}.svg`}
        alt={this.props.intl.formatMessage({id: 'Common.Add'})}
        style={styles.addIcon} />
      {children}
    </a>;
  }
}

RemoveButton.propTypes = {
  onClick: PropTypes.func,
  style: PropTypes.object
};

export function RemoveButton({onClick, style}) {
  return <span data-qa='remove-button'><Art
    src={'XSmall.svg'}
    alt={formatMessage('Common.Close')}
    style={{...styles.removeButton, ...style}}
    onClick={onClick}
  /></span>;
}


const buttonInlineMargin = 0;
const buttonSize = 24;
//todo move it to some common styles
export const RemoveButtonStyles = {
  inline: { // add this style, when remove button is added to a row of elements
    marginRight: -(buttonSize + buttonInlineMargin), // apply negative margin to not break vertical alignment of a last row element with other element on other rows
    marginLeft: buttonInlineMargin
  }
};

export default class FieldList extends Component {

  static propTypes = {
    items: PropTypes.array,
    itemHeight: PropTypes.number,
    getKey: PropTypes.func,
    minItems: PropTypes.number,
    children: PropTypes.node,
    style: PropTypes.object,
    emptyLabel: PropTypes.string,
    className: PropTypes.string,
    listContainer: PropTypes.node,
    propName: PropTypes.string
  };

  static defaultProps = {
    emptyLabel: 'No Items' // todo: loc
  };

  //
  // Animation
  //

  // todo: chris curious what uses these. also why they return more than just style info
  getDefaultStyles = () => {
    const {items, itemHeight, getKey} = this.props;
    return items.map((item) => ({key: getKey(item), data: item, style: {height: itemHeight}, opacity: 1}));
  };
  getStyles = () => {
    const {items, itemHeight, getKey} = this.props;
    return items.map((item) => ({
      key: getKey(item),
      data: item,
      style: {
        height: spring(itemHeight),
        opacity: spring(1)
      }
    }));
  };

  canRemove() {
    const {items, minItems} = this.props;
    return !minItems || items.length > minItems;
  }

  removeItem(index, e) {
    e.preventDefault();
    if (index < this.props.items.length) {
      this.props.items.removeField(index);
    }
  }

  willEnter() {
    return {
      height: 0,
      opacity: 0
    };
  }

  willLeave() {
    return {
      height: spring(0),
      opacity: spring(0)
    };
  }

  render() {
    const {emptyLabel, children, listContainer, propName} = this.props;

    // React doesn't like rendering empty objects ({}) so use null to signify no children
    let childNodes = null;

    if (this.props.items) {
      childNodes = this.props.items.map((data, index) => {
        return React.cloneElement(React.Children.only(children), {
          key: index,
          item: data,
          onRemove: this.canRemove() ? this.removeItem.bind(this, index) : undefined
        });
      });
    }

    if (listContainer) {
      childNodes = React.cloneElement(listContainer, {}, childNodes);
    }

    return (
      <div style={this.props.style} className={`field ${this.props.className}`} data-qa={propName}>
        {(!this.props.items || this.props.items.length === 0) &&
          <div style={styles.empty}>
            {emptyLabel}
          </div>
        }
        {childNodes}
      </div>
    );
  }
}

const styles = {
  addButton: {
    paddingTop: 10,
    marginTop: -5,
    marginBottom: 10,
    display: 'block',
    cursor: 'pointer',
    fontWeight: 400,
    fontSize: 12,
    color: '#999999'
  },
  addIcon: {
    marginBottom: -6,
    width: 20,
    height: 20
  },
  removeButton: {
    color: '#999',
    fontSize: '10px',
    cursor: 'pointer',
    width: buttonSize,
    height: buttonSize
  },
  empty: {
    color: '#ccc',
    fontSize: 14,
    marginBottom: 5
  }
};
