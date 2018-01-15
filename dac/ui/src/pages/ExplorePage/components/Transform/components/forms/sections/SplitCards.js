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
import { Component } from 'react';
import Immutable from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';
import { cardsWrap } from 'uiTheme/radium/exploreTransform';
import SplitCard from './SplitCard';
import EmptyCard from './EmptyCard';

@Radium
export default class SplitCards extends Component {

  static getFields() {
    return [
      ...SplitCard.getFields().map((field) => `cards[].${field}`),
      'activeCard',
      'transformType'
    ];
  }

  static validate(values) {
    const activeCardValues = values.cards && values.cards[values.activeCard];
    if (activeCardValues) {
      return {
        cards: {
          [values.activeCard]: SplitCard.validate(activeCardValues)
        }
      };
    }
  }

  static propTypes = {
    cards: PropTypes.instanceOf(Immutable.List),
    fields: PropTypes.object,
    hasSelection: PropTypes.bool
  };

  handleCardClick(index) {
    const { fields: { activeCard }} = this.props;
    if (index !== activeCard.value) {
      activeCard.onChange(index);
    }
  }

  render() {
    const { cards, fields, hasSelection } = this.props;
    return <div className='transform-selection' style={cardsWrap}>
      {fields.cards.map((card, index) =>
        <SplitCard
          key={index}
          card={cards.get(index) || Immutable.Map()}
          fields={card}
          active={index === fields.activeCard.value}
          onClick={this.handleCardClick.bind(this, index)}
        />
      )}
      {!hasSelection ? <EmptyCard /> : null}
    </div>;
  }
}
