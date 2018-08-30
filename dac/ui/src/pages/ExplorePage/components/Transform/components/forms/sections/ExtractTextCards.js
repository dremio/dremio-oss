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
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import Cards from '@app/pages/ExplorePage/components/Transform/components/Cards';
import ExtractTextCard from './ExtractTextCard';
import EmptyCard from './EmptyCard';

@Radium
export default class ExtractTextCards extends Component {

  static getFields() {
    return [
      ...ExtractTextCard.getFields().map((field) => `cards[].${field}`),
      'activeCard'
    ];
  }

  static validate(values) {
    const activeCardValues = values.cards && values.cards[values.activeCard];
    if (activeCardValues) {
      return {
        cards: {
          [values.activeCard]: ExtractTextCard.validate(activeCardValues)
        }
      };
    }
  }

  static propTypes = {
    columnName: PropTypes.string,
    cards: PropTypes.instanceOf(Immutable.List),
    fields: PropTypes.object,
    hasSelection: PropTypes.bool
  };

  handleCardClick(index) {
    const {fields: {activeCard}} = this.props;
    if (index !== activeCard.value) {
      activeCard.onChange(index);
    }
  }

  render() {
    const {cards, fields, columnName, hasSelection} = this.props;
    return <Cards>
      {fields.cards.map((cardFields, index) =>
        <ExtractTextCard
          key={index}
          columnName={columnName}
          card={cards.get(index) || Immutable.Map()}
          fields={cardFields}
          active={index === fields.activeCard.value}
          onClick={this.handleCardClick.bind(this, index)}
        />
      )}
      {!hasSelection ? <EmptyCard /> : null}
    </Cards>;
  }
}
