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
import ExtractMapCard from './ExtractMapCard';

@Radium
export default class ExtractMapCards extends Component {

  static getFields() {
    return [
      ...ExtractMapCard.getFields().map((field) => `cards[].${field}`),
      'activeCard'
    ];
  }

  static validate = (values) => ({
    ...ExtractMapCard.validate(values)
  });

  static propTypes = {
    cards: PropTypes.instanceOf(Immutable.List),
    fields: PropTypes.object
  };

  handleCardClick(index) {
    const {fields: {activeCard}} = this.props;
    if (index !== activeCard.value) {
      activeCard.onChange(index);
    }
  }

  render() {
    const {cards, fields} = this.props;
    return <Cards>
      {fields.cards.map((card, index) =>
        <ExtractMapCard
          key={index}
          card={cards.get(index) || Immutable.Map()}
          fields={card}
          active={index === fields.activeCard.value}
          onClick={this.handleCardClick.bind(this, index)}/>
      )}
    </Cards>;

  }
}
