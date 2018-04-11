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
import Tabs from 'components/Tabs';
import { Radio } from 'components/Fields';

import { formLabel, fixedWidthBold } from 'uiTheme/radium/typography';
import { inlineLabel, inlineFieldWrap } from 'uiTheme/radium/exploreTransform';

import ExtractPosition from './../../cardContent/ExtractPosition';
import ExtractPattern from './../../cardContent/ExtractPattern';
import CardContentView from './../../cardContent/CardContentView';

import TransformCard from './../../TransformCard';
import { styles } from './ExtractListCard';

@Radium
export default class ExtractTextCard extends Component {

  static getFields() {
    return [
      ...ExtractPosition.getFields(),
      ...ExtractPattern.getFields(),
      'type'
    ];
  }

  static propTypes = {
    columnName: PropTypes.string,
    card: PropTypes.instanceOf(Immutable.Map),
    fields: PropTypes.object,
    active: PropTypes.bool,
    onClick: PropTypes.func
  };

  static validate(values) {
    if (values.type === 'position') {
      return ExtractPosition.validate(values);
    } else if (values.type === 'pattern') {
      return ExtractPattern.validate(values);
    }
    return {
      type: 'Type is required.'
    };
  }

  constructor(props) {
    super(props);
  }

  renderFront() {
    const { card } = this.props;
    if (!card) {
      return <div/>;
    }

    return (
      <div style={styles.cardStyle}>
        <div style={styles.header}>
          <span style={fixedWidthBold} data-qa='card-title'>On: {card.get('description')}</span>
        </div>
        <CardContentView
          data={card.get('examplesList') || Immutable.List()}
          isInProgress={card.get('isInProgress')}
        />
      </div>
    );
  }

  renderBack() {
    const {fields, columnName} = this.props;
    return (
      <div>
        <div style={inlineFieldWrap}>
          <span style={inlineLabel}>
            Extract By
          </span>
          <Radio
            {...fields.type}
            radioValue='position'
            label='Position'
            style={{...formLabel, ...styles.radio}}/>
          <Radio
            {...fields.type}
            radioValue='pattern'
            label='Pattern'
            style={{...formLabel, ...styles.radio}}/>
        </div>
        <div className='transform-card-content'>
          <Tabs activeTab={fields.type.value}>
            <ExtractPosition
              tabId='position'
              firstElementLabel='First Character Position'
              lastElementLabel='Last Character Position'
              fields={fields}/>
            <ExtractPattern tabId='pattern' columnName={columnName} fields={fields}/>
          </Tabs>
        </div>
      </div>
    );
  }

  render() {
    const {card, active, onClick} = this.props;

    return (
      <TransformCard
        front={this.renderFront()}
        back={this.renderBack()}
        card={card}
        active={active}
        onClick={onClick}/>
    );
  }
}
