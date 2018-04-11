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
import { fixedWidthBold, formLabel } from 'uiTheme/radium/typography';

import ReplacePattern from './../../cardContent/ReplacePattern';
import CardContentView from './../../cardContent/CardContentView';
import TransformCard from './../../TransformCard';
import { styles } from './ExtractListCard';

@Radium
export default class ReplacePatternCard extends Component {

  static getFields() {
    return [
      ...ReplacePattern.getFields(),
      'type'
    ];
  }

  static validate(values) {
    return ReplacePattern.validate(values);
  }

  static propTypes = {
    card: PropTypes.instanceOf(Immutable.Map),
    fields: PropTypes.object,
    active: PropTypes.bool,
    onClick: PropTypes.func
  };

  renderFront() {
    const {card} = this.props;
    return (
      <div style={styles.cardStyle}>
        <div style={styles.header}>
          <span style={fixedWidthBold}>On: {card.get('description')}</span>
        </div>
        <CardContentView
          data={card.get('examplesList') || Immutable.List()}
          isInProgress={card.get('isInProgress')}
        />
      </div>
    );
  }

  renderBack() {
    const {fields} = this.props;
    return (
      <div className='transform-card-content'>
        <div style={[styles.header, formLabel]}>Edit Selection</div>
        <ReplacePattern fields={fields}/>
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
