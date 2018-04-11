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
import { applyValidators, isRequired, isRegularExpression } from 'utils/validation';
import { fixedWidthBold } from 'uiTheme/radium/typography';

import SplitContent from './../../cardContent/SplitContent';
import CardContentView from './../../cardContent/CardContentView';
import TransformCard from './../../TransformCard';
import { styles } from './ExtractListCard';

@Radium
export default class SplitCard extends Component {
  static getFields() {
    return [
      'rule.ignoreCase',
      'rule.pattern',
      'rule.matchType',
      'type'
    ];
  }

  static validate(values) {
    const validators = [isRequired('rule.pattern', 'Pattern')];
    if (values.rule.matchType === 'regex') {
      validators.push(isRegularExpression('rule.pattern'));
    }
    return applyValidators(values, validators);
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
      <div>
        <div style={[styles.title, {marginBottom: 0}]}>{la('Edit Selection')}</div>
        <div className='transform-card-content'>
          <SplitContent
            pattern={fields.rule.pattern}
            ignoreCase={fields.rule.ignoreCase}
            matchType={fields.rule.matchType}/>
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
