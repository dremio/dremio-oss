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
import { applyValidators, isRequired } from 'utils/validation';
import ExtractMap from './../../cardContent/ExtractMap';
import TransformCard from './../../TransformCard';
import { styles } from './ExtractListCard';

@Radium
export default class ExtractMapCard extends Component {

  static getFields() {
    return ['path', 'type'];
  }

  static validate = (values) => applyValidators(values, [isRequired(`cards[${values.activeCard}].path`)]);

  static propTypes = {
    card: PropTypes.instanceOf(Immutable.Map),
    fields: PropTypes.object,
    active: PropTypes.bool,
    onClick: PropTypes.func
  };

  renderBack() {
    const { fields } = this.props;
    return (
      <div>
        <div style={styles.title}>{la('Path:')}</div>
        <div className='transform-card-content'>
          <ExtractMap path={fields.path}/>
        </div>
      </div>
    );
  }

  render() {
    const { card, active, onClick } = this.props;
    return (
      <TransformCard
        back={this.renderBack()}
        card={card}
        active={active}
        onClick={onClick}
        editing
      />
    );
  }
}
