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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { injectIntl } from 'react-intl';
import { Link } from 'react-router';
import Art from 'components/Art';
import EllipsedText from 'components/EllipsedText';
import jobsUtils from 'utils/jobsUtils';

import './ReflectionList.css';

@injectIntl
export default class ReflectionList extends PureComponent {
  static propTypes = {
    jobDetails: PropTypes.instanceOf(Immutable.Map),
    intl: PropTypes.object.isRequired,
    reflections: PropTypes.array.isRequired
  };

  render() {
    const items = this.props.reflections.map(({relationship, dataset, reflection, materialization}) => {
      const name = reflection.name || this.props.intl.formatMessage({ id: 'Reflection.UnnamedReflection' });

      let desc = '';
      if (relationship === 'CHOSEN') {
        if (materialization.refreshChainStartTime) { // protect against pre-1.3 materializations
          const age = jobsUtils.msToHHMMSS(this.props.jobDetails.get('startTime') - materialization.refreshChainStartTime);
          desc = this.props.intl.formatMessage({ id: 'Reflection.Age' }, {age});
        }
      } else if (relationship === 'MATCHED') {
        desc = this.props.intl.formatMessage({ id: 'Reflection.TooExpensive' });
      } else if (relationship === 'CONSIDERED') {
        desc = this.props.intl.formatMessage({ id: 'Reflection.DidNotCoverQuery' });
      }

      return <li>
        <div>
          <Art src={reflection.type === 'RAW' ? 'RawMode.svg' : 'Aggregate.svg'}
            style={{height: 24, marginRight: 5}}
            alt={this.props.intl.formatMessage({ id: reflection.type === 'RAW' ? 'Reflection.Raw' : 'Reflection.Aggregation' })}
            title />
          <div>
            <EllipsedText text={name}>
              <Link to={{
                ...location,
                state: {
                  modal: 'AccelerationModal',
                  accelerationId: dataset.id, // BE reuses the IDs
                  layoutId: reflection.id
                }
              }}>{name}</Link>
            </EllipsedText>
            <EllipsedText text={dataset.path.join('.')} />
          </div>
        </div>
        <div>
          {desc}
        </div>
      </li>;
    });

    return <ul className='reflection-list' children={items} />;
  }
}
