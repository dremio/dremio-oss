/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import jobsUtils from '@app/utils/jobsUtils';
import PropTypes from 'prop-types';
import { injectIntl } from 'react-intl';
import Immutable from 'immutable';
import Art from '@app/components/Art';
import timeUtils from 'utils/timeUtils';

import './Reflection.less';


const renderIcon = (iconName, className) => {
  return (<Art src={iconName} alt='Reflection Icon' title='Reflection' className={className} />);
};

const Reflection = (props) => {
  const {
    reflectionsUsed,
    reflectionsNotUsed,
    intl: {
      formatMessage
    },
    isAcceleration,
    location
  } = props;

  const getReflectionIcon = (isStarFlake, reflectionType, isReflectionUsed) => {
    if (isStarFlake) {
      const starflakeIcon = isReflectionUsed ? 'StarflakeUsed.svg' : 'StarflakeNotUsed.svg';
      const starflakeClassName = isReflectionUsed ? 'starflakeUsed' : 'starflakeNotUsed';
      return renderIcon(starflakeIcon, starflakeClassName);
    } else if (isReflectionUsed) {
      const reflectionUsedIcon = reflectionType === 'RAW'
        ? 'Reflection.svg' : 'ReflectionsUsedAgg.svg';
      return renderIcon(reflectionUsedIcon, 'reflectionIcon');
    } else if (!isReflectionUsed) {
      const reflectionNotUsedIcon = reflectionType === 'RAW'
        ? 'ReflectionsNotUsedRaw.svg' : 'ReflectionsNotUsedAgg.svg';
      const reflectionUsedClass = reflectionType === 'RAW' ? 'reflectionNotUsed' : 'reflectionIcon';
      return renderIcon(reflectionNotUsedIcon, reflectionUsedClass);
    }
  };
  const isReflectionsShown = reflectionsUsed.size > 0 || reflectionsNotUsed.size > 0;
  return (
    <div>
      {
        isReflectionsShown &&
        <div className='reflection'>
          <div className='reflection-header'>
            <div className='reflection-header__title'>
              {formatMessage({ id: 'Acceleration' })}
            </div>
            {isAcceleration
              &&
              <span className='reflection-header__iconWrapper'>
                {renderIcon('Reflection.svg', 'reflectionIcon')}
                <span>
                  {formatMessage({ id: 'Reflections.Query_Accelerated' })}
                </span>
              </span>
            }
          </div>
          <div>
            <div className='reflection-content__title'>
              {formatMessage({ id: 'Reflections.Used' })}
            </div>
            {reflectionsUsed && reflectionsUsed.map((item, index) => {
              const reflectionType = item.get('reflectionType');
              const isStarFlake = item.get('isStarFlake');
              return (<div
                key={`reflectionUsed-${index}`}
                className='reflection-content__rowWrapper'
                data-qa='reflectionUsedTestCase'>
                {
                  getReflectionIcon(isStarFlake, reflectionType, true)
                }
                <div className='reflection-content__dataWrapper'>
                  <span className='reflection-content__dataHeader'>
                    <div className='reflection-content__dataLabel'>
                      { jobsUtils.getReflectionsLink(item, location) }
                      <span className='reflection-content__dataLabel__algbric'>
                        {
                          item.get('reflectionMatchingType') === 'ALGEBRAIC'
                          &&
                          '[AlgMatch]'
                        }
                      </span>
                    </div>
                    <div className='reflection-content__dataLabelSubscription'>
                      {item.get('reflectionDatasetPath')}
                    </div>
                  </span>
                  <span className='reflection-content__dataHeaderContent'>
                    {formatMessage({ id: 'Reflections.Age' })}
                    {timeUtils.toNow(Number(item.get('reflectionCreated')))}
                  </span>
                </div>
              </div>
              );
            })
            }
          </div>
          <div>
            <div className='reflection-content__title'>
              {formatMessage({ id: 'Reflections.NotUsed' })}
            </div>
            {reflectionsNotUsed && reflectionsNotUsed.map((item, index) => {
              const reflectionType = item.get('reflectionType');
              const isStarFlake = item.get('isStarFlake');
              return (<div
                key={`reflectionsNotUsed-${index}`}
                className='reflection-content__rowWrapper'
                data-qa='reflectionNotUsedTestCase'>
                {
                  getReflectionIcon(isStarFlake, reflectionType, false)
                }
                <div className='reflection-content__dataWrapper'>
                  <span className='reflection-content__dataHeader'>
                    <div className='reflection-content__dataLabel'>
                      { jobsUtils.getReflectionsLink(item, location) }
                      <span className='reflection-content__dataLabel__algbric'>
                        {
                          item.get('reflectionMatchingType') === 'ALGEBRAIC'
                          &&
                          '[AlgMatch]'
                        }
                      </span>
                    </div>
                    <div className='reflection-content__dataLabelSubscription'>
                      {item.get('reflectionDatasetPath')}
                    </div>
                  </span>
                  <span className='reflection-content__dataHeaderContent'>
                    {formatMessage({ id: 'Reflections.DidNotCoverQuery' })}
                  </span>
                </div>
              </div>
              );
            })
            }
          </div>
        </div>
      }
    </div>
  );
};

Reflection.propTypes = {
  intl: PropTypes.object.isRequired,
  reflectionsUsed: PropTypes.instanceOf(Immutable.List),
  reflectionsNotUsed: PropTypes.instanceOf(Immutable.List),
  isAcceleration: PropTypes.bool,
  reflections: PropTypes.instanceOf(Immutable.List),
  location: PropTypes.object
};
export default injectIntl(Reflection);
