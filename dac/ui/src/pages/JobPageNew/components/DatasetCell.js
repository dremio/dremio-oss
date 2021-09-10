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
import { useState, useRef, useEffect } from 'react';
import PropTypes from 'prop-types';

import Art from '@app/components/Art';
import { Tooltip } from '@app/components/Tooltip';
import FontIcon from 'components/Icon/FontIcon';
import { getIconByEntityType } from 'utils/iconUtils';

import './JobsContent.less';

const DatasetCell = ({ job }) => {
  const [tooltipOpen, setTooltipOpen] = useState(false);

  const TooltipInnerStyle = {
    width: 'auto',
    maxWidth: '450px',
    background: '#F5FCFF',
    border: '1.5px solid #31D3DB',
    padding: '0px 8px 12px 8px'
  };

  useEffect(() => {
    const timer = setTimeout(() => setTooltipOpen(false), 3000);
    return () => clearTimeout(timer);
  }, [tooltipOpen]);

  const handleMouseEnter = () => {
    setTooltipOpen(true);
  };

  const handleMouseLeave = () => {
    setTooltipOpen(false);
  };

  const datasetRef = useRef(null);
  const datasetArray = job.get('queriedDatasets');
  const isAcceleration = job.get('accelerated');
  const isStarFlakeAccelerated = job.get('starFlakeAccelerated');
  const isInternalQuery = job.get('queryType') && job.get('queryType') === 'UI_INITIAL_PREVIEW';

  return (
    datasetArray && <div
      className='jobsContent-dataset'
      ref={datasetRef}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      data-qa='dataSetCell'
    >
      <span className='jobsContent-dataset__accelerationIconWrapper'>
        {isStarFlakeAccelerated && <Art
          src='StarFlakeAccelerated.svg'
          alt='icon'
          title='icon'
          className='jobsContent-dataset__accelerationIcon'
        /> ||
        isAcceleration && <Art
          src='Reflection.svg'
          alt='icon'
          title='icon'
          className='jobsContent-dataset__accelerationIcon'
        />}
      </span>
      <span className='jobsContent-dataset__dataset'>
        <FontIcon
          type={getIconByEntityType(isInternalQuery ? 'OTHER' : datasetArray.getIn([0, 'datasetType']))}
          iconStyle={{
            verticalAlign: 'middle',
            flexShrink: 0
          }}
        />
      </span>
      <div className='jobsContent-dataset__name'>
        {datasetArray.getIn([0, 'datasetName'])}
      </div>
      <Tooltip
        key='tooltip'
        target={() => (tooltipOpen ? datasetRef.current : null)}
        placement='bottom-start'
        type='custom'
        className='jobsContent-dataset__tooltip'
        tooltipArrowClass='textWithHelp__tooltipArrow --light'
        tooltipInnerStyle={TooltipInnerStyle}
      >
        {datasetArray.map((dataset, index) => {
          const datasetName = dataset.get('datasetName');
          const datasetPath = dataset.get('datasetPath');
          const queryText = job.get('queryText');
          const description = job.get('description');
          const datasetDescription = !queryText || queryText === 'NA'
            ? description : datasetPath;
          return (
            <div key={`datasetCell-${index}`} className='jobsContent-dataset__tooltipWrapper'>
              <FontIcon
                type={getIconByEntityType(datasetArray.getIn([0, 'datasetType']))}
                iconStyle={{
                  verticalAlign: 'middle',
                  flexShrink: 0
                }}
              />
              <div className='jobsContent-dataset__nameWrapper'>
                <div className='jobsContent-dataset__label'>{datasetName}</div>
                <div className='jobsContent-dataset__path'>{datasetDescription}</div>
              </div>
            </div>
          );
        })}
      </Tooltip>
    </div>
  );
};

DatasetCell.propTypes = {
  job: PropTypes.object
};

DatasetCell.defaultProps = {
  job: null
};

export default DatasetCell;
