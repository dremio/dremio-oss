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
import { useState } from 'react';
import { injectIntl } from 'react-intl';
import PropTypes from 'prop-types';
import { Label } from 'dremio-ui-lib';
import jobsUtils from 'utils/jobsUtils';
import FontIcon from 'components/Icon/FontIcon';
import Art from '@app/components/Art';
import TextWithHelp from '@app/components/TextWithHelp';
import { getIconByEntityType } from 'utils/iconUtils';
import './Scans.less';

const renderScanTooltip = (tooltip, scanName) => {
  return tooltip ? <TextWithHelp
    text={tooltip}
    helpText={scanName}
    showToolTip
    color='light'
  /> : scanName;
};

const renderIcon = (iconName, className) => {
  return <Art src={iconName} alt='icon' title='icon' className={className} />;
};
const ScanItem = ({ scan, scansForFilter, intl: { formatMessage } }) => {
  const [isScanOpen, setIsScanOpen] = useState(false);
  const collapseIconUsed = isScanOpen ? 'DownArrow.svg' : 'Right_Arrow.svg';
  const dataSetType = scan.get('datasetType');
  return (
    <div className='scans-content'>
      <div className='scans-content__header'>
        <span
          data-qa='dropdown-customer'
          onClick={() => setIsScanOpen(!isScanOpen)}
          className='scans-content__expandIcon'
        >
          {renderIcon(collapseIconUsed, 'scans-content__dropdownIcon')}
        </span>
        {dataSetType === 'REFLECTION'
          ? renderIcon('Reflection.svg',
            'scans-content__reflectionIcon')
          :
          <span className='margin-top--half'>
            <FontIcon
              type={getIconByEntityType(dataSetType)}
              iconStyle={{ height: '28px', width: '28px' }}
            />
          </span>}
        <span className='scans-content__dataLabel'>
          {renderScanTooltip(scan.get('description'), scan.get('name'))}
        </span>
      </div>
      {isScanOpen && (
        <div className='gutter-left'>
          {scansForFilter.map((item, scanIndex) => {
            const labelValue = item.key === 'nrScannedRows'
              ? jobsUtils.getFormattedNumber(scan.get(item.key))
              : scan.get(item.key);
            return (
              <div
                key={`scansForFilter-${scanIndex}`}
                className='scans-content__dataWrapper'
              >
                <Label
                  value={formatMessage({ id: item.label })}
                  className='scans-content__dataLabelHeader'
                />
                <span className='scans-content__dataContent'>
                  {labelValue}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};

ScanItem.propTypes = {
  scansForFilter: PropTypes.array,
  scan: PropTypes.object,
  intl: PropTypes.object.isRequired
};
export default injectIntl(ScanItem);
