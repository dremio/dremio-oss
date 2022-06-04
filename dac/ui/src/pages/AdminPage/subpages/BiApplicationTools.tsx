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
import Art from '@app/components/Art';
import { CLIENT_TOOL_ID } from '@app/constants/Constants';
import { useIntl } from 'react-intl';
import './BiApplicationTools.less';

export const RESERVED = [
  CLIENT_TOOL_ID.powerbi,
  CLIENT_TOOL_ID.tableau,
  CLIENT_TOOL_ID.qlik
];

type RenderToolProps = {
  id: string,
  name: string,
  icon: string,
  alt: string,
  renderSettings: Function
};

const iconStyle = { width: '26.7px', height: '26.7px'};
const generalInfoIconStyle = { width: '24px', height: '24px', paddingLeft: '5px', paddingTop: '2px'};

const RenderTool = ({id, name, icon, alt, renderSettings}: RenderToolProps) => {
  return (
    <div className='tool-wrap'>
      <div className='icon-cell'>
        <Art src={icon} alt={name} style={iconStyle} />
      </div>
      <div className='tool-name'>{name}</div>
      <Art src='GeneralInfo.svg' style={generalInfoIconStyle} alt={alt} title={alt} />
      <div className='action-cell'>
        {renderSettings(id, {showLabel: false})}
      </div>
    </div>
  );
};

type BiApplicationToolsProps = {
  renderSettings: Function
}

const BiApplicationTools = ({renderSettings}: BiApplicationToolsProps) => {
  const { formatMessage } = useIntl();
  return (
    <div className='biApplication-tools'>
      <div className='header'>
        {formatMessage({id: 'Admin.Project.BIApplications.Header'})}
      </div>
      <div className='description'>
        {formatMessage({id: 'Admin.Project.BIApplications.Description'})}
      </div>
      <div className='tools-table'>
        <div className='table-right-header'>{formatMessage({id: 'Common.Enabled'})}</div>
        <RenderTool
          id={CLIENT_TOOL_ID.tableau}
          name='Tableau Desktop'
          icon='Tableau.svg'
          alt='Enable this option to allow users to click a button to download the connection metadata for a dataset as a TDS file. The button displays the Tableau logo.'
          renderSettings={renderSettings}
        />
        <RenderTool
          id={CLIENT_TOOL_ID.powerbi}
          name='Microsoft Power BI Desktop'
          icon='PowerBi.svg'
          alt='Enable this option to allow users to click a button to download the connection metadata for a dataset as a PBIDS file. The button displays the Power BI logo.'
          renderSettings={renderSettings}
        />
      </div>
    </div>
  );
};


export default BiApplicationTools;