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
import { useIntl } from 'react-intl';

//@ts-ignore
import { CopyToClipboard } from 'dremio-ui-lib';
import BranchPicker from '@app/pages/HomePage/components/BranchPicker/BranchPicker';
import NessieBreadcrumb from '../NessieBreadcrumb/NessieBreadcrumb';
import { useNessieContext } from '../../utils/context';

import './PageBreadcrumbHeader.less';

function PageBreadcrumbHeader({
  path,
  rightContent,
  hasBranchPicker = true
}: {
  path?: string[];
  rightContent?: any;
  hasBranchPicker?: boolean;
}) {
  const intl = useIntl();
  const { source } = useNessieContext();

  return (
    <div className='pageBreadcrumbHeader'>
      <span className='pageBreadcrumbHeader-crumbContainer'>
        <NessieBreadcrumb path={path} />
        {hasBranchPicker && <BranchPicker />}
        <span className='pageBreadcrumbHeader-copyButton'>
          <CopyToClipboard
            tooltipText={intl.formatMessage({ id: 'Common.PathCopied' })}
            value={[source.name, ...(path || [])].join('.')}
          />
        </span>
      </span>
      <span className='pageBreadcrumbHeader-rightContent'>
        {rightContent}
      </span>
    </div>
  );
}
export default PageBreadcrumbHeader;
