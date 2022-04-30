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
import CommitHash from '@app/pages/HomePage/components/BranchPicker/components/CommitBrowser/components/CommitHash/CommitHash';
import UserIcon from '@app/pages/HomePage/components/BranchPicker/components/CommitBrowser/components/UserIcon/UserIcon';
import { formatDate } from '@app/utils/date';
import { CommitMeta } from '@app/services/nessie/client';

import './CommitDetails.less';

function CommitDetails({ commitMeta, branch }: { commitMeta: CommitMeta, branch: string }) {
  const intl = useIntl();
  return (
    <span className='commitDetails'>
      <div className='commitDetails-header'>
        {commitMeta.hash && (
          <span className='commitDetails-hash'>
            <CommitHash branch={branch} hash={commitMeta.hash} />
          </span>
        )}
        <span className='commitDetails-desc text-ellipsis' title={commitMeta.message}>
          {commitMeta.message}
        </span>
      </div>
      <div className='commitDetails-content'>
        <span className='commitDetails-details'>
          {commitMeta.author && (
            <span className='commitDetails-metaSection'>
              <div className='commitDetails-metaHeader'>
                {intl.formatMessage({ id: 'Common.Author' })}
              </div>
              <div className='commitDetails-userInfo commitDetails-metaDetail' title={commitMeta.author}>
                <UserIcon user={commitMeta.author} />
                <span className='commitDetails-userName text-ellipsis'>{commitMeta.author}</span>
              </div>
            </span>
          )}
          {commitMeta.commitTime && (
            <span className='commitDetails-metaSection'>
              <div className='commitDetails-metaHeader'>
                {intl.formatMessage({ id: 'Common.CommitTime' })}
              </div>
              <div className='commitDetails-commitTime commitDetails-metaDetail'>
                {formatDate(commitMeta.commitTime + '', 'MM/DD/YYYY hh:mm A')}
              </div>
            </span>
          )}
        </span>
      </div>
    </span>
  );
}

export default CommitDetails;
