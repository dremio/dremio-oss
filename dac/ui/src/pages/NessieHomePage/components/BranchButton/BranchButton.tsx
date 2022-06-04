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
import { Button } from '@material-ui/core';
import { FormattedMessage } from 'react-intl';

import FontIcon from '@app/components/Icon/FontIcon';

import './BranchButton.less';

const iconStyle = { width: '16px', height: '16px' };

function BranchButton({
  onClick,
  text,
  iconType = 'NewBranch'
}: {
  iconType?: string;
  text?: any;
  onClick: () => void;
}) {
  return (
    <span className='branch-button'>
      <Button
        variant='outlined'
        size='small'
        startIcon={<FontIcon type={iconType} theme={{ Icon: iconStyle }} />}
        onClick={onClick}
      >
        <span className='branch-button-text'>
          {text || <FormattedMessage id='RepoView.CreateBranch' />}
        </span>
      </Button>
    </span>
  );
}

export default BranchButton;
