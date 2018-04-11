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
import { Link } from 'react-router';
import { FormattedMessage } from 'react-intl';

import { abilities } from 'utils/datasetUtils';

export default function(input) {
  Object.assign(input.prototype, { // eslint-disable-line no-restricted-properties
    renderMoveLink() {
      const { entity, location } = this.props;
      const moveLink = {
        ...location,
        state: {
          modal: 'UpdateDataset',
          item: entity,
          query: {
            mode: 'move'
          }
        }
      };

      const { canMove } = abilities(entity, entity.get('entityType'));

      if (!canMove) {
        return null;
      }

      return (
        <Link to={moveLink} style={{marginTop: 15, display: 'block'}}>
          <FormattedMessage id = 'Common.Move' />
        </Link>
      );
    }
  });
}
