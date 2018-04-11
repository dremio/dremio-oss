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
import FontIcon from 'components/Icon/FontIcon';
import { FormattedMessage } from 'react-intl';

const SampleDataMessage = () => (
  <span style={styles.warntext} data-qa='sample-data-message'>
    <FontIcon type='WarningSolid' style={styles.iconStyle} />
    <FormattedMessage id='Dataset.SampleDatasetWarn'/>
  </span>
);

export default SampleDataMessage;

const styles = {
  warntext: {
    display: 'inline-flex',
    alignItems: 'center',
    userSelect: 'text'
  },
  iconStyle: {
    height: 24
  }
};
