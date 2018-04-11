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
import PropTypes from 'prop-types';
import FileUtils from 'utils/FileUtils';

// todo: ax
export default function Footprint(props) {
  const currentByteSize = props.currentByteSize || 0; // needs fallback, API can skip if NEW
  const currentSizeText = FileUtils.getFormattedBytes(currentByteSize);
  const totalSizeText = FileUtils.getFormattedBytes(props.totalByteSize || currentByteSize);
  return <span>
    <span title={la('Storage used for latest Reflection.')}>{currentSizeText}</span>
    {props.totalByteSize !== currentByteSize && <span>
      {' '}<span title={la('Total storage used (including expired data).')}>({totalSizeText})</span>
    </span>}
  </span>;
}

Footprint.propTypes = {
  totalByteSize: PropTypes.number,
  currentByteSize: PropTypes.number
};
