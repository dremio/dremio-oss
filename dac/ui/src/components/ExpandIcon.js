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
import Art from 'components/Art';
import { formatMessage } from '../utils/locale';

const ARROW_DOWN = 'ArrowDownSmall';


const ExpandIcon = ({ expanded }) => {
  const topArrowStyle = expanded ? {
    ...styles.transform,
    bottom: -4
  } : {};
  const bottomArrowStyle = !expanded ? {
    bottom: -4
  } : styles.transform;
  return (
    <div style={styles.wrapper} role='img' aria-label={formatMessage(`Common.${expanded ? 'Collapse' : 'Expand'}`)}>
      <Art src={`${ARROW_DOWN}.svg`} alt={''} style={{ ...styles.base, ...topArrowStyle }}/>
      <Art src={`${ARROW_DOWN}.svg`} alt={''} style={{ ...styles.base, ...bottomArrowStyle }} />
    </div>
  );
};

ExpandIcon.propTypes = {
  expanded: PropTypes.bool
};

const styles = {
  wrapper: {
    width: 24,
    height: 24,
    position: 'relative'
  },
  base: {
    position: 'absolute'
  },
  transform: {
    transform: 'rotate(180deg)'
  }
};

export default ExpandIcon;
