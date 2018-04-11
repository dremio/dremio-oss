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

const EmptyCard = () => {
  return (
    <div style={styles.emptyCard} data-qa='no-selection'>
      or, select text in the field to see more patterns
    </div>
  );
};

const styles = {
  emptyCard: {
    height: 150,
    width: 460,
    minWidth: 455,
    marginTop: 5,
    marginLeft: 10,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    position: 'relative',
    backgroundColor: '#fff',
    border: '1px solid #f2f2f2',
    cursor: 'pointer'
  }
};

export default EmptyCard;
