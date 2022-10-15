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

import * as classes from "./NodeTableCellStatus.module.less";

const NodeTableCellStatus = (props: { icon: string }) => {
  const { icon } = props;

  // use the icon name to determine which 'color' to use (running / stopped / disabled)
  return (
    <div className={classes["nodeTableCellStatus"]}>
      <dremio-icon
        name={icon}
        alt={icon}
        class={`${classes["nodeTableCellIcon"]} ${classes[icon.split("/")[1]]}`}
      />
    </div>
  );
};

export default NodeTableCellStatus;
