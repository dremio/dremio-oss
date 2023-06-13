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
const digitColumn = {
  textAlign: "right",
};

export const tableStyles = {
  hidden: {
    display: "none",
  },
  actionColumn: {
    height: "100%",
    display: "flex",
    alignItems: "center",
    justifyContent: "flex-end",
  },
  digitColumn,
  datasetsColumn: {
    ...digitColumn,
    paddingRight: 30,
  },
  searchField: {
    width: 240,
    marginRight: "6px",
  },
  closeIcon: {
    Icon: {
      width: 22,
      height: 22,
    },
    Container: {
      cursor: "pointer",
      position: "absolute",
      right: 3,
      top: 0,
      bottom: 0,
      margin: "auto",
      marginRight: "16px",
      width: 22,
      height: 22,
    },
  },
};
