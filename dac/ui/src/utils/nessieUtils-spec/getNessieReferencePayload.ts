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
import { NESSIE_REF_PREFIX } from "#oss/constants/nessie";
import { initialState } from "#oss/reducers/nessie/nessie";
import { NessieRootState, Reference } from "#oss/types/nessie";
export const init = {
  dataplane: {
    ...initialState,
  },
};
export const initRef = {
  [`${NESSIE_REF_PREFIX}dataplane`]: {
    ...initialState,
  },
};

export const nullStates = {
  dataplane: null,
  [`${NESSIE_REF_PREFIX}dataplane`]: null,
};

export const noRef: NessieRootState = { ...init };
export const empty: NessieRootState = { ...init, ...initRef };

const extraState = {
  defaultReference: {
    type: "BRANCH",
    name: "main",
    hash: "234e3e9145a9bee506c9a521096237b40b2364461b390de59e71d5d8d7d728ca",
  } as Reference,
  date: null,
  loading: {
    NESSIE_DEFAULT_REF_REQUEST: false,
  },
  errors: {},
};

export const nessieState: NessieRootState = {
  "ref/dataplane": {
    reference: {
      type: "BRANCH",
      name: "test3",
      hash: "bde23e95962a19c935594bebd084aeec1efdb46b3343f964cdcfd2e4944bd864",
    },
    hash: null,
    ...extraState,
  },
  "ref/dataplane2": {
    reference: {
      type: "BRANCH",
      name: "aaaaa",
      hash: "2867b05cd832839c9536aa58b2152bf541a00099b48244479e95fbb83ee5a228",
    },
    hash: "2867b05cd832839c9536aa58b2152bf541a00099b48244479e95fbb83ee5a228",
    ...extraState,
  },
  "ref/ref/dataplane3": {
    reference: {
      type: "TAG",
      name: "aaaaa",
      hash: "2867b05cd832839c9536aa58b2152bf541a00099b48244479e95fbb83ee5a228",
    },
    hash: null,
    ...extraState,
  },
  "ref/": {
    reference: {
      type: "BRANCH",
      name: "bbbb",
      hash: "2867b05cd832839c9536aa58b2152bf541a00099b48244479e95fbb83ee5a228",
    },
    hash: null,
    ...extraState,
  },
};
