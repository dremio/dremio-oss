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
import { INIT_REFS, SET_REFS } from "#oss/actions/nessie/nessie";
import { expect } from "chai";
import nessieReducer from "./nessie";
import {
  testStateEmpty,
  empty,
  populatedState,
  branchState,
  tagState,
  commitState,
  datasetPayload,
} from "./nessie-spec/index";

describe("nessie root reducer", () => {
  it("returns empty object with no state input", () => {
    const result = nessieReducer(undefined, { type: "blah" } as any);
    expect(result).to.deep.equal({});
  });
  it("returns same state when action does not start with NESSIE_", () => {
    const input = {};
    const result = nessieReducer(input, { type: "blah" } as any);
    expect(result).to.equal(input);
  });
  it("correctly initializes refs when state is empty", () => {
    const result = nessieReducer(testStateEmpty, { type: INIT_REFS } as any);
    expect(result).to.deep.equal({
      ...testStateEmpty,
      "ref/nessie": empty,
      "ref/nessie2": empty,
    });
  });
  it("correctly initializes dataset refs when state is not empty", () => {
    const result = nessieReducer(populatedState, { type: INIT_REFS } as any);
    expect(result).to.deep.equal({
      ...populatedState,
      "ref/arctic_source": commitState,
      "ref/arctic_source1": tagState,
      "ref/arctic_source3": branchState,
    });
  });
  it("correctly initializes refs from server dataset object", () => {
    const result = nessieReducer(populatedState, {
      type: SET_REFS,
      payload: datasetPayload,
    });
    const { hash, ...tagRef } = tagState.reference;
    const { hash: bHash, ...branchRef } = branchState.reference;
    expect(result).to.deep.equal({
      ...populatedState,
      "ref/arctic_source": commitState,
      "ref/arctic_source1": {
        ...tagState,
        reference: tagRef,
      },
      "ref/arctic_source3": {
        ...branchState,
        reference: branchRef,
      },
    });
  });
});
