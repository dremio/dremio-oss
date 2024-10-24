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
import { renderHook } from "@testing-library/react-hooks";
import { NESSIE_REF_PREFIX } from "#oss/constants/nessie";
import { getContextValue, useCtxPickerActions, getCtxState } from "./utils";
import { NessieRootState } from "#oss/types/nessie";
import * as sinon from "sinon";
import { act } from "@testing-library/react";

const populatedState = {
  defaultReference: null,
  reference: {
    type: "BRANCH",
    name: "main",
  },
  hash: null,
  date: null,
  loading: {},
  errors: {},
};

const emptyState = {
  defaultReference: null,
  reference: null,
  hash: null,
  date: null,
  loading: {
    NESSIE_DEFAULT_REF_REQUEST: true,
  },
  errors: {},
};

describe("ContextPicker Utils", () => {
  describe("edit query context", () => {
    it("parse correct context value", () => {
      const result = getContextValue(["my-space", "my", "folder"]);
      expect(result).to.eql("my-space.my.folder");
    });
    it("should default to correct value", () => {
      const result = getContextValue([""]);
      expect(result).to.eql("(None selected)");
    });
    it("should default to correct value using trim", () => {
      const result = getContextValue();
      expect(result).to.eql("<none>");
    });
  });
  describe("getCtxState utility", () => {
    it("Should parse correct state and loading flag", () => {
      const sourceName = "arctic1";
      const nessieState = {
        [`${NESSIE_REF_PREFIX}${sourceName}`]: populatedState,
      } as NessieRootState;
      const [state, loading] = getCtxState({ nessieState, sourceName });
      expect(state).to.eql(populatedState);
      expect(loading).to.eql(false);
    });
    it("Should return loading=true", () => {
      const sourceName = "arctic1";
      const nessieState = {
        [`${NESSIE_REF_PREFIX}${sourceName}`]: emptyState,
      } as NessieRootState;
      const [state, loading] = getCtxState({ nessieState, sourceName });
      expect(state).to.eql(emptyState);
      expect(loading).to.eql(true);
    });
  });
  describe("useCtxPickerActions - Populated state", () => {
    let result: any;
    let resetRefs: sinon.SinonSpy;
    beforeEach(() => {
      const sourceName = "arctic1";
      const nessieState = {
        [`${NESSIE_REF_PREFIX}${sourceName}`]: populatedState,
      } as NessieRootState;
      resetRefs = sinon.spy();
      const render = renderHook(() =>
        useCtxPickerActions({
          nessieState,
          resetRefs,
        }),
      );
      result = render.result;
    });
    it("Starts closed, opens, closes", () => {
      let [show, open, close] = result.current;
      expect(show).to.eql(false);
      act(open);
      [show, open, close] = result.current;
      expect(show).to.eql(true);
      act(close);
      [show, open, close] = result.current;
      expect(show).to.eql(false);
      expect(resetRefs.notCalled).to.eql(true);
    });
    it("Only calls resetRefs if canceled when opened", () => {
      let [show, open, close, cancel] = result.current;
      act(cancel);
      [show, open, close, cancel] = result.current;
      expect(resetRefs.notCalled).to.eql(true);
      expect(show).to.eql(false);
      act(open);
      act(close);
      expect(resetRefs.notCalled).to.eql(true);
      act(open);
      act(cancel);
      expect(resetRefs.args.length).to.eql(1);
      expect(resetRefs.args[0][0]).to.eql({
        arctic1: { type: "BRANCH", value: "main" },
      });
      [show] = result.current;
      expect(show).to.eql(false);
    });
  });
  describe("useCtxPickerActions - Null state", () => {
    it("Render without error with null state", () => {
      const resetRefs = sinon.spy();
      const { result } = renderHook(() =>
        useCtxPickerActions({
          nessieState: null,
          resetRefs,
        }),
      );
      const [, , , cancel] = result.current;
      act(cancel);
      expect(resetRefs.notCalled).to.eql(true);
    });
  });
  describe("useCtxPickerActions - No ref values doesn't reset refs", () => {
    it("Does not call reset refs if when no refs present (empty object)", () => {
      const sourceName = "arctic1";
      const nessieState = {
        [`${NESSIE_REF_PREFIX}${sourceName}`]: {
          ...populatedState,
          reference: null,
        },
      } as NessieRootState;
      const resetRefs = sinon.spy();
      const { result } = renderHook(() =>
        useCtxPickerActions({
          nessieState,
          resetRefs,
        }),
      );
      const [, open, , cancel] = result.current;
      act(open);
      act(cancel);
      expect(resetRefs.notCalled).to.eql(true);
    });
  });
});
