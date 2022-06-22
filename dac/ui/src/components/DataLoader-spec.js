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
import { mount } from "enzyme";
import { DataLoader } from "@app/components/DataLoader";

describe("DataLoader", () => {
  const onChange = sinon.stub();

  beforeEach(() => {
    onChange.resetHistory();
  });

  it("calls onChange once on mount", () => {
    mount(<DataLoader onChange={onChange} />);

    expect(onChange).calledOnce;
  });

  it("calls onChange once on mount even isInvalidated = true", () => {
    mount(<DataLoader onChange={onChange} isInvalidated />);

    expect(onChange).callCount(1);
  });

  it("calls onChange if keyValue is changed", () => {
    const wrapper = mount(<DataLoader onChange={onChange} />);
    onChange.resetHistory();

    wrapper.setProps({
      keyValue: 1,
    });

    expect(onChange).calledOnce;
  });

  it("calls onChange if isInvalidated changed to true", () => {
    const wrapper = mount(
      <DataLoader onChange={onChange} isInvalidated={false} />
    );
    onChange.resetHistory();

    wrapper.setProps({
      isInvalidated: true,
    });

    expect(onChange).calledOnce;
  });

  it("does NOT call onChange if isInvalidated changed to false", () => {
    const wrapper = mount(<DataLoader onChange={onChange} isInvalidated />);
    onChange.resetHistory();

    wrapper.setProps({
      isInvalidated: false,
    });

    expect(onChange).not.called;
  });
});
