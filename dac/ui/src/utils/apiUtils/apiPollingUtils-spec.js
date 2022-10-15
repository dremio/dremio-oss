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
import ApiPolling from "./apiPollingUtils";
import ApiUtils from "./apiUtils";

describe.skip("ApiPollingUtils", () => {
  const props = {
    endpoint: "ep",
    options: { method: "GET" },
    version: 2,
    handleFailure: sinon.stub().returns(true), //returns boolean isStopPollingNeeded
    handleSuccess: sinon.stub().returns(false), //returns boolean isStopPollingNeeded
  };

  it("should call fetch at least once", async () => {
    const stub = sinon.stub(ApiUtils, "fetch").resolves({ ok: true });
    await ApiPolling(props);
    expect(ApiUtils.fetch).to.have.been.called;
    stub.restore();
  });
  it("should call handleFailure upon failure", async () => {
    const stub = sinon.stub(ApiUtils, "fetch").rejects({ error: "foo" });
    await ApiPolling(props);
    expect(props.handleFailure).to.have.been.called;
    stub.restore();
  });
  it("should call handleSuccess upon success", async () => {
    const stub = sinon.stub(ApiUtils, "fetch").resolves({ ok: true });
    await ApiPolling(props);
    expect(props.handleSuccess).to.have.been.called;
    stub.restore();
  });
  it("should call handleSuccess multiple times", async () => {
    props.handleSuccess.resetHistory();
    const stub = sinon.stub(ApiUtils, "fetch").resolves({ ok: true });
    await ApiPolling(props, 1, 2);
    setTimeout(() => {
      expect(props.handleSuccess).to.have.been.calledTwice;
    }, 1500);
    stub.restore();
  });
});
