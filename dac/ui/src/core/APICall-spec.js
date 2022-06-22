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
import APICall, { APIV2Call } from "./APICall";

describe("APICall", () => {
  it("v2", () => {
    const apiCall = new APICall().paths("foo/bar").apiVersion(2);

    expect(apiCall.toString()).to.eql("//localhost/apiv2/foo/bar");
  });

  it("v3", () => {
    const apiCall = new APICall().paths("foo/bar").apiVersion(3);

    expect(apiCall.toString()).to.eql("//localhost/api/v3/foo/bar");
  });

  it("path with many segments", () => {
    const apiCall = new APICall()
      .paths("foo/bar/b#az")
      .path("baz")
      .apiVersion(3);

    expect(apiCall.toString()).to.eql("//localhost/api/v3/foo/bar/b%23az/baz");
  });

  it("path with multiple calls", () => {
    const apiCall = new APICall()
      .paths("foo/bar")
      .paths("/baz/boo/")
      .paths("/bar")
      .apiVersion(3);

    expect(apiCall.toString()).to.eql("//localhost/api/v3/foo/bar/baz/boo/bar");
  });

  it("params", () => {
    const apiCall = new APICall()
      .paths("foo/bar")
      .apiVersion(3)
      .params({ a: 1, b: "a/?.b" });

    expect(apiCall.toString()).to.eql(
      "//localhost/api/v3/foo/bar/?a=1&b=a%2F%3F.b"
    );
  });

  it("params with list", () => {
    const apiCall = new APICall()
      .paths("foo/bar")
      .apiVersion(3)
      .params({ a: [1, 2, 3] });

    expect(apiCall.toString()).to.eql(
      "//localhost/api/v3/foo/bar/?a=1&a=2&a=3"
    );
  });

  it("APIV2Call", () => {
    const apiCall = new APIV2Call()
      .paths("foo/bar")
      .params({ a: 1, b: "a/?.b" });

    expect(apiCall.toString()).to.eql(
      "//localhost/apiv2/foo/bar/?a=1&b=a%2F%3F.b"
    );
  });

  it("path should support leading / as well as missing", () => {
    const apiCall = new APIV2Call().paths("foo/bar");
    expect(apiCall.toString()).to.eql("//localhost/apiv2/foo/bar");

    const apiCall2 = new APIV2Call().paths("/foo/bar");
    expect(apiCall2.toString()).to.eql("//localhost/apiv2/foo/bar");
  });

  it("toPath()", () => {
    const apiCall = new APIV2Call().paths("foo/bar");
    expect(apiCall.toString()).to.eql("//localhost/apiv2/foo/bar");
    expect(apiCall.toPath()).to.eql("/foo/bar");
  });

  it("fullpath", () => {
    const apiCall = new APIV2Call().fullpath("foo/bar");
    expect(apiCall.toPath()).to.eql("/foo/bar");
  });

  it("fullpath with query param", () => {
    const apiCall = new APIV2Call()
      .fullpath("foo/bar?baz=foo")
      .param("test", 1);
    expect(apiCall.toPath()).to.eql("/foo/bar?baz=foo&test=1");
  });
});
