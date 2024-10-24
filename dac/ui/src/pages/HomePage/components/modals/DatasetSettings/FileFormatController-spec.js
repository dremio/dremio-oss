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
import { shallow } from "enzyme";
import Immutable from "immutable";

import FileFormatForm from "pages/HomePage/components/forms/FileFormatForm";
import ApiUtils from "utils/apiUtils/apiUtils";

import { FileFormatController, VIEW_ID } from "./FileFormatController";

describe("FileFormatController", () => {
  let commonProps;
  let context;
  beforeEach(() => {
    commonProps = {
      isOpen: false,
      hide: sinon.spy(),
      query: {},
      fullPath: ["source1", "file1"],
      fileFormat: Immutable.Map(),
      viewState: Immutable.Map({
        isInProgress: false,
        isFailed: false,
      }),
      previewViewState: Immutable.Map({
        isInProgress: false,
        isFailed: false,
      }),
      loadFileFormat: sinon.spy(),
      saveFileFormat: sinon.spy(),
      loadFilePreview: sinon.spy(),
      resetFileFormatPreview: sinon.spy(),
      resetViewState: sinon.spy(),
      onDone: sinon.spy(),
    };
    context = {
      router: { push: sinon.spy(), replace: sinon.spy() },
    };
  });

  it("renders <FileFormatForm>", () => {
    const wrapper = shallow(<FileFormatController {...commonProps} />, {
      context,
    });
    expect(wrapper.find(FileFormatForm)).to.have.length(1);
  });

  it("triggers load when opened", () => {
    const wrapper = shallow(<FileFormatController {...commonProps} />, {
      context,
    });
    expect(commonProps.loadFileFormat).to.have.not.been.called;
    const url = "test/url";
    wrapper.setProps({ formatUrl: url });
    expect(commonProps.loadFileFormat).to.have.been.calledWith(url, VIEW_ID);
    expect(commonProps.resetViewState).to.have.been.calledWith(VIEW_ID);
  });

  describe("#onSubmitFormat", () => {
    let wrapper;
    let instance;
    let props;
    beforeEach(() => {
      props = {
        ...commonProps,
        entity: Immutable.fromJS({
          links: { query: "/query/link" },
          fileFormat: "fileFormat",
        }),
      };
      wrapper = shallow(<FileFormatController {...props} />, { context });
      instance = wrapper.instance();
      sinon
        .stub(ApiUtils, "attachFormSubmitHandlers")
        .returns({ then: (f) => f() });
    });
    afterEach(() => {
      ApiUtils.attachFormSubmitHandlers.restore();
    });

    it("should call saveFileFormat from props on submit", () => {
      instance.onSubmitFormat({ type: "JSON", version: 1 });
      expect(ApiUtils.attachFormSubmitHandlers).to.be.called;
      expect(props.saveFileFormat).to.be.calledWith(
        "/source/source1/file_format/file1",
        { type: "JSON", version: 1 },
      );
      expect(props.onDone).to.be.called;
    });

    it("should call saveFileFormat with links.self if entity fileFormat is not defined", () => {
      const fileFormatLink = "/source/s3/file_format/test.json";
      wrapper.setProps({
        fullPath: ["s3", "test.json"],
      });

      instance.onSubmitFormat({ type: "JSON", version: 1 });
      expect(ApiUtils.attachFormSubmitHandlers).to.be.called;
      expect(props.saveFileFormat).to.be.calledWith(fileFormatLink, {
        type: "JSON",
        version: 1,
      });
      expect(props.onDone).to.be.called;
    });

    it('should redirect to links.query from entity when query.then is "query" from props', () => {
      wrapper.setProps({
        query: { then: "query" },
      });
      const values = {};
      instance.onSubmitFormat(values);
      expect(ApiUtils.attachFormSubmitHandlers).to.be.called;
      expect(props.saveFileFormat).to.be.calledWith(
        "/source/source1/file_format/file1",
        values,
      );
      expect(context.router.replace).to.be.calledWith("/source/source1/file1");
      expect(props.onDone).to.be.not.called;
    });
  });
});
