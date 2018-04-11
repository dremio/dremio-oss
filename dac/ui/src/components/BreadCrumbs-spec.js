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
import { shallow } from 'enzyme';
import { Link } from 'react-router';
import Immutable from 'immutable';

import BreadCrumbs, {getPartialPath, getPathElements} from './BreadCrumbs';

const getLinkText = (wrapper, index) => {
  return wrapper.children().at(index).find('Link').children().text();
};

describe('BreadCrumbs', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      fullPath: Immutable.List(['foo', 'bar']),
      pathname: '/space/foo/folder/bar'
    };
  });

  describe('render', () => {
    it('should render dots', () => {
      const wrapper = shallow(<BreadCrumbs {...commonProps}/>);
      expect(
        wrapper.children().first().text()
      ).to.eql('<Link />.');

      expect(
        wrapper.children().at(1).text()
      ).to.eql('bar');
    });

    it('should not render last item when hideLastItem is true', () => {
      const wrapper = shallow(<BreadCrumbs {...commonProps}/>);
      wrapper.setProps({
        fullPath: Immutable.List(['foo', 'bar', 'baz']),
        hideLastItem: true
      });
      expect(wrapper.children()).to.have.length(2);
      expect(
        wrapper.children().first().text()
      ).to.eql('<Link />.');
      expect(getLinkText(wrapper, 0)).to.be.eql('foo');
      expect(
        wrapper.children().at(1).text()
      ).to.eql('<Link />');
      expect(getLinkText(wrapper, 1)).to.be.eql('bar');
    });
  });

  describe('getPartialPath', () => {
    it('should return no action for first item', () => {
      expect(getPartialPath(0, Immutable.List(['foo']), '/space/foo')).to.eql('/space/foo');
      expect(getPartialPath(0, Immutable.List(['foo', 'bar']), '/space/foo/folder/bar')).to.eql('/space/foo');
    });

    it('should return folder action for subsequent items', () => {
      expect(
        getPartialPath(1, Immutable.List(['foo', 'bar', 'baz']), '/space/foo/query/bar/baz')
      ).to.eql('/space/foo/folder/bar');
      expect(
        getPartialPath(2, Immutable.List(['foo', 'bar', 'baz']), '/space/foo/query/bar/baz')
      ).to.eql('/space/foo/folder/bar/baz');
    });

    it('should handle url encoded path', () => {
      expect(
        getPartialPath(1, Immutable.List(['foo bar', 'baz']), '/space/foo%20bar/folder/baz')
      ).to.eql('/space/foo%20bar/folder/baz');
      expect(
        getPartialPath(0, Immutable.List(['foo bar']), '/space/foo%20bar')
      ).to.eql('/space/foo%20bar');
    });

    it('should return / for home path', () => {
      expect(
        getPartialPath(0, Immutable.List(['@test_user', 'bar']), '/home/@test_user/folder/bar')
      ).to.eql('/');
      expect(
        getPartialPath(1, Immutable.List(['@test_user', 'bar']), '/home/@test_user/folder/bar')
      ).to.eql('/home/%40test_user/folder/bar');
    });

    it('should encode path parts', () => {
      const fullPath = Immutable.List(['space+name', 'folder/name', 'folder@2']);
      const pathname = '/space/space%2Bname/folder/folder%2Fname/folder%402';
      expect(
        getPartialPath(0, fullPath, pathname)
      ).to.eql('/space/space%2Bname');

      expect(
        getPartialPath(1, fullPath, pathname)
      ).to.eql('/space/space%2Bname/folder/folder%2Fname');
    });
  });

  describe('getPathElements', () => {
    it('should return the last item without a link', () => {
      expect(
        getPathElements(Immutable.List(['foo', 'bar']), '/space/foo/folder/bar', {})
      ).to.eql(
        [
          <Link key={'foo0'} to='/space/foo' style={{}}>foo</Link>,
          <span key={'bar1'} style={{}}>bar</span>
        ]
      );
    });
  });

  describe('formatFullPath', () => {
    it('should wrap item with "" if it contains a dot', () => {
      expect(BreadCrumbs.formatFullPath(Immutable.List(['foo.bar', 'baz']))).to.eql(
        Immutable.List(['"foo.bar"', 'baz'])
      );
    });

    it('should not wrap item with "" if it contains a dot but only has one item in path', () => {
      expect(BreadCrumbs.formatFullPath(Immutable.List(['foo.bar']))).to.eql(
        Immutable.List(['foo.bar'])
      );
    });
  });
});
