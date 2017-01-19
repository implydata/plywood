/*
 * Copyright 2015-2017 Imply Data, Inc.
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

const { expect } = require("chai");

let plywood = require('../plywood');
let { Expression, $, ply, r } = plywood;

let toJS = (extractAndRest) => {
  if (!extractAndRest) return extractAndRest;
  return {
    extract: extractAndRest.extract.toJS(),
    rest: extractAndRest.rest.toJS()
  };
};

function freeReferenceExtractor(refName) {
  return function(ex) {
    let freeRefs = ex.getFreeReferences();
    return freeRefs.length === 1 && freeRefs[0] === refName;
  };
}

describe("extractFromAnd", () => {
  it('works with TRUE expression', () => {
    let ex = Expression.TRUE;

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('venue')))).to.deep.equal(toJS({
      extract: Expression.TRUE,
      rest: Expression.TRUE
    }));
  });

  it('works with FALSE expression', () => {
    let ex = Expression.FALSE;

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('venue')))).to.deep.equal(toJS({
      extract: Expression.TRUE,
      rest: Expression.FALSE
    }));
  });

  it('works on a single extract expression', () => {
    let ex = $('venue').is('Google');

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('venue')))).to.deep.equal(toJS({
      extract: ex,
      rest: Expression.TRUE
    }));
  });

  it('works on a single rest expression', () => {
    let ex = $('venue').is('Google');

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('make')))).to.deep.equal(toJS({
      extract: Expression.TRUE,
      rest: ex
    }));
  });

  it('works on a small AND expression', () => {
    let ex = $('venue').is('Google').and($('country').is('USA'));

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('country')))).to.deep.equal(toJS({
      extract: $('country').is('USA'),
      rest: $('venue').is('Google')
    }));
  });

  it('works on an AND expression', () => {
    let ex = $('venue').is('Google').and($('country').is('USA'), $('state').is('California'));

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('country')))).to.deep.equal(toJS({
      extract: $('country').is('USA'),
      rest: $('venue').is('Google').and($('state').is('California'))
    }));
  });

  it('extracts a NOT expression', () => {
    let ex = $('venue').is('Google').and($('country').is('USA').not(), $('state').is('California'));

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('country')))).to.deep.equal(toJS({
      extract: $('country').is('USA').not(),
      rest: $('venue').is('Google').and($('state').is('California'))
    }));
  });

  it('works on mixed OR filter (all in)', () => {
    let ex = $('venue').is('Apple').or($('venue').is('Google').not());

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('venue')))).to.deep.equal(toJS({
      extract: ex,
      rest: Expression.TRUE
    }));
  });

  it('works on mixed OR filter (all out)', () => {
    let ex = $('venue').is('Google').or($('country').is('USA'), $('state').is('California'));

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('model')))).to.deep.equal(toJS({
      extract: Expression.TRUE,
      rest: ex
    }));
  });
});
