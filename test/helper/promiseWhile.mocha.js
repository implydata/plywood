var { expect } = require("chai");

var Q = require('q');
var { helper } = require("../../build/plywood");

describe('Promise While', () => {

  it('should loop three times asynchronously', (testComplete) => {
    var res = [];
    var i = 0;
    helper.promiseWhile(
      function () {
        return i < 3
      },
      function () {
        return new Promise(function (resolve, reject) {
          setTimeout(function () {
            res.push('aye' + i);
            resolve(i++);
          }, 10)
        })
      }
    )
      .then(function () {
        expect(res).to.deep.equal(['aye0', 'aye1', 'aye2']);
        testComplete();
      })
      .done();
  });

  it('should propagate rejection', (testComplete) => {
    function TestError() {}

    helper.promiseWhile(
      function () {
        return true;
      },
      function () {
        return Q.reject(new TestError('test'));
      }
    )
      .then(() => {
        throw new Error('did not error');
      })
      .catch(function (err) {
        expect(err).be.instanceof(TestError);
        testComplete();
      })
      .done();
  });

  it('should propagate conditions throw', (testComplete) => {
    function TestError() {}

    helper.promiseWhile(
      function () {
        throw new TestError('test');
      },
      function () {
        return Q();
      }
    )
      .then(() => {
        throw new Error('did not error');
      })
      .catch(function (err) {
        expect(err).be.instanceof(TestError);
        testComplete();
      })
      .done();
  });

  it('should propagate action throw', (testComplete) => {
    function TestError() {}

    helper.promiseWhile(
      function () {
        return true;
      },
      function () {
        throw new TestError('test');
      }
    )
      .then(() => {
        throw new Error('did not error');
      })
      .catch(function (err) {
        expect(err).be.instanceof(TestError);
        testComplete();
      })
      .done();
  });

});
