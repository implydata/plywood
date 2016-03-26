var { assert } = require("chai");
var { deepEqual } = assert;

var plywood = require("../../build/plywood");
var { parseISODate } = plywood.helper;

describe('parseISODate', () => {
  var sixHours       = 6 * 60 * 60 * 1000,
    sixHoursThirty = sixHours + 30 * 60 * 1000;

  // Taken from https://github.com/csnover/js-iso8601/blob/lax/tests/test.js
  it('date-part', function () {
    deepEqual(parseISODate('1970-01-01'), new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 0)), 'Unix epoch');

    deepEqual(parseISODate('2001'),        new Date(Date.UTC(2001, 0, 1, 0, 0, 0, 0)), '2001');
    deepEqual(parseISODate('2001-02'),     new Date(Date.UTC(2001, 1, 1, 0, 0, 0, 0)), '2001-02');
    deepEqual(parseISODate('2001-02-03'),  new Date(Date.UTC(2001, 1, 3, 0, 0, 0, 0)), '2001-02-03');
    deepEqual(parseISODate('2001-02-03Z'), new Date(Date.UTC(2001, 1, 3, 0, 0, 0, 0)), '2001-02-03Z');

    deepEqual(parseISODate('-002001'),       new Date(Date.UTC(-2001, 0, 1, 0, 0, 0, 0)), '-002001');
    deepEqual(parseISODate('-002001-02'),    new Date(Date.UTC(-2001, 1, 1, 0, 0, 0, 0)), '-002001-02');
    deepEqual(parseISODate('-002001-02-03'), new Date(Date.UTC(-2001, 1, 3, 0, 0, 0, 0)), '-002001-02-03');

    deepEqual(parseISODate('+010000-02'),    new Date(Date.UTC(10000, 1, 1, 0, 0, 0, 0)), '+010000-02');
    deepEqual(parseISODate('+010000-02-03'), new Date(Date.UTC(10000, 1, 3, 0, 0, 0, 0)), '+010000-02-03');
    deepEqual(parseISODate('-010000-02'),    new Date(Date.UTC(-10000, 1, 1, 0, 0, 0, 0)), '-010000-02');
    deepEqual(parseISODate('-010000-02-03'), new Date(Date.UTC(-10000, 1, 3, 0, 0, 0, 0)), '-010000-02-03');

    deepEqual(parseISODate('19700101'),      new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 0)), 'non-hyphenated Unix epoch');
    deepEqual(parseISODate('19700101Z'),     new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 0)), 'non-hyphenated Unix epoch Z');

    // 2 digit years incorrectly parsed as though they were prefixed with 19
    /* disabled, since no native implementations get this right either
     deepEqual(parseISODate('0099-12-31').valueOf(, new Date(Date.UTC(100, 0, 1, 0, 0, 0, 0) - 1000 * 60 * 60 * 24), '0099-12-31');
     deepEqual(parseISODate('0099-12-31T00:00Z').valueOf(, new Date(Date.UTC(100, 0, 1, 0, 0, 0, 0) - 1000 * 60 * 60 * 24), '0099-12-31T00:00Z');
     */

    deepEqual(parseISODate('asdf'), null, 'invalid YYYY (non-digits)');
    deepEqual(parseISODate('1970-as-df'), null, 'invalid YYYY-MM-DD (non-digits)');
    deepEqual(parseISODate('1970-01-'), null, 'invalid YYYY-MM- (extra hyphen)');
    deepEqual(parseISODate('197001'),  new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 0)), 'non-hyphenated year-month');
    deepEqual(parseISODate('197001Z'), new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 0)), 'non-hyphenated year-month Z');

    // TODO: Test for invalid YYYYMM and invalid YYYYY?
  });

  it('date-time', function () {
    deepEqual(parseISODate('2001-02-03T04:05'),        new Date(Date.UTC(2001, 1, 3, 4, 5, 0, 0)), '2001-02-03T04:05');
    deepEqual(parseISODate('2001-02-03T04:05:06'),     new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 0)), '2001-02-03T04:05:06');
    deepEqual(parseISODate('2001-02-03T04:05:06.007'), new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 7)), '2001-02-03T04:05:06.007');

    deepEqual(parseISODate('2001-02-03T04:05Z'),        new Date(Date.UTC(2001, 1, 3, 4, 5, 0, 0)), '2001-02-03T04:05Z');
    deepEqual(parseISODate('2001-02-03T04:05:06Z'),     new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 0)), '2001-02-03T04:05:06Z');
    deepEqual(parseISODate('2001-02-03T04:05:06.007Z'), new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 7)), '2001-02-03T04:05:06.007Z');

    deepEqual(parseISODate('2001-02-03T04:05-00:00'),        new Date(Date.UTC(2001, 1, 3, 4, 5, 0, 0)), '2001-02-03T04:05-00:00');
    deepEqual(parseISODate('2001-02-03T04:05:06-00:00'),     new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 0)), '2001-02-03T04:05:06-00:00');
    deepEqual(parseISODate('2001-02-03T04:05:06.007-00:00'), new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 7)), '2001-02-03T04:05:06.007-00:00');

    deepEqual(parseISODate('2001-02-03T04:05+00:00'),        new Date(Date.UTC(2001, 1, 3, 4, 5, 0, 0)), '2001-02-03T04:05+00:00');
    deepEqual(parseISODate('2001-02-03T04:05:06+00:00'),     new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 0)), '2001-02-03T04:05:06+00:00');
    deepEqual(parseISODate('2001-02-03T04:05:06.007+00:00'), new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 7)), '2001-02-03T04:05:06.007+00:00');

    deepEqual(parseISODate('2001-02-03T04:05-06:30'),        new Date(Date.UTC(2001, 1, 3, 4, 5, 0, 0) + sixHoursThirty), '2001-02-03T04:05-06:30');
    deepEqual(parseISODate('2001-02-03T04:05:06-06:30'),     new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 0) + sixHoursThirty), '2001-02-03T04:05:06-06:30');
    deepEqual(parseISODate('2001-02-03T04:05:06.007-06:30'), new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 7) + sixHoursThirty), '2001-02-03T04:05:06.007-06:30');

    deepEqual(parseISODate('2001-02-03T04:05+06:30'),        new Date(Date.UTC(2001, 1, 3, 4, 5, 0, 0) - sixHoursThirty), '2001-02-03T04:05+06:30');
    deepEqual(parseISODate('2001-02-03T04:05:06+06:30'),     new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 0) - sixHoursThirty), '2001-02-03T04:05:06+06:30');
    deepEqual(parseISODate('2001-02-03T04:05:06.007+06:30'), new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 7) - sixHoursThirty), '2001-02-03T04:05:06.007+06:30');

    deepEqual(parseISODate('2001T04:05:06.007'),             new Date(Date.UTC(2001, 0, 1, 4, 5, 6, 7)), '2001T04:05:06.007');
    deepEqual(parseISODate('2001-02T04:05:06.007'),          new Date(Date.UTC(2001, 1, 1, 4, 5, 6, 7)), '2001-02T04:05:06.007');
    deepEqual(parseISODate('2001-02-03T04:05:06.007'),       new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 7)), '2001-02-03T04:05:06.007');
    deepEqual(parseISODate('2001-02-03T04:05:06.07'),       new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 70)), '2001-02-03T04:05:06.07');
    deepEqual(parseISODate('2001-02-03T04:05:06.7'),       new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 700)), '2001-02-03T04:05:06.7');
    deepEqual(parseISODate('2001-02-03T04:05:06.007-06:30'), new Date(Date.UTC(2001, 1, 3, 4, 5, 6, 7) + sixHoursThirty), '2001-02-03T04:05:06.007-06:30');

    deepEqual(parseISODate('-010000T04:05'),       new Date(Date.UTC(-10000, 0, 1, 4, 5, 0, 0)), '-010000T04:05');
    deepEqual(parseISODate('-010000-02T04:05'),    new Date(Date.UTC(-10000, 1, 1, 4, 5, 0, 0)), '-010000-02T04:05');
    deepEqual(parseISODate('-010000-02-03T04:05'), new Date(Date.UTC(-10000, 1, 3, 4, 5, 0, 0)), '-010000-02-03T04:05');

    deepEqual(parseISODate('1970-01-01 00:00:00Z'),       new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 0)), 'space-separated datetime');
    deepEqual(parseISODate('1970-01-01T00:00:00.987654'), new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 987)), 'extended sub-second precision');
    deepEqual(parseISODate('1970-01-01T00:00:00,123'),    new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 123)), 'comma-delimited milliseconds');
    deepEqual(parseISODate('1970-01-01T00:00:00+0630'),   new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 0) - sixHoursThirty), 'colon-free timezone part');
    deepEqual(parseISODate('1970-01-01T000000'),          new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 0)), 'colon-free time part');
    deepEqual(parseISODate('1970-01-01T0000'),            new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 0)), 'colon-free time part minute hour');
    deepEqual(parseISODate('1970-01-01T00'),              new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 0)), 'hour only time part');
    deepEqual(parseISODate('1970-01-01T00:00.000'), null, 'invalid date-time (msec with missing seconds)');

    // TODO: DRY
  });

});
