(function(f){if(typeof exports==="object"&&typeof module!=="undefined"){module.exports=f()}else if(typeof define==="function"&&define.amd){define([],f)}else{var g;if(typeof window!=="undefined"){g=window}else if(typeof global!=="undefined"){g=global}else if(typeof self!=="undefined"){g=self}else{g=this}g.plywood = f()}})(function(){var define,module,exports;return (function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);var f=new Error("Cannot find module '"+o+"'");throw f.code="MODULE_NOT_FOUND",f}var l=n[o]={exports:{}};t[o][0].call(l.exports,function(e){var n=t[o][1][e];return s(n?n:e)},l,l.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({1:[function(require,module,exports){
"use strict";
var ImmutableClass = require("immutable-class");
var q = require("q");
var Q = q;
var chronoshift = require("chronoshift");
var Chronoshift = chronoshift;
var dummyObject = {};
var objectHasOwnProperty = Object.prototype.hasOwnProperty;
function hasOwnProperty(obj, key) {
    return objectHasOwnProperty.call(obj, key);
}
function repeat(str, times) {
    return new Array(times + 1).join(str);
}
function deduplicateSort(a) {
    a = a.sort();
    var newA = [];
    var last = null;
    for (var _i = 0; _i < a.length; _i++) {
        var v = a[_i];
        if (v !== last)
            newA.push(v);
        last = v;
    }
    return newA;
}
function multiMerge(elements, mergeFn) {
    var newElements = [];
    for (var _i = 0; _i < elements.length; _i++) {
        var accumulator = elements[_i];
        var tempElements = [];
        for (var _a = 0; _a < newElements.length; _a++) {
            var newElement = newElements[_a];
            var mergeElement = mergeFn(accumulator, newElement);
            if (mergeElement) {
                accumulator = mergeElement;
            }
            else {
                tempElements.push(newElement);
            }
        }
        tempElements.push(accumulator);
        newElements = tempElements;
    }
    return newElements;
}
function arraysEqual(a, b) {
    if (a === b)
        return true;
    var length = a.length;
    if (length !== b.length)
        return false;
    for (var i = 0; i < length; i++) {
        if (a[i] !== b[i])
            return false;
    }
    return true;
}
function dictEqual(dictA, dictB) {
    if (dictA === dictB)
        return true;
    if (!dictA !== !dictB)
        return false;
    var keys = Object.keys(dictA);
    if (keys.length !== Object.keys(dictB).length)
        return false;
    for (var _i = 0; _i < keys.length; _i++) {
        var key = keys[_i];
        if (dictA[key] !== dictB[key])
            return false;
    }
    return true;
}
function higherArraysEqual(a, b) {
    var length = a.length;
    if (length !== b.length)
        return false;
    for (var i = 0; i < length; i++) {
        if (!a[i].equals(b[i]))
            return false;
    }
    return true;
}
var expressionParser;
var sqlParser;
var Plywood;
(function (Plywood) {
    Plywood.version = '0.8.1';
    Plywood.isInstanceOf = ImmutableClass.isInstanceOf;
    Plywood.isImmutableClass = ImmutableClass.isImmutableClass;
    Plywood.Timezone = Chronoshift.Timezone;
    Plywood.Duration = Chronoshift.Duration;
    Plywood.WallTime = Chronoshift.WallTime;
    function safeAdd(num, delta) {
        var stringDelta = String(delta);
        var dotIndex = stringDelta.indexOf(".");
        if (dotIndex === -1 || stringDelta.length === 18) {
            return num + delta;
        }
        else {
            var scale = Math.pow(10, stringDelta.length - dotIndex - 1);
            return (num * scale + delta * scale) / scale;
        }
    }
    Plywood.safeAdd = safeAdd;
    function find(array, fn) {
        for (var i = 0, n = array.length; i < n; i++) {
            var a = array[i];
            if (fn.call(array, a, i))
                return a;
        }
        return null;
    }
    Plywood.find = find;
    function continuousFloorExpression(variable, floorFn, size, offset) {
        var expr = variable;
        if (offset !== 0) {
            expr = expr + " - " + offset;
        }
        if (offset !== 0 && size !== 1) {
            expr = "(" + expr + ")";
        }
        if (size !== 1) {
            expr = expr + " / " + size;
        }
        expr = floorFn + "(" + expr + ")";
        if (size !== 1) {
            expr = expr + " * " + size;
        }
        if (offset !== 0) {
            expr = expr + " + " + offset;
        }
        return expr;
    }
    Plywood.continuousFloorExpression = continuousFloorExpression;
})(Plywood || (Plywood = {}));
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var Plywood;
(function (Plywood) {
    var TIME_BUCKETING = {
        "PT1S": "%Y-%m-%dT%H:%i:%SZ",
        "PT1M": "%Y-%m-%dT%H:%iZ",
        "PT1H": "%Y-%m-%dT%H:00Z",
        "P1D": "%Y-%m-%dZ",
        "P1M": "%Y-%m-01Z",
        "P1Y": "%Y-01-01Z"
    };
    var SQLDialect = (function () {
        function SQLDialect() {
        }
        SQLDialect.prototype.escapeName = function (name) {
            if (name.indexOf('`') !== -1)
                throw new Error("can not convert to SQL");
            return '`' + name + '`';
        };
        SQLDialect.prototype.escapeLiteral = function (name) {
            return JSON.stringify(name);
        };
        SQLDialect.prototype.booleanToSQL = function (bool) {
            return ('' + bool).toUpperCase();
        };
        SQLDialect.prototype.numberToSQL = function (num) {
            if (num === null)
                return 'NULL';
            return '' + num;
        };
        SQLDialect.prototype.timeToSQL = function (date) {
            if (!date)
                return 'NULL';
            var str = date.toISOString()
                .replace("T", " ")
                .replace(/\.\d\d\dZ$/, "")
                .replace(" 00:00:00", "");
            return "'" + str + "'";
        };
        SQLDialect.prototype.inExpression = function (operand, start, end, bounds) {
            var startSQL = null;
            if (start !== 'NULL') {
                startSQL = start + (bounds[0] === '[' ? '<=' : '<') + operand;
            }
            var endSQL = null;
            if (end !== 'NULL') {
                endSQL = operand + (bounds[1] === ']' ? '<=' : '<') + end;
            }
            if (startSQL) {
                return endSQL ? "(" + startSQL + " AND " + endSQL + ")" : startSQL;
            }
            else {
                return endSQL ? endSQL : 'TRUE';
            }
        };
        SQLDialect.prototype.timeBucketExpression = function (operand, duration, timezone) {
            throw new Error('Must implement timeBucketExpression');
        };
        SQLDialect.prototype.timePartExpression = function (operand, part, timezone) {
            throw new Error('Must implement timePartExpression');
        };
        SQLDialect.prototype.offsetTimeExpression = function (operand, duration) {
            throw new Error('Must implement offsetTimeExpression');
        };
        return SQLDialect;
    })();
    Plywood.SQLDialect = SQLDialect;
    var MySQLDialect = (function (_super) {
        __extends(MySQLDialect, _super);
        function MySQLDialect() {
            _super.call(this);
        }
        MySQLDialect.prototype.timezoneConvert = function (operand, timezone) {
            if (timezone.isUTC())
                return operand;
            return "CONVERT_TZ(" + operand + ",'+0:00','" + timezone.toString() + "')";
        };
        MySQLDialect.prototype.timeBucketExpression = function (operand, duration, timezone) {
            var bucketFormat = TIME_BUCKETING[duration.toString()];
            if (!bucketFormat)
                throw new Error("unsupported duration '" + duration + "'");
            return "DATE_FORMAT(" + this.timezoneConvert(operand, timezone) + ",'" + bucketFormat + "')";
        };
        MySQLDialect.prototype.timePartExpression = function (operand, part, timezone) {
            var timePartFunction = MySQLDialect.TIME_PART_TO_FUNCTION[part];
            if (!timePartFunction)
                throw new Error("unsupported part " + part + " in MySQL dialect");
            return timePartFunction.replace(/\$\$/g, this.timezoneConvert(operand, timezone));
        };
        MySQLDialect.prototype.offsetTimeExpression = function (operand, duration) {
            var sqlFn = "DATE_ADD(";
            var spans = duration.valueOf();
            if (spans.week) {
                return sqlFn + operand + ", INTERVAL " + String(spans.week) + ' WEEK)';
            }
            if (spans.year || spans.month) {
                var expr = String(spans.year || 0) + "-" + String(spans.month || 0);
                operand = sqlFn + operand + ", INTERVAL '" + expr + "' YEAR_MONTH)";
            }
            if (spans.day || spans.hour || spans.minute || spans.second) {
                var expr = String(spans.day || 0) + " " + [spans.hour || 0, spans.minute || 0, spans.second || 0].join(':');
                operand = sqlFn + operand + ", INTERVAL '" + expr + "' DAY_SECOND)";
            }
            return operand;
        };
        MySQLDialect.TIME_PART_TO_FUNCTION = {
            SECOND_OF_MINUTE: 'SECOND($$)',
            SECOND_OF_HOUR: '(MINUTE($$)*60+SECOND($$))',
            SECOND_OF_DAY: '((HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
            SECOND_OF_WEEK: '(((WEEKDAY($$)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
            SECOND_OF_MONTH: '((((DAYOFMONTH($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
            SECOND_OF_YEAR: '((((DAYOFYEAR($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
            MINUTE_OF_HOUR: 'MINUTE($$)',
            MINUTE_OF_DAY: 'HOUR($$)*60+MINUTE($$)',
            MINUTE_OF_WEEK: '(WEEKDAY($$)*24)+HOUR($$)*60+MINUTE($$)',
            MINUTE_OF_MONTH: '((DAYOFMONTH($$)-1)*24)+HOUR($$)*60+MINUTE($$)',
            MINUTE_OF_YEAR: '((DAYOFYEAR($$)-1)*24)+HOUR($$)*60+MINUTE($$)',
            HOUR_OF_DAY: 'HOUR($$)',
            HOUR_OF_WEEK: '(WEEKDAY($$)*24+HOUR($$))',
            HOUR_OF_MONTH: '((DAYOFMONTH($$)-1)*24+HOUR($$))',
            HOUR_OF_YEAR: '((DAYOFYEAR($$)-1)*24+HOUR($$))',
            DAY_OF_WEEK: 'WEEKDAY($$)',
            DAY_OF_MONTH: '(DAYOFMONTH($$)-1)',
            DAY_OF_YEAR: '(DAYOFYEAR($$)-1)',
            WEEK_OF_MONTH: null,
            WEEK_OF_YEAR: 'WEEK($$)',
            MONTH_OF_YEAR: 'MONTH($$)'
        };
        return MySQLDialect;
    })(SQLDialect);
    Plywood.MySQLDialect = MySQLDialect;
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    function getValueType(value) {
        var typeofValue = typeof value;
        if (typeofValue === 'object') {
            if (value === null) {
                return 'NULL';
            }
            else if (value.toISOString) {
                return 'TIME';
            }
            else {
                var ctrType = value.constructor.type;
                if (!ctrType) {
                    if (Plywood.Expression.isExpression(value)) {
                        throw new Error("expression used as datum value " + value.toString());
                    }
                    else {
                        throw new Error("can not have an object without a type: " + JSON.stringify(value));
                    }
                }
                if (ctrType === 'SET')
                    ctrType += '/' + value.setType;
                return ctrType;
            }
        }
        else {
            if (typeofValue !== 'boolean' && typeofValue !== 'number' && typeofValue !== 'string') {
                throw new TypeError('unsupported JS type ' + typeofValue);
            }
            return typeofValue.toUpperCase();
        }
    }
    Plywood.getValueType = getValueType;
    function getFullType(value) {
        var myType = getValueType(value);
        return myType === 'DATASET' ? value.getFullType() : { type: myType };
    }
    Plywood.getFullType = getFullType;
    function valueFromJS(v, typeOverride) {
        if (typeOverride === void 0) { typeOverride = null; }
        if (v == null) {
            return null;
        }
        else if (Array.isArray(v)) {
            return Plywood.Dataset.fromJS(v);
        }
        else if (typeof v === 'object') {
            switch (typeOverride || v.type) {
                case 'NUMBER':
                    var n = Number(v.value);
                    if (isNaN(n))
                        throw new Error("bad number value '" + String(v.value) + "'");
                    return n;
                case 'NUMBER_RANGE':
                    return Plywood.NumberRange.fromJS(v);
                case 'TIME':
                    return typeOverride ? v : new Date(v.value);
                case 'TIME_RANGE':
                    return Plywood.TimeRange.fromJS(v);
                case 'SET':
                    return Plywood.Set.fromJS(v);
                default:
                    if (v.toISOString) {
                        return v;
                    }
                    else {
                        throw new Error('can not have an object without a `type` as a datum value');
                    }
            }
        }
        else if (typeof v === 'string' && typeOverride === 'TIME') {
            return new Date(v);
        }
        return v;
    }
    Plywood.valueFromJS = valueFromJS;
    function valueToJS(v) {
        if (v == null) {
            return null;
        }
        else {
            var typeofV = typeof v;
            if (typeofV === 'object') {
                if (v.toISOString) {
                    return v;
                }
                else {
                    return v.toJS();
                }
            }
            else if (typeofV === 'number' && !isFinite(v)) {
                return String(v);
            }
        }
        return v;
    }
    Plywood.valueToJS = valueToJS;
    function valueToJSInlineType(v) {
        if (v == null) {
            return null;
        }
        else {
            var typeofV = typeof v;
            if (typeofV === 'object') {
                if (v.toISOString) {
                    return { type: 'TIME', value: v };
                }
                else {
                    var js = v.toJS();
                    if (!Array.isArray(js)) {
                        js.type = v.constructor.type;
                    }
                    return js;
                }
            }
            else if (typeofV === 'number' && !isFinite(v)) {
                return { type: 'NUMBER', value: String(v) };
            }
        }
        return v;
    }
    Plywood.valueToJSInlineType = valueToJSInlineType;
    function datumHasExternal(datum) {
        for (var name in datum) {
            var value = datum[name];
            if (value instanceof Plywood.External)
                return true;
            if (value instanceof Plywood.Dataset && value.hasExternal())
                return true;
        }
        return false;
    }
    Plywood.datumHasExternal = datumHasExternal;
    function introspectDatum(datum) {
        return Q.all(Object.keys(datum).map(function (name) {
            var external = datum[name];
            if (external instanceof Plywood.External && external.needsIntrospect()) {
                return external.introspect().then(function (newExternal) {
                    datum[name] = newExternal;
                });
            }
            return null;
        }).filter(Boolean)).then(function () { return datum; });
    }
    Plywood.introspectDatum = introspectDatum;
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    function isInteger(n) {
        return !isNaN(n) && n % 1 === 0;
    }
    function isPositiveInteger(n) {
        return isInteger(n) && 0 < n;
    }
    var check;
    var AttributeInfo = (function () {
        function AttributeInfo(parameters) {
            if (parameters.special)
                this.special = parameters.special;
            if (typeof parameters.name !== "string") {
                throw new Error("name must be a string");
            }
            this.name = parameters.name;
            if (hasOwnProperty(parameters, 'type') && typeof parameters.type !== "string") {
                throw new Error("type must be a string");
            }
            this.type = parameters.type;
            this.datasetType = parameters.datasetType;
            this.filterable = hasOwnProperty(parameters, 'filterable') ? Boolean(parameters.filterable) : true;
            this.splitable = hasOwnProperty(parameters, 'splitable') ? Boolean(parameters.splitable) : true;
        }
        AttributeInfo.isAttributeInfo = function (candidate) {
            return Plywood.isInstanceOf(candidate, AttributeInfo);
        };
        AttributeInfo.register = function (ex) {
            var op = ex.name.replace('AttributeInfo', '').replace(/^\w/, function (s) { return s.toLowerCase(); });
            AttributeInfo.classMap[op] = ex;
        };
        AttributeInfo.fromJS = function (parameters) {
            if (typeof parameters !== "object") {
                throw new Error("unrecognizable attributeMeta");
            }
            if (!hasOwnProperty(parameters, 'special')) {
                return new AttributeInfo(parameters);
            }
            var Class = AttributeInfo.classMap[parameters.special];
            if (!Class) {
                throw new Error("unsupported special attributeInfo '" + parameters.special + "'");
            }
            return Class.fromJS(parameters);
        };
        AttributeInfo.fromJSs = function (attributeJSs) {
            if (!Array.isArray(attributeJSs)) {
                if (attributeJSs && typeof attributeJSs === 'object') {
                    var newAttributeJSs = [];
                    for (var attributeName in attributeJSs) {
                        if (!hasOwnProperty(attributeJSs, attributeName))
                            continue;
                        var attributeJS = attributeJSs[attributeName];
                        attributeJS['name'] = attributeName;
                        newAttributeJSs.push(attributeJS);
                    }
                    console.warn('attributes now needs to be passed as an array like so: ' + JSON.stringify(newAttributeJSs, null, 2));
                    attributeJSs = newAttributeJSs;
                }
                else {
                    throw new TypeError("invalid attributeJSs");
                }
            }
            return attributeJSs.map(function (attributeJS) { return AttributeInfo.fromJS(attributeJS); });
        };
        AttributeInfo.toJSs = function (attributes) {
            return attributes.map(function (attribute) { return attribute.toJS(); });
        };
        AttributeInfo.applyOverrides = function (attributes, attributeOverrides) {
            attributeOverrides.forEach(function (attributeOverride) {
                var attributeOverrideName = attributeOverride.name;
                var added = false;
                attributes = attributes.map(function (a) {
                    if (a.name === attributeOverrideName) {
                        added = true;
                        return attributeOverride;
                    }
                    else {
                        return a;
                    }
                });
                if (!added) {
                    attributes = attributes.concat(attributeOverride);
                }
            });
            return attributes;
        };
        AttributeInfo.prototype._ensureSpecial = function (special) {
            if (!this.special) {
                this.special = special;
                return;
            }
            if (this.special !== special) {
                throw new TypeError("incorrect attributeInfo `special` '" + this.special + "' (needs to be: '" + special + "')");
            }
        };
        AttributeInfo.prototype._ensureType = function (myType) {
            if (!this.type) {
                this.type = myType;
                return;
            }
            if (this.type !== myType) {
                throw new TypeError("incorrect attributeInfo `type` '" + this.type + "' (needs to be: '" + myType + "')");
            }
        };
        AttributeInfo.prototype.toString = function () {
            var special = this.special || 'basic';
            return special + "(" + this.type + ")";
        };
        AttributeInfo.prototype.valueOf = function () {
            var value = {
                name: this.name,
                type: this.type,
                filterable: this.filterable,
                splitable: this.splitable
            };
            if (this.special)
                value.special = this.special;
            if (this.datasetType)
                value.datasetType = this.datasetType;
            return value;
        };
        AttributeInfo.prototype.toJS = function () {
            var js = {
                name: this.name,
                type: this.type
            };
            if (!this.filterable)
                js.filterable = false;
            if (!this.splitable)
                js.splitable = false;
            if (this.special)
                js.special = this.special;
            if (this.datasetType)
                js.datasetType = this.datasetType;
            return js;
        };
        AttributeInfo.prototype.toJSON = function () {
            return this.toJS();
        };
        AttributeInfo.prototype.equals = function (other) {
            return AttributeInfo.isAttributeInfo(other) &&
                this.special === other.special &&
                this.name === other.name &&
                this.type === other.type;
        };
        AttributeInfo.prototype.serialize = function (value) {
            return value;
        };
        AttributeInfo.classMap = {};
        return AttributeInfo;
    })();
    Plywood.AttributeInfo = AttributeInfo;
    check = AttributeInfo;
    var RangeAttributeInfo = (function (_super) {
        __extends(RangeAttributeInfo, _super);
        function RangeAttributeInfo(parameters) {
            _super.call(this, parameters);
            this.separator = parameters.separator;
            this.rangeSize = parameters.rangeSize;
            this.digitsBeforeDecimal = parameters.digitsBeforeDecimal;
            this.digitsAfterDecimal = parameters.digitsAfterDecimal;
            this._ensureSpecial("range");
            this._ensureType('NUMBER_RANGE');
            this.separator || (this.separator = ";");
            if (!(typeof this.separator === "string" && this.separator.length)) {
                throw new TypeError("`separator` must be a non-empty string");
            }
            if (typeof this.rangeSize !== "number") {
                throw new TypeError("`rangeSize` must be a number");
            }
            if (this.rangeSize > 1) {
                if (!isInteger(this.rangeSize)) {
                    throw new Error("`rangeSize` greater than 1 must be an integer");
                }
            }
            else {
                if (!isInteger(1 / this.rangeSize)) {
                    throw new Error("`rangeSize` less than 1 must divide 1");
                }
            }
            if (this.digitsBeforeDecimal != null) {
                if (!isPositiveInteger(this.digitsBeforeDecimal)) {
                    throw new Error("`digitsBeforeDecimal` must be a positive integer");
                }
            }
            else {
                this.digitsBeforeDecimal = null;
            }
            if (this.digitsAfterDecimal != null) {
                if (!isPositiveInteger(this.digitsAfterDecimal)) {
                    throw new Error("`digitsAfterDecimal` must be a positive integer");
                }
                var digitsInSize = (String(this.rangeSize).split(".")[1] || "").length;
                if (this.digitsAfterDecimal < digitsInSize) {
                    throw new Error("`digitsAfterDecimal` must be at least " + digitsInSize + " to accommodate for a `rangeSize` of " + this.rangeSize);
                }
            }
            else {
                this.digitsAfterDecimal = null;
            }
        }
        RangeAttributeInfo.fromJS = function (parameters) {
            return new RangeAttributeInfo(parameters);
        };
        RangeAttributeInfo.prototype.valueOf = function () {
            var attributeMetaSpec = _super.prototype.valueOf.call(this);
            if (this.separator !== ";") {
                attributeMetaSpec.separator = this.separator;
            }
            attributeMetaSpec.rangeSize = this.rangeSize;
            if (this.digitsBeforeDecimal !== null) {
                attributeMetaSpec.digitsBeforeDecimal = this.digitsBeforeDecimal;
            }
            if (this.digitsAfterDecimal !== null) {
                attributeMetaSpec.digitsAfterDecimal = this.digitsAfterDecimal;
            }
            return attributeMetaSpec;
        };
        RangeAttributeInfo.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.separator === other.separator &&
                this.rangeSize === other.rangeSize &&
                this.digitsBeforeDecimal === other.digitsBeforeDecimal &&
                this.digitsAfterDecimal === other.digitsAfterDecimal;
        };
        RangeAttributeInfo.prototype._serializeNumber = function (value) {
            if (value === null)
                return "";
            var valueStr = String(value);
            if (this.digitsBeforeDecimal === null && this.digitsAfterDecimal === null) {
                return valueStr;
            }
            var valueStrSplit = valueStr.split(".");
            var before = valueStrSplit[0];
            var after = valueStrSplit[1];
            if (this.digitsBeforeDecimal) {
                before = repeat("0", this.digitsBeforeDecimal - before.length) + before;
            }
            if (this.digitsAfterDecimal) {
                after || (after = "");
                after += repeat("0", this.digitsAfterDecimal - after.length);
            }
            valueStr = before;
            if (after)
                valueStr += "." + after;
            return valueStr;
        };
        RangeAttributeInfo.prototype.serialize = function (range) {
            if (!(Array.isArray(range) && range.length === 2))
                return null;
            return this._serializeNumber(range[0]) + this.separator + this._serializeNumber(range[1]);
        };
        RangeAttributeInfo.prototype.getMatchingRegExpString = function () {
            var separatorRegExp = this.separator.replace(/[.$^{[(|)*+?\\]/g, function (c) { return "\\" + c; });
            var beforeRegExp = this.digitsBeforeDecimal ? "-?\\d{" + this.digitsBeforeDecimal + "}" : "(?:-?[1-9]\\d*|0)";
            var afterRegExp = this.digitsAfterDecimal ? "\\.\\d{" + this.digitsAfterDecimal + "}" : "(?:\\.\\d*[1-9])?";
            var numberRegExp = beforeRegExp + afterRegExp;
            return "/^(" + numberRegExp + ")" + separatorRegExp + "(" + numberRegExp + ")$/";
        };
        return RangeAttributeInfo;
    })(AttributeInfo);
    Plywood.RangeAttributeInfo = RangeAttributeInfo;
    AttributeInfo.register(RangeAttributeInfo);
    var UniqueAttributeInfo = (function (_super) {
        __extends(UniqueAttributeInfo, _super);
        function UniqueAttributeInfo(parameters) {
            _super.call(this, parameters);
            this._ensureSpecial("unique");
            this._ensureType('STRING');
        }
        UniqueAttributeInfo.fromJS = function (parameters) {
            return new UniqueAttributeInfo(parameters);
        };
        UniqueAttributeInfo.prototype.serialize = function (value) {
            throw new Error("can not serialize an approximate unique value");
        };
        return UniqueAttributeInfo;
    })(AttributeInfo);
    Plywood.UniqueAttributeInfo = UniqueAttributeInfo;
    AttributeInfo.register(UniqueAttributeInfo);
    var HistogramAttributeInfo = (function (_super) {
        __extends(HistogramAttributeInfo, _super);
        function HistogramAttributeInfo(parameters) {
            _super.call(this, parameters);
            this._ensureSpecial("histogram");
            this._ensureType('NUMBER');
        }
        HistogramAttributeInfo.fromJS = function (parameters) {
            return new HistogramAttributeInfo(parameters);
        };
        HistogramAttributeInfo.prototype.serialize = function (value) {
            throw new Error("can not serialize a histogram value");
        };
        return HistogramAttributeInfo;
    })(AttributeInfo);
    Plywood.HistogramAttributeInfo = HistogramAttributeInfo;
    AttributeInfo.register(HistogramAttributeInfo);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var BOUNDS_REG_EXP = /^[\[(][\])]$/;
    var Range = (function () {
        function Range(start, end, bounds) {
            if (bounds) {
                if (!BOUNDS_REG_EXP.test(bounds)) {
                    throw new Error("invalid bounds " + bounds);
                }
            }
            else {
                bounds = Range.DEFAULT_BOUNDS;
            }
            if (start !== null && end !== null && this._endpointEqual(start, end)) {
                if (bounds !== '[]') {
                    start = end = this._zeroEndpoint();
                }
                if (bounds === '(]' || bounds === '()')
                    this.bounds = '[)';
            }
            else {
                if (start !== null && end !== null && end < start) {
                    throw new Error('must have start <= end');
                }
                if (start === null && bounds[0] === '[') {
                    bounds = '(' + bounds[1];
                }
                if (end === null && bounds[1] === ']') {
                    bounds = bounds[0] + ')';
                }
            }
            this.start = start;
            this.end = end;
            this.bounds = bounds;
        }
        Range.fromJS = function (parameters) {
            if (typeof parameters.start === 'number' || typeof parameters.end === 'number') {
                return Plywood.NumberRange.fromJS(parameters);
            }
            else {
                return Plywood.TimeRange.fromJS(parameters);
            }
        };
        Range.prototype._zeroEndpoint = function () {
            return 0;
        };
        Range.prototype._endpointEqual = function (a, b) {
            return a === b;
        };
        Range.prototype._endpointToString = function (a) {
            return String(a);
        };
        Range.prototype._equalsHelper = function (other) {
            return Boolean(other) &&
                this.bounds === other.bounds &&
                this._endpointEqual(this.start, other.start) &&
                this._endpointEqual(this.end, other.end);
        };
        Range.prototype.toString = function () {
            var bounds = this.bounds;
            return bounds[0] + this._endpointToString(this.start) + ',' + this._endpointToString(this.end) + bounds[1];
        };
        Range.prototype.openStart = function () {
            return this.bounds[0] === '(';
        };
        Range.prototype.openEnd = function () {
            return this.bounds[1] === ')';
        };
        Range.prototype.empty = function () {
            return this._endpointEqual(this.start, this.end) && this.bounds === '[)';
        };
        Range.prototype.degenerate = function () {
            return this._endpointEqual(this.start, this.end) && this.bounds === '[]';
        };
        Range.prototype.contains = function (val) {
            if (val === null)
                return false;
            var start = this.start;
            var end = this.end;
            var bounds = this.bounds;
            if (bounds[0] === '[') {
                if (val < start)
                    return false;
            }
            else {
                if (start !== null && val <= start)
                    return false;
            }
            if (bounds[1] === ']') {
                if (end < val)
                    return false;
            }
            else {
                if (end !== null && end <= val)
                    return false;
            }
            return true;
        };
        Range.prototype.intersects = function (other) {
            return this.contains(other.start) || this.contains(other.end)
                || other.contains(this.start) || other.contains(this.end)
                || this._equalsHelper(other);
        };
        Range.prototype.adjacent = function (other) {
            return (this._endpointEqual(this.end, other.start) && this.openEnd() !== other.openStart())
                || (this._endpointEqual(this.start, other.end) && this.openStart() !== other.openEnd());
        };
        Range.prototype.mergeable = function (other) {
            return this.intersects(other) || this.adjacent(other);
        };
        Range.prototype.union = function (other) {
            if (!this.mergeable(other))
                return null;
            return this.extend(other);
        };
        Range.prototype.extend = function (other) {
            var thisStart = this.start;
            var thisEnd = this.end;
            var otherStart = other.start;
            var otherEnd = other.end;
            var start;
            var startBound;
            if (thisStart === null || otherStart === null) {
                start = null;
                startBound = '(';
            }
            else if (thisStart < otherStart) {
                start = thisStart;
                startBound = this.bounds[0];
            }
            else {
                start = otherStart;
                startBound = other.bounds[0];
            }
            var end;
            var endBound;
            if (thisEnd === null || otherEnd === null) {
                end = null;
                endBound = ')';
            }
            else if (thisEnd < otherEnd) {
                end = otherEnd;
                endBound = other.bounds[1];
            }
            else {
                end = thisEnd;
                endBound = this.bounds[1];
            }
            return new this.constructor({ start: start, end: end, bounds: startBound + endBound });
        };
        Range.prototype.intersect = function (other) {
            if (!this.mergeable(other))
                return null;
            var thisStart = this.start;
            var thisEnd = this.end;
            var otherStart = other.start;
            var otherEnd = other.end;
            var start;
            var startBound;
            if (thisStart === null || otherStart === null) {
                if (otherStart === null) {
                    start = thisStart;
                    startBound = this.bounds[0];
                }
                else {
                    start = otherStart;
                    startBound = other.bounds[0];
                }
            }
            else if (otherStart < thisStart) {
                start = thisStart;
                startBound = this.bounds[0];
            }
            else {
                start = otherStart;
                startBound = other.bounds[0];
            }
            var end;
            var endBound;
            if (thisEnd === null || otherEnd === null) {
                if (thisEnd == null) {
                    end = otherEnd;
                    endBound = other.bounds[1];
                }
                else {
                    end = thisEnd;
                    endBound = this.bounds[1];
                }
            }
            else if (otherEnd < thisEnd) {
                end = otherEnd;
                endBound = other.bounds[1];
            }
            else {
                end = thisEnd;
                endBound = this.bounds[1];
            }
            return new this.constructor({ start: start, end: end, bounds: startBound + endBound });
        };
        Range.DEFAULT_BOUNDS = '[)';
        return Range;
    })();
    Plywood.Range = Range;
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    function finiteOrNull(n) {
        return (isNaN(n) || isFinite(n)) ? n : null;
    }
    var check;
    var NumberRange = (function (_super) {
        __extends(NumberRange, _super);
        function NumberRange(parameters) {
            if (isNaN(parameters.start))
                throw new TypeError('`start` must be a number');
            if (isNaN(parameters.end))
                throw new TypeError('`end` must be a number');
            _super.call(this, parameters.start, parameters.end, parameters.bounds);
        }
        NumberRange.isNumberRange = function (candidate) {
            return Plywood.isInstanceOf(candidate, NumberRange);
        };
        NumberRange.numberBucket = function (num, size, offset) {
            var start = Math.floor((num - offset) / size) * size + offset;
            return new NumberRange({
                start: start,
                end: start + size,
                bounds: Plywood.Range.DEFAULT_BOUNDS
            });
        };
        NumberRange.fromNumber = function (n) {
            return new NumberRange({ start: n, end: n, bounds: '[]' });
        };
        NumberRange.fromJS = function (parameters) {
            if (typeof parameters !== "object") {
                throw new Error("unrecognizable numberRange");
            }
            var start = parameters.start;
            var end = parameters.end;
            return new NumberRange({
                start: start === null ? null : finiteOrNull(Number(start)),
                end: end === null ? null : finiteOrNull(Number(end)),
                bounds: parameters.bounds
            });
        };
        NumberRange.prototype.valueOf = function () {
            return {
                start: this.start,
                end: this.end,
                bounds: this.bounds
            };
        };
        NumberRange.prototype.toJS = function () {
            var js = {
                start: this.start,
                end: this.end
            };
            if (this.bounds !== Plywood.Range.DEFAULT_BOUNDS)
                js.bounds = this.bounds;
            return js;
        };
        NumberRange.prototype.toJSON = function () {
            return this.toJS();
        };
        NumberRange.prototype.equals = function (other) {
            return NumberRange.isNumberRange(other) && this._equalsHelper(other);
        };
        NumberRange.prototype.midpoint = function () {
            return (this.start + this.end) / 2;
        };
        NumberRange.type = 'NUMBER_RANGE';
        return NumberRange;
    })(Plywood.Range);
    Plywood.NumberRange = NumberRange;
    check = NumberRange;
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    function dateString(date) {
        return date.toISOString();
    }
    function arrayFromJS(xs, setType) {
        return xs.map(function (x) { return Plywood.valueFromJS(x, setType); });
    }
    function unifyElements(elements) {
        var newElements = Object.create(null);
        for (var _i = 0; _i < elements.length; _i++) {
            var accumulator = elements[_i];
            var newElementsKeys = Object.keys(newElements);
            for (var _a = 0; _a < newElementsKeys.length; _a++) {
                var newElementsKey = newElementsKeys[_a];
                var newElement = newElements[newElementsKey];
                var unionElement = accumulator.union(newElement);
                if (unionElement) {
                    accumulator = unionElement;
                    delete newElements[newElementsKey];
                }
            }
            newElements[accumulator.toString()] = accumulator;
        }
        return Object.keys(newElements).map(function (k) { return newElements[k]; });
    }
    function intersectElements(elements1, elements2) {
        var newElements = [];
        for (var _i = 0; _i < elements1.length; _i++) {
            var element1 = elements1[_i];
            for (var _a = 0; _a < elements2.length; _a++) {
                var element2 = elements2[_a];
                var intersect = element1.intersect(element2);
                if (intersect)
                    newElements.push(intersect);
            }
        }
        return newElements;
    }
    var typeUpgrades = {
        'NUMBER': 'NUMBER_RANGE',
        'TIME': 'TIME_RANGE'
    };
    var check;
    var Set = (function () {
        function Set(parameters) {
            var setType = parameters.setType;
            this.setType = setType;
            var keyFn = setType === 'TIME' ? dateString : String;
            this.keyFn = keyFn;
            var elements = parameters.elements;
            var newElements = null;
            var hash = Object.create(null);
            for (var i = 0; i < elements.length; i++) {
                var element = elements[i];
                var key = keyFn(element);
                if (hash[key]) {
                    if (!newElements)
                        newElements = elements.slice(0, i);
                }
                else {
                    hash[key] = element;
                    if (newElements)
                        newElements.push(element);
                }
            }
            if (newElements) {
                elements = newElements;
            }
            if (setType === 'NUMBER_RANGE' || setType === 'TIME_RANGE') {
                elements = unifyElements(elements);
            }
            this.elements = elements;
            this.hash = hash;
        }
        Set.isSet = function (candidate) {
            return Plywood.isInstanceOf(candidate, Set);
        };
        Set.convertToSet = function (thing) {
            var thingType = Plywood.getValueType(thing);
            if (thingType.indexOf('SET/') === 0)
                return thing;
            return Set.fromJS({ setType: thingType, elements: [thing] });
        };
        Set.generalUnion = function (a, b) {
            var aSet = Set.convertToSet(a);
            var bSet = Set.convertToSet(b);
            var aSetType = aSet.setType;
            var bSetType = bSet.setType;
            if (typeUpgrades[aSetType] === bSetType) {
                aSet = aSet.upgradeType();
            }
            else if (typeUpgrades[bSetType] === aSetType) {
                bSet = bSet.upgradeType();
            }
            else if (aSetType !== bSetType) {
                return null;
            }
            return aSet.union(bSet).simplify();
        };
        Set.generalIntersect = function (a, b) {
            var aSet = Set.convertToSet(a);
            var bSet = Set.convertToSet(b);
            var aSetType = aSet.setType;
            var bSetType = bSet.setType;
            if (typeUpgrades[aSetType] === bSetType) {
                aSet = aSet.upgradeType();
            }
            else if (typeUpgrades[bSetType] === aSetType) {
                bSet = bSet.upgradeType();
            }
            else if (aSetType !== bSetType) {
                return null;
            }
            return aSet.intersect(bSet).simplify();
        };
        Set.fromJS = function (parameters) {
            if (Array.isArray(parameters)) {
                parameters = { elements: parameters };
            }
            if (typeof parameters !== "object") {
                throw new Error("unrecognizable set");
            }
            var setType = parameters.setType;
            var elements = parameters.elements;
            if (!setType) {
                setType = Plywood.getValueType(elements.length ? elements[0] : null);
            }
            return new Set({
                setType: setType,
                elements: arrayFromJS(elements, setType)
            });
        };
        Set.prototype.valueOf = function () {
            return {
                setType: this.setType,
                elements: this.elements
            };
        };
        Set.prototype.toJS = function () {
            return {
                setType: this.setType,
                elements: this.elements.map(Plywood.valueToJS)
            };
        };
        Set.prototype.toJSON = function () {
            return this.toJS();
        };
        Set.prototype.toString = function () {
            return 'SET_' + this.setType + '(' + Object.keys(this.elements).length + ')';
        };
        Set.prototype.equals = function (other) {
            return Set.isSet(other) &&
                this.setType === other.setType &&
                this.elements.length === other.elements.length &&
                this.elements.slice().sort().join('') === other.elements.slice().sort().join('');
        };
        Set.prototype.size = function () {
            return this.elements.length;
        };
        Set.prototype.empty = function () {
            return this.elements.length === 0;
        };
        Set.prototype.simplify = function () {
            var simpleSet = this.downgradeType();
            var simpleSetElements = simpleSet.elements;
            return simpleSetElements.length === 1 ? simpleSetElements[0] : simpleSet;
        };
        Set.prototype.upgradeType = function () {
            if (this.setType === 'NUMBER') {
                return Set.fromJS({
                    setType: 'NUMBER_RANGE',
                    elements: this.elements.map(Plywood.NumberRange.fromNumber)
                });
            }
            else if (this.setType === 'TIME') {
                return Set.fromJS({
                    setType: 'TIME_RANGE',
                    elements: this.elements.map(Plywood.TimeRange.fromTime)
                });
            }
            else {
                return this;
            }
        };
        Set.prototype.downgradeType = function () {
            if (this.setType === 'NUMBER_RANGE' || this.setType === 'TIME_RANGE') {
                var elements = this.elements;
                var simpleElements = [];
                for (var _i = 0; _i < elements.length; _i++) {
                    var element = elements[_i];
                    if (element.degenerate()) {
                        simpleElements.push(element.start);
                    }
                    else {
                        return this;
                    }
                }
                return Set.fromJS(simpleElements);
            }
            else {
                return this;
            }
        };
        Set.prototype.extent = function () {
            var setType = this.setType;
            if (hasOwnProperty(typeUpgrades, setType)) {
                return this.upgradeType().extent();
            }
            if (setType !== 'NUMBER_RANGE' && setType !== 'TIME_RANGE')
                return null;
            var elements = this.elements;
            var extent = elements[0] || null;
            for (var i = 1; i < elements.length; i++) {
                extent = extent.extend(elements[i]);
            }
            return extent;
        };
        Set.prototype.union = function (other) {
            if (this.empty())
                return other;
            if (other.empty())
                return this;
            if (this.setType !== other.setType) {
                throw new TypeError("can not union sets of different types");
            }
            var newElements = this.elements.slice();
            var otherElements = other.elements;
            for (var _i = 0; _i < otherElements.length; _i++) {
                var el = otherElements[_i];
                if (this.contains(el))
                    continue;
                newElements.push(el);
            }
            return new Set({
                setType: this.setType,
                elements: newElements
            });
        };
        Set.prototype.intersect = function (other) {
            if (this.empty() || other.empty())
                return Set.EMPTY;
            var setType = this.setType;
            if (this.setType !== other.setType) {
                throw new TypeError("can not intersect sets of different types");
            }
            var thisElements = this.elements;
            var newElements;
            if (setType === 'NUMBER_RANGE' || setType === 'TIME_RANGE') {
                var otherElements = other.elements;
                newElements = intersectElements(thisElements, otherElements);
            }
            else {
                newElements = [];
                for (var _i = 0; _i < thisElements.length; _i++) {
                    var el = thisElements[_i];
                    if (!other.contains(el))
                        continue;
                    newElements.push(el);
                }
            }
            return new Set({
                setType: this.setType,
                elements: newElements
            });
        };
        Set.prototype.contains = function (value) {
            var setType = this.setType;
            if ((setType === 'NUMBER_RANGE' && typeof value === 'number')
                || (setType === 'TIME_RANGE' && Plywood.isDate(value))) {
                return this.containsWithin(value);
            }
            return hasOwnProperty(this.hash, this.keyFn(value));
        };
        Set.prototype.containsWithin = function (value) {
            var elements = this.elements;
            for (var k in elements) {
                if (!hasOwnProperty(elements, k))
                    continue;
                if (elements[k].contains(value))
                    return true;
            }
            return false;
        };
        Set.prototype.add = function (value) {
            var setType = this.setType;
            var valueType = Plywood.getValueType(value);
            if (setType === 'NULL')
                setType = valueType;
            if (valueType !== 'NULL' && setType !== valueType)
                throw new Error('value type must match');
            if (this.contains(value))
                return this;
            return new Set({
                setType: setType,
                elements: this.elements.concat([value])
            });
        };
        Set.prototype.remove = function (value) {
            if (!this.contains(value))
                return this;
            var keyFn = this.keyFn;
            var key = keyFn(value);
            return new Set({
                setType: this.setType,
                elements: this.elements.filter(function (element) { return keyFn(element) !== key; })
            });
        };
        Set.prototype.toggle = function (value) {
            return this.contains(value) ? this.remove(value) : this.add(value);
        };
        Set.type = 'SET';
        return Set;
    })();
    Plywood.Set = Set;
    check = Set;
    Set.EMPTY = Set.fromJS([]);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    function toDate(date, name) {
        if (date === null)
            return null;
        if (typeof date === "undefined")
            throw new TypeError("timeRange must have a " + name);
        if (typeof date === 'string' || typeof date === 'number')
            date = new Date(date);
        if (!date.getDay)
            throw new TypeError("timeRange must have a " + name + " that is a Date");
        return date;
    }
    var START_OF_TIME = "1000-01-01";
    var END_OF_TIME = "3000-01-01";
    function dateToIntervalPart(date) {
        return date.toISOString()
            .replace('Z', '')
            .replace('.000', '')
            .replace(/:00$/, '')
            .replace(/:00$/, '')
            .replace(/T00$/, '');
    }
    var check;
    var TimeRange = (function (_super) {
        __extends(TimeRange, _super);
        function TimeRange(parameters) {
            _super.call(this, parameters.start, parameters.end, parameters.bounds);
        }
        TimeRange.isTimeRange = function (candidate) {
            return Plywood.isInstanceOf(candidate, TimeRange);
        };
        TimeRange.intervalFromDate = function (date) {
            return dateToIntervalPart(date) + '/' + dateToIntervalPart(new Date(date.valueOf() + 1));
        };
        TimeRange.timeBucket = function (date, duration, timezone) {
            if (!date)
                return null;
            var start = duration.floor(date, timezone);
            return new TimeRange({
                start: start,
                end: duration.move(start, timezone, 1),
                bounds: Plywood.Range.DEFAULT_BOUNDS
            });
        };
        TimeRange.fromTime = function (t) {
            return new TimeRange({ start: t, end: t, bounds: '[]' });
        };
        TimeRange.fromJS = function (parameters) {
            if (typeof parameters !== "object") {
                throw new Error("unrecognizable timeRange");
            }
            return new TimeRange({
                start: toDate(parameters.start, 'start'),
                end: toDate(parameters.end, 'end'),
                bounds: parameters.bounds
            });
        };
        TimeRange.prototype._zeroEndpoint = function () {
            return new Date(0);
        };
        TimeRange.prototype._endpointEqual = function (a, b) {
            if (a === null) {
                return b === null;
            }
            else {
                return b !== null && a.valueOf() === b.valueOf();
            }
        };
        TimeRange.prototype._endpointToString = function (a) {
            if (!a)
                return 'null';
            return a.toISOString();
        };
        TimeRange.prototype.valueOf = function () {
            return {
                start: this.start,
                end: this.end,
                bounds: this.bounds
            };
        };
        TimeRange.prototype.toJS = function () {
            var js = {
                start: this.start,
                end: this.end
            };
            if (this.bounds !== Plywood.Range.DEFAULT_BOUNDS)
                js.bounds = this.bounds;
            return js;
        };
        TimeRange.prototype.toJSON = function () {
            return this.toJS();
        };
        TimeRange.prototype.equals = function (other) {
            return TimeRange.isTimeRange(other) && this._equalsHelper(other);
        };
        TimeRange.prototype.toInterval = function () {
            var _a = this, start = _a.start, end = _a.end, bounds = _a.bounds;
            var interval = [START_OF_TIME, END_OF_TIME];
            if (start) {
                if (bounds[0] === '(')
                    start = new Date(start.valueOf() + 1);
                interval[0] = dateToIntervalPart(start);
            }
            if (end) {
                if (bounds[1] === ']')
                    end = new Date(end.valueOf() + 1);
                interval[1] = dateToIntervalPart(end);
            }
            return interval.join("/");
        };
        TimeRange.prototype.midpoint = function () {
            return new Date((this.start.valueOf() + this.end.valueOf()) / 2);
        };
        TimeRange.type = 'TIME_RANGE';
        return TimeRange;
    })(Plywood.Range);
    Plywood.TimeRange = TimeRange;
    check = TimeRange;
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    function foldContext(d, c) {
        var newContext = Object.create(c);
        for (var k in d) {
            newContext[k] = d[k];
        }
        return newContext;
    }
    Plywood.foldContext = foldContext;
    var directionFns = {
        ascending: function (a, b) {
            if (a == null) {
                return b == null ? 0 : -1;
            }
            else {
                if (a.compare)
                    return a.compare(b);
                if (b == null)
                    return 1;
            }
            return a < b ? -1 : a > b ? 1 : a >= b ? 0 : NaN;
        },
        descending: function (a, b) {
            if (b == null) {
                return a == null ? 0 : -1;
            }
            else {
                if (b.compare)
                    return b.compare(a);
                if (a == null)
                    return 1;
            }
            return b < a ? -1 : b > a ? 1 : b >= a ? 0 : NaN;
        }
    };
    function typePreference(type) {
        switch (type) {
            case 'TIME': return 0;
            case 'STRING': return 1;
            case 'DATASET': return 5;
            default: return 2;
        }
    }
    function sortColumns(columns) {
        return columns.sort(function (c1, c2) {
            var typeDiff = typePreference(c1.type) - typePreference(c2.type);
            if (typeDiff)
                return typeDiff;
            return c1.name.localeCompare(c2.name);
        });
    }
    function uniqueColumns(columns) {
        var seen = {};
        var uniqueColumns = [];
        for (var _i = 0; _i < columns.length; _i++) {
            var column = columns[_i];
            if (!seen[column.name]) {
                uniqueColumns.push(column);
                seen[column.name] = true;
            }
        }
        return uniqueColumns;
    }
    function flattenColumns(nestedColumns, prefixColumns) {
        var flatColumns = [];
        var i = 0;
        var prefixString = '';
        while (i < nestedColumns.length) {
            var nestedColumn = nestedColumns[i];
            if (nestedColumn.type === 'DATASET') {
                nestedColumns = nestedColumn.columns;
                if (prefixColumns)
                    prefixString += nestedColumn.name + '.';
                i = 0;
            }
            else {
                flatColumns.push({
                    name: prefixString + nestedColumn.name,
                    type: nestedColumn.type
                });
                i++;
            }
        }
        return sortColumns(uniqueColumns(flatColumns));
    }
    var typeOrder = {
        'NULL': 0,
        'TIME': 1,
        'TIME_RANGE': 2,
        'SET/TIME': 3,
        'SET/TIME_RANGE': 4,
        'STRING': 5,
        'SET/STRING': 6,
        'BOOLEAN': 7,
        'NUMBER': 8,
        'NUMBER_RANGE': 9,
        'SET/NUMBER': 10,
        'SET/NUMBER_RANGE': 11,
        'DATASET': 12
    };
    var defaultFormatter = {
        'NULL': function (v) { return 'NULL'; },
        'TIME': function (v) { return v.toISOString(); },
        'TIME_RANGE': function (v) { return String(v); },
        'SET/TIME': function (v) { return String(v); },
        'SET/TIME_RANGE': function (v) { return String(v); },
        'STRING': function (v) {
            v = '' + v;
            if (v.indexOf('"') === -1)
                return v;
            return '"' + v.replace(/"/g, '""') + '"';
        },
        'SET/STRING': function (v) { return String(v); },
        'BOOLEAN': function (v) { return String(v); },
        'NUMBER': function (v) { return String(v); },
        'NUMBER_RANGE': function (v) { return String(v); },
        'SET/NUMBER': function (v) { return String(v); },
        'SET/NUMBER_RANGE': function (v) { return String(v); },
        'DATASET': function (v) { return 'DATASET'; }
    };
    function isDate(dt) {
        return Boolean(dt && dt.toISOString);
    }
    Plywood.isDate = isDate;
    function isBoolean(b) {
        return b === true || b === false;
    }
    function isNumber(n) {
        return n !== null && !isNaN(Number(n));
    }
    function isString(str) {
        return typeof str === "string";
    }
    function getAttributeInfo(name, attributeValue) {
        if (attributeValue == null)
            return null;
        if (isDate(attributeValue)) {
            return new Plywood.AttributeInfo({ name: name, type: 'TIME' });
        }
        else if (isBoolean(attributeValue)) {
            return new Plywood.AttributeInfo({ name: name, type: 'BOOLEAN' });
        }
        else if (isNumber(attributeValue)) {
            return new Plywood.AttributeInfo({ name: name, type: 'NUMBER' });
        }
        else if (isString(attributeValue)) {
            return new Plywood.AttributeInfo({ name: name, type: 'STRING' });
        }
        else if (Plywood.NumberRange.isNumberRange(attributeValue)) {
            return new Plywood.AttributeInfo({ name: name, type: 'NUMBER_RANGE' });
        }
        else if (Plywood.TimeRange.isTimeRange(attributeValue)) {
            return new Plywood.AttributeInfo({ name: name, type: 'TIME_RANGE' });
        }
        else if (attributeValue instanceof Dataset) {
            return new Plywood.AttributeInfo({ name: name, type: 'DATASET', datasetType: attributeValue.getFullType().datasetType });
        }
        else {
            throw new Error("Could not introspect");
        }
    }
    function datumFromJS(js) {
        if (typeof js !== 'object')
            throw new TypeError("datum must be an object");
        var datum = Object.create(null);
        for (var k in js) {
            if (!hasOwnProperty(js, k))
                continue;
            datum[k] = Plywood.valueFromJS(js[k]);
        }
        return datum;
    }
    function datumToJS(datum) {
        var js = {};
        for (var k in datum) {
            var v = datum[k];
            if (v && v.suppress === true)
                continue;
            js[k] = Plywood.valueToJSInlineType(v);
        }
        return js;
    }
    function joinDatums(datumA, datumB) {
        var newDatum = Object.create(null);
        for (var k in datumA) {
            newDatum[k] = datumA[k];
        }
        for (var k in datumB) {
            newDatum[k] = datumB[k];
        }
        return newDatum;
    }
    function copy(obj) {
        var newObj = {};
        var k;
        for (k in obj) {
            if (hasOwnProperty(obj, k))
                newObj[k] = obj[k];
        }
        return newObj;
    }
    var check;
    var Dataset = (function () {
        function Dataset(parameters) {
            this.attributes = null;
            this.attributeOverrides = null;
            this.keys = null;
            if (parameters.suppress === true)
                this.suppress = true;
            if (parameters.attributes) {
                this.attributes = parameters.attributes;
            }
            if (parameters.attributeOverrides) {
                this.attributeOverrides = parameters.attributeOverrides;
            }
            if (parameters.keys) {
                this.keys = parameters.keys;
            }
            var data = parameters.data;
            if (Array.isArray(data)) {
                this.data = data;
            }
            else {
                throw new TypeError("must have a `data` array");
            }
        }
        Dataset.isDataset = function (candidate) {
            return Plywood.isInstanceOf(candidate, Dataset);
        };
        Dataset.fromJS = function (parameters) {
            if (Array.isArray(parameters)) {
                parameters = { data: parameters };
            }
            if (!Array.isArray(parameters.data)) {
                throw new Error('must have data');
            }
            var value = {};
            if (hasOwnProperty(parameters, 'attributes')) {
                value.attributes = Plywood.AttributeInfo.fromJSs(parameters.attributes);
            }
            else if (hasOwnProperty(parameters, 'attributeOverrides')) {
                value.attributeOverrides = Plywood.AttributeInfo.fromJSs(parameters.attributeOverrides);
            }
            value.keys = parameters.keys;
            value.data = parameters.data.map(datumFromJS);
            return new Dataset(value);
        };
        Dataset.prototype.valueOf = function () {
            var value = {};
            if (this.suppress)
                value.suppress = true;
            if (this.attributes)
                value.attributes = this.attributes;
            if (this.attributeOverrides)
                value.attributeOverrides = this.attributeOverrides;
            if (this.keys)
                value.keys = this.keys;
            value.data = this.data;
            return value;
        };
        Dataset.prototype.toJS = function () {
            return this.data.map(datumToJS);
        };
        Dataset.prototype.toString = function () {
            return "Dataset(" + this.data.length + ")";
        };
        Dataset.prototype.toJSON = function () {
            return this.toJS();
        };
        Dataset.prototype.equals = function (other) {
            return Dataset.isDataset(other) &&
                this.data.length === other.data.length;
        };
        Dataset.prototype.hide = function () {
            var value = this.valueOf();
            value.suppress = true;
            return new Dataset(value);
        };
        Dataset.prototype.basis = function () {
            var data = this.data;
            return data.length === 1 && Object.keys(data[0]).length === 0;
        };
        Dataset.prototype.hasExternal = function () {
            if (!this.data.length)
                return false;
            return Plywood.datumHasExternal(this.data[0]);
        };
        Dataset.prototype.getFullType = function () {
            this.introspect();
            var attributes = this.attributes;
            if (!attributes)
                throw new Error("dataset has not been introspected");
            var myDatasetType = {};
            for (var _i = 0; _i < attributes.length; _i++) {
                var attribute = attributes[_i];
                var attrName = attribute.name;
                if (attribute.type === 'DATASET') {
                    myDatasetType[attrName] = {
                        type: 'DATASET',
                        datasetType: attribute.datasetType
                    };
                }
                else {
                    myDatasetType[attrName] = {
                        type: attribute.type
                    };
                }
            }
            var myFullType = {
                type: 'DATASET',
                datasetType: myDatasetType
            };
            return myFullType;
        };
        Dataset.prototype.apply = function (name, exFn, context) {
            var data = this.data;
            for (var _i = 0; _i < data.length; _i++) {
                var datum = data[_i];
                datum[name] = exFn(datum, context);
            }
            this.attributes = null;
            return this;
        };
        Dataset.prototype.applyPromise = function (name, exFn, context) {
            var _this = this;
            var ds = this;
            var promises = this.data.map(function (datum) { return exFn(datum, context); });
            return Q.all(promises).then(function (values) {
                var data = ds.data;
                var n = data.length;
                for (var i = 0; i < n; i++)
                    data[i][name] = values[i];
                _this.attributes = null;
                return ds;
            });
        };
        Dataset.prototype.filter = function (exFn, context) {
            var value = this.valueOf();
            value.data = this.data.filter(function (datum) { return exFn(datum, context); });
            return new Dataset(value);
        };
        Dataset.prototype.sort = function (exFn, direction, context) {
            var value = this.valueOf();
            var directionFn = directionFns[direction];
            value.data = this.data.sort(function (a, b) {
                return directionFn(exFn(a, context), exFn(b, context));
            });
            return new Dataset(value);
        };
        Dataset.prototype.limit = function (limit) {
            var data = this.data;
            if (data.length <= limit)
                return this;
            var value = this.valueOf();
            value.data = data.slice(0, limit);
            return new Dataset(value);
        };
        Dataset.prototype.count = function () {
            return this.data.length;
        };
        Dataset.prototype.sum = function (exFn, context) {
            var data = this.data;
            var sum = 0;
            for (var _i = 0; _i < data.length; _i++) {
                var datum = data[_i];
                sum += exFn(datum, context);
            }
            return sum;
        };
        Dataset.prototype.average = function (exFn, context) {
            var count = this.count();
            return count ? (this.sum(exFn, context) / count) : null;
        };
        Dataset.prototype.min = function (exFn, context) {
            var data = this.data;
            var min = Infinity;
            for (var _i = 0; _i < data.length; _i++) {
                var datum = data[_i];
                var v = exFn(datum, context);
                if (v < min)
                    min = v;
            }
            return min;
        };
        Dataset.prototype.max = function (exFn, context) {
            var data = this.data;
            var max = -Infinity;
            for (var _i = 0; _i < data.length; _i++) {
                var datum = data[_i];
                var v = exFn(datum, context);
                if (max < v)
                    max = v;
            }
            return max;
        };
        Dataset.prototype.countDistinct = function (exFn, context) {
            var data = this.data;
            var seen = Object.create(null);
            for (var _i = 0; _i < data.length; _i++) {
                var datum = data[_i];
                seen[exFn(datum, context)] = 1;
            }
            return Object.keys(seen).length;
        };
        Dataset.prototype.quantile = function (exFn, quantile, context) {
            return quantile;
        };
        Dataset.prototype.split = function (splitFns, datasetName, context) {
            var data = this.data;
            var keys = Object.keys(splitFns);
            var numberOfKeys = keys.length;
            var splitFnList = keys.map(function (k) { return splitFns[k]; });
            var splits = {};
            var datas = {};
            var finalData = [];
            for (var _i = 0; _i < data.length; _i++) {
                var datum = data[_i];
                var valueList = splitFnList.map(function (splitFn) { return splitFn(datum, context); });
                var key = valueList.join(';_PLYw00d_;');
                if (hasOwnProperty(datas, key)) {
                    datas[key].push(datum);
                }
                else {
                    var newDatum = Object.create(null);
                    for (var i = 0; i < numberOfKeys; i++) {
                        newDatum[keys[i]] = valueList[i];
                    }
                    newDatum[datasetName] = (datas[key] = [datum]);
                    splits[key] = newDatum;
                    finalData.push(newDatum);
                }
            }
            for (var _a = 0; _a < finalData.length; _a++) {
                var finalDatum = finalData[_a];
                finalDatum[datasetName] = new Dataset({ suppress: true, data: finalDatum[datasetName] });
            }
            return new Dataset({
                keys: keys,
                data: finalData
            });
        };
        Dataset.prototype.introspect = function () {
            if (this.attributes)
                return;
            var data = this.data;
            if (!data.length) {
                this.attributes = [];
                return;
            }
            var attributeNamesToIntrospect = Object.keys(data[0]);
            var attributes = [];
            for (var _i = 0; _i < data.length; _i++) {
                var datum = data[_i];
                var attributeNamesStillToIntrospect = [];
                for (var _a = 0; _a < attributeNamesToIntrospect.length; _a++) {
                    var attributeNameToIntrospect = attributeNamesToIntrospect[_a];
                    var attributeInfo = getAttributeInfo(attributeNameToIntrospect, datum[attributeNameToIntrospect]);
                    if (attributeInfo) {
                        attributes.push(attributeInfo);
                    }
                    else {
                        attributeNamesStillToIntrospect.push(attributeNameToIntrospect);
                    }
                }
                attributeNamesToIntrospect = attributeNamesStillToIntrospect;
                if (!attributeNamesToIntrospect.length)
                    break;
            }
            for (var _b = 0; _b < attributeNamesToIntrospect.length; _b++) {
                var attributeName = attributeNamesToIntrospect[_b];
                attributes.push(new Plywood.AttributeInfo({ name: attributeName, type: 'STRING' }));
            }
            attributes.sort(function (a, b) {
                var typeDiff = typeOrder[a.type] - typeOrder[b.type];
                if (typeDiff)
                    return typeDiff;
                return a.name.localeCompare(b.name);
            });
            var attributeOverrides = this.attributeOverrides;
            if (attributeOverrides) {
                attributes = Plywood.AttributeInfo.applyOverrides(attributes, attributeOverrides);
                this.attributeOverrides = null;
            }
            this.attributes = attributes;
        };
        Dataset.prototype.getExternals = function () {
            if (this.data.length === 0)
                return [];
            var datum = this.data[0];
            var externals = [];
            Object.keys(datum).forEach(function (applyName) {
                var applyValue = datum[applyName];
                if (applyValue instanceof Dataset) {
                    externals.push(applyValue.getExternals());
                }
            });
            return Plywood.mergeExternals(externals);
        };
        Dataset.prototype.getExternalIds = function () {
            if (this.data.length === 0)
                return [];
            var datum = this.data[0];
            var push = Array.prototype.push;
            var externalIds = [];
            Object.keys(datum).forEach(function (applyName) {
                var applyValue = datum[applyName];
                if (applyValue instanceof Dataset) {
                    push.apply(externalIds, applyValue.getExternals());
                }
            });
            return deduplicateSort(externalIds);
        };
        Dataset.prototype.join = function (other) {
            if (!other)
                return this;
            var thisKey = this.keys[0];
            if (!thisKey)
                throw new Error('join lhs must have a key (be a product of a split)');
            var otherKey = other.keys[0];
            if (!otherKey)
                throw new Error('join rhs must have a key (be a product of a split)');
            var thisData = this.data;
            var otherData = other.data;
            var k;
            var mapping = Object.create(null);
            for (var i = 0; i < thisData.length; i++) {
                var datum = thisData[i];
                k = String(thisKey ? datum[thisKey] : i);
                mapping[k] = [datum];
            }
            for (var i = 0; i < otherData.length; i++) {
                var datum = otherData[i];
                k = String(otherKey ? datum[otherKey] : i);
                if (!mapping[k])
                    mapping[k] = [];
                mapping[k].push(datum);
            }
            var newData = [];
            for (var j in mapping) {
                var datums = mapping[j];
                if (datums.length === 1) {
                    newData.push(datums[0]);
                }
                else {
                    newData.push(joinDatums(datums[0], datums[1]));
                }
            }
            return new Dataset({ data: newData });
        };
        Dataset.prototype.getNestedColumns = function () {
            this.introspect();
            var nestedColumns = [];
            var attributes = this.attributes;
            var subDatasetAdded = false;
            for (var _i = 0; _i < attributes.length; _i++) {
                var attribute = attributes[_i];
                var column = {
                    name: attribute.name,
                    type: attribute.type
                };
                if (attribute.type === 'DATASET') {
                    if (!subDatasetAdded) {
                        subDatasetAdded = true;
                        column.columns = this.data[0][attribute.name].getNestedColumns();
                        nestedColumns.push(column);
                    }
                }
                else {
                    nestedColumns.push(column);
                }
            }
            return nestedColumns.sort(function (a, b) {
                var typeDiff = typeOrder[a.type] - typeOrder[b.type];
                if (typeDiff)
                    return typeDiff;
                return a.name.localeCompare(b.name);
            });
        };
        Dataset.prototype.getColumns = function (options) {
            if (options === void 0) { options = {}; }
            var prefixColumns = options.prefixColumns;
            return flattenColumns(this.getNestedColumns(), prefixColumns);
        };
        Dataset.prototype._flattenHelper = function (nestedColumns, prefix, order, nestingName, parentName, nesting, context, flat) {
            var data = this.data;
            var leaf = nestedColumns[nestedColumns.length - 1].type !== 'DATASET';
            for (var _i = 0; _i < data.length; _i++) {
                var datum = data[_i];
                var flatDatum = context ? copy(context) : {};
                if (nestingName)
                    flatDatum[nestingName] = nesting;
                if (parentName)
                    flatDatum[parentName] = context;
                for (var _a = 0; _a < nestedColumns.length; _a++) {
                    var flattenedColumn = nestedColumns[_a];
                    if (flattenedColumn.type === 'DATASET') {
                        var nextPrefix = null;
                        if (prefix !== null)
                            nextPrefix = prefix + flattenedColumn.name + '.';
                        if (order === 'preorder')
                            flat.push(flatDatum);
                        datum[flattenedColumn.name]._flattenHelper(flattenedColumn.columns, nextPrefix, order, nestingName, parentName, nesting + 1, flatDatum, flat);
                        if (order === 'postorder')
                            flat.push(flatDatum);
                    }
                    else {
                        var flatName = (prefix !== null ? prefix : '') + flattenedColumn.name;
                        flatDatum[flatName] = datum[flattenedColumn.name];
                    }
                }
                if (leaf)
                    flat.push(flatDatum);
            }
        };
        Dataset.prototype.flatten = function (options) {
            if (options === void 0) { options = {}; }
            var prefixColumns = options.prefixColumns;
            var order = options.order;
            var nestingName = options.nestingName;
            var parentName = options.parentName;
            var nestedColumns = this.getNestedColumns();
            var flatData = [];
            if (nestedColumns.length) {
                this._flattenHelper(nestedColumns, (prefixColumns ? '' : null), order, nestingName, parentName, 0, null, flatData);
            }
            return flatData;
        };
        Dataset.prototype.toTabular = function (tabulatorOptions) {
            var formatter = tabulatorOptions.formatter || {};
            var data = this.flatten(tabulatorOptions);
            var columns = this.getColumns(tabulatorOptions);
            var lines = [];
            lines.push(columns.map(function (c) { return c.name; }).join(tabulatorOptions.separator || ','));
            for (var i = 0; i < data.length; i++) {
                var datum = data[i];
                lines.push(columns.map(function (c) {
                    return String((formatter[c.type] || defaultFormatter[c.type])(datum[c.name]));
                }).join(tabulatorOptions.separator || ','));
            }
            var lineBreak = tabulatorOptions.lineBreak || '\n';
            return lines.join(lineBreak) + (lines.length > 0 ? lineBreak : '');
        };
        Dataset.prototype.toCSV = function (tabulatorOptions) {
            if (tabulatorOptions === void 0) { tabulatorOptions = {}; }
            tabulatorOptions.separator = tabulatorOptions.separator || ',';
            tabulatorOptions.lineBreak = tabulatorOptions.lineBreak || '\r\n';
            return this.toTabular(tabulatorOptions);
        };
        Dataset.prototype.toTSV = function (tabulatorOptions) {
            if (tabulatorOptions === void 0) { tabulatorOptions = {}; }
            tabulatorOptions.separator = tabulatorOptions.separator || '\t';
            tabulatorOptions.lineBreak = tabulatorOptions.lineBreak || '\r\n';
            return this.toTabular(tabulatorOptions);
        };
        Dataset.type = 'DATASET';
        return Dataset;
    })();
    Plywood.Dataset = Dataset;
    check = Dataset;
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    function mergeExternals(externalGroups) {
        var seen = {};
        externalGroups.forEach(function (externalGroup) {
            externalGroup.forEach(function (external) {
                var id = external.getId();
                if (seen[id])
                    return;
                seen[id] = external;
            });
        });
        return Object.keys(seen).sort().map(function (k) { return seen[k]; });
    }
    Plywood.mergeExternals = mergeExternals;
    function getSampleValue(valueType, ex) {
        switch (valueType) {
            case 'BOOLEAN':
                return true;
            case 'NUMBER':
                return 4;
            case 'NUMBER_RANGE':
                var numberBucketAction;
                if (ex instanceof Plywood.ChainExpression && (numberBucketAction = ex.getSingleAction('numberBucket'))) {
                    return new Plywood.NumberRange({
                        start: numberBucketAction.offset,
                        end: numberBucketAction.offset + numberBucketAction.size
                    });
                }
                else {
                    return new Plywood.NumberRange({ start: 0, end: 1 });
                }
            case 'TIME':
                return new Date('2015-03-14T00:00:00');
            case 'TIME_RANGE':
                var timeBucketAction;
                if (ex instanceof Plywood.ChainExpression && (timeBucketAction = ex.getSingleAction('timeBucket'))) {
                    var start = timeBucketAction.duration.floor(new Date('2015-03-14T00:00:00'), timeBucketAction.timezone);
                    return new Plywood.TimeRange({
                        start: start,
                        end: timeBucketAction.duration.move(start, timeBucketAction.timezone, 1)
                    });
                }
                else {
                    return new Plywood.TimeRange({ start: new Date('2015-03-14T00:00:00'), end: new Date('2015-03-15T00:00:00') });
                }
            case 'STRING':
                if (ex instanceof Plywood.RefExpression) {
                    return 'some_' + ex.name;
                }
                else {
                    return 'something';
                }
            case 'SET/STRING':
                if (ex instanceof Plywood.RefExpression) {
                    return Plywood.Set.fromJS([ex.name + '1']);
                }
                else {
                    return Plywood.Set.fromJS(['something']);
                }
            default:
                throw new Error("unsupported simulation on: " + valueType);
        }
    }
    function immutableAdd(obj, key, value) {
        var newObj = Object.create(null);
        for (var k in obj)
            newObj[k] = obj[k];
        newObj[key] = value;
        return newObj;
    }
    var External = (function () {
        function External(parameters, dummy) {
            if (dummy === void 0) { dummy = null; }
            this.attributes = null;
            this.attributeOverrides = null;
            this.rawAttributes = null;
            if (dummy !== dummyObject) {
                throw new TypeError("can not call `new External` directly use External.fromJS instead");
            }
            this.engine = parameters.engine;
            this.suppress = parameters.suppress === true;
            if (parameters.attributes) {
                this.attributes = parameters.attributes;
            }
            if (parameters.attributeOverrides) {
                this.attributeOverrides = parameters.attributeOverrides;
            }
            this.rawAttributes = parameters.rawAttributes;
            this.requester = parameters.requester;
            this.mode = parameters.mode || 'raw';
            this.derivedAttributes = parameters.derivedAttributes || {};
            this.filter = parameters.filter || Plywood.Expression.TRUE;
            this.split = parameters.split;
            this.dataName = parameters.dataName;
            this.applies = parameters.applies;
            this.sort = parameters.sort;
            this.limit = parameters.limit;
            this.havingFilter = parameters.havingFilter;
            if (this.mode !== 'raw') {
                this.applies = this.applies || [];
                if (this.mode === 'split') {
                    if (!this.split)
                        throw new Error('must have split action in split mode');
                    this.havingFilter = this.havingFilter || Plywood.Expression.TRUE;
                }
            }
        }
        External.isExternal = function (candidate) {
            return Plywood.isInstanceOf(candidate, External);
        };
        External.getSimpleInflater = function (splitExpression, label) {
            switch (splitExpression.type) {
                case 'BOOLEAN': return External.booleanInflaterFactory(label);
                case 'NUMBER': return External.numberInflaterFactory(label);
                case 'TIME': return External.timeInflaterFactory(label);
                default: return null;
            }
        };
        External.booleanInflaterFactory = function (label) {
            return function (d) {
                var v = '' + d[label];
                switch (v) {
                    case 'null':
                        d[label] = null;
                        break;
                    case 'false':
                        d[label] = false;
                        break;
                    case 'true':
                        d[label] = true;
                        break;
                    default:
                        throw new Error("got strange result from boolean: " + v);
                }
            };
        };
        External.timeRangeInflaterFactory = function (label, duration, timezone) {
            return function (d) {
                var v = d[label];
                if ('' + v === "null") {
                    d[label] = null;
                    return;
                }
                var start = new Date(v);
                d[label] = new Plywood.TimeRange({ start: start, end: duration.move(start, timezone) });
            };
        };
        External.consecutiveTimeRangeInflaterFactory = function (label, duration, timezone) {
            var canonicalDurationLengthAndThenSome = duration.getCanonicalLength() * 1.5;
            return function (d, i, data) {
                var v = d[label];
                if ('' + v === "null") {
                    d[label] = null;
                    return;
                }
                var start = new Date(v);
                var next = data[i + 1];
                var nextTimestamp;
                if (next) {
                    nextTimestamp = new Date(next[label]);
                }
                var end = (nextTimestamp &&
                    start.valueOf() < nextTimestamp.valueOf() &&
                    nextTimestamp.valueOf() - start.valueOf() < canonicalDurationLengthAndThenSome) ? nextTimestamp
                    : duration.move(start, timezone, 1);
                d[label] = new Plywood.TimeRange({ start: start, end: end });
            };
        };
        External.numberRangeInflaterFactory = function (label, rangeSize) {
            return function (d) {
                var v = d[label];
                if ('' + v === "null") {
                    d[label] = null;
                    return;
                }
                var start = Number(v);
                d[label] = new Plywood.NumberRange({
                    start: start,
                    end: Plywood.safeAdd(start, rangeSize)
                });
            };
        };
        External.numberInflaterFactory = function (label) {
            return function (d) {
                var v = d[label];
                if ('' + v === "null") {
                    d[label] = null;
                    return;
                }
                d[label] = Number(v);
            };
        };
        External.timeInflaterFactory = function (label) {
            return function (d) {
                var v = d[label];
                if ('' + v === "null") {
                    d[label] = null;
                    return;
                }
                d[label] = new Date(v);
            };
        };
        External.jsToValue = function (parameters) {
            var value = {
                engine: parameters.engine,
                suppress: true
            };
            if (parameters.attributes) {
                value.attributes = Plywood.AttributeInfo.fromJSs(parameters.attributes);
            }
            if (parameters.attributeOverrides) {
                value.attributeOverrides = Plywood.AttributeInfo.fromJSs(parameters.attributeOverrides);
            }
            if (parameters.requester)
                value.requester = parameters.requester;
            value.filter = parameters.filter ? Plywood.Expression.fromJS(parameters.filter) : Plywood.Expression.TRUE;
            return value;
        };
        External.register = function (ex, id) {
            if (id === void 0) { id = null; }
            if (!id)
                id = ex.name.replace('External', '').replace(/^\w/, function (s) { return s.toLowerCase(); });
            External.classMap[id] = ex;
        };
        External.fromJS = function (parameters) {
            if (!hasOwnProperty(parameters, "engine")) {
                throw new Error("external `engine` must be defined");
            }
            var engine = parameters.engine;
            if (typeof engine !== "string") {
                throw new Error("dataset must be a string");
            }
            var ClassFn = External.classMap[engine];
            if (!ClassFn) {
                throw new Error("unsupported engine '" + engine + "'");
            }
            return ClassFn.fromJS(parameters);
        };
        External.prototype._ensureEngine = function (engine) {
            if (!this.engine) {
                this.engine = engine;
                return;
            }
            if (this.engine !== engine) {
                throw new TypeError("incorrect engine '" + this.engine + "' (needs to be: '" + engine + "')");
            }
        };
        External.prototype.valueOf = function () {
            var value = {
                engine: this.engine
            };
            if (this.suppress)
                value.suppress = this.suppress;
            if (this.attributes)
                value.attributes = this.attributes;
            if (this.attributeOverrides)
                value.attributeOverrides = this.attributeOverrides;
            if (this.rawAttributes) {
                value.rawAttributes = this.rawAttributes;
            }
            if (this.requester) {
                value.requester = this.requester;
            }
            value.mode = this.mode;
            if (this.dataName) {
                value.dataName = this.dataName;
            }
            value.derivedAttributes = this.derivedAttributes;
            value.filter = this.filter;
            if (this.split) {
                value.split = this.split;
            }
            if (this.applies) {
                value.applies = this.applies;
            }
            if (this.sort) {
                value.sort = this.sort;
            }
            if (this.limit) {
                value.limit = this.limit;
            }
            if (this.havingFilter) {
                value.havingFilter = this.havingFilter;
            }
            return value;
        };
        External.prototype.toJS = function () {
            var js = {
                engine: this.engine
            };
            if (this.attributes)
                js.attributes = Plywood.AttributeInfo.toJSs(this.attributes);
            if (this.attributeOverrides)
                js.attributeOverrides = Plywood.AttributeInfo.toJSs(this.attributeOverrides);
            if (this.rawAttributes)
                js.rawAttributes = Plywood.AttributeInfo.toJSs(this.rawAttributes);
            if (this.requester) {
                js.requester = this.requester;
            }
            if (!this.filter.equals(Plywood.Expression.TRUE)) {
                js.filter = this.filter.toJS();
            }
            return js;
        };
        External.prototype.toJSON = function () {
            return this.toJS();
        };
        External.prototype.toString = function () {
            switch (this.mode) {
                case 'raw':
                    return "ExternalRaw(" + this.filter.toString() + ")";
                case 'total':
                    return "ExternalTotal(" + this.applies.length + ")";
                case 'split':
                    return "ExternalSplit(" + this.applies.length + ")";
                default:
                    return 'External()';
            }
        };
        External.prototype.equals = function (other) {
            return External.isExternal(other) &&
                this.engine === other.engine &&
                this.mode === other.mode &&
                this.filter.equals(other.filter);
        };
        External.prototype.getId = function () {
            return this.engine + ':' + this.filter.toString();
        };
        External.prototype.hasExternal = function () {
            return true;
        };
        External.prototype.getExternals = function () {
            return [this];
        };
        External.prototype.getExternalIds = function () {
            return [this.getId()];
        };
        External.prototype.getAttributesInfo = function (attributeName) {
            var attributes = this.rawAttributes || this.attributes;
            for (var _i = 0; _i < attributes.length; _i++) {
                var attribute = attributes[_i];
                if (attribute.name === attributeName)
                    return attribute;
            }
            return null;
        };
        External.prototype.updateAttribute = function (newAttribute) {
            if (!this.attributes)
                return this;
            var newAttributeName = newAttribute.name;
            var added = false;
            var value = this.valueOf();
            value.attributes = value.attributes.map(function (attribute) {
                if (attribute.name === newAttributeName) {
                    added = true;
                    return newAttribute;
                }
                else {
                    return attribute;
                }
            });
            if (!added) {
                value.attributes.push(newAttribute);
            }
            return new (External.classMap[this.engine])(value);
        };
        External.prototype.show = function () {
            var value = this.valueOf();
            value.suppress = false;
            return new (External.classMap[this.engine])(value);
        };
        External.prototype.canHandleFilter = function (ex) {
            throw new Error("must implement canHandleFilter");
        };
        External.prototype.canHandleTotal = function () {
            throw new Error("must implement canHandleTotal");
        };
        External.prototype.canHandleSplit = function (ex) {
            throw new Error("must implement canHandleSplit");
        };
        External.prototype.canHandleApply = function (ex) {
            throw new Error("must implement canHandleApply");
        };
        External.prototype.canHandleSort = function (sortAction) {
            throw new Error("must implement canHandleSort");
        };
        External.prototype.canHandleLimit = function (limitAction) {
            throw new Error("must implement canHandleLimit");
        };
        External.prototype.canHandleHavingFilter = function (ex) {
            throw new Error("must implement canHandleHavingFilter");
        };
        External.prototype.getRaw = function () {
            if (this.mode === 'raw')
                return this;
            var value = this.valueOf();
            value.suppress = true;
            value.mode = 'raw';
            value.dataName = null;
            value.attributes = value.rawAttributes;
            value.rawAttributes = null;
            value.applies = [];
            value.split = null;
            value.sort = null;
            value.limit = null;
            return (new (External.classMap[this.engine])(value));
        };
        External.prototype.makeTotal = function (dataName) {
            if (this.mode !== 'raw')
                return null;
            if (!this.canHandleTotal())
                return null;
            var value = this.valueOf();
            value.suppress = false;
            value.mode = 'total';
            value.dataName = dataName;
            value.rawAttributes = value.attributes;
            value.attributes = [];
            return (new (External.classMap[this.engine])(value));
        };
        External.prototype.addAction = function (action) {
            if (action instanceof Plywood.FilterAction) {
                return this._addFilterAction(action);
            }
            if (action instanceof Plywood.SplitAction) {
                return this._addSplitAction(action);
            }
            if (action instanceof Plywood.ApplyAction) {
                return this._addApplyAction(action);
            }
            if (action instanceof Plywood.SortAction) {
                return this._addSortAction(action);
            }
            if (action instanceof Plywood.LimitAction) {
                return this._addLimitAction(action);
            }
            return null;
        };
        External.prototype._addFilterAction = function (action) {
            return this.addFilter(action.expression);
        };
        External.prototype.addFilter = function (expression) {
            if (!expression.resolved())
                return null;
            var value = this.valueOf();
            switch (this.mode) {
                case 'raw':
                    if (!this.canHandleFilter(expression))
                        return null;
                    value.filter = value.filter.and(expression).simplify();
                    break;
                case 'split':
                    if (!this.canHandleHavingFilter(expression))
                        return null;
                    value.havingFilter = value.havingFilter.and(expression).simplify();
                    break;
                default:
                    return null;
            }
            return (new (External.classMap[this.engine])(value));
        };
        External.prototype._addSplitAction = function (splitAction) {
            if (this.mode !== 'raw')
                return null;
            var value = this.valueOf();
            value.suppress = false;
            value.mode = 'split';
            value.dataName = splitAction.dataName;
            value.split = splitAction;
            value.rawAttributes = value.attributes;
            value.attributes = splitAction.mapSplits(function (name, expression) { return new Plywood.AttributeInfo({ name: name, type: expression.type }); });
            return (new (External.classMap[this.engine])(value));
        };
        External.prototype._addApplyAction = function (action) {
            var expression = action.expression;
            if (expression.type !== 'NUMBER' && expression.type !== 'TIME')
                return null;
            if (!this.canHandleApply(action.expression))
                return null;
            var value = this.valueOf();
            if (this.mode === 'raw') {
                value.derivedAttributes = immutableAdd(value.derivedAttributes, action.name, action.expression);
                value.attributes = value.attributes.concat(new Plywood.AttributeInfo({ name: action.name, type: action.expression.type }));
            }
            else {
                if (this.split && this.split.hasKey(action.name))
                    return null;
                var basicActions = this.processApply(action);
                for (var _i = 0; _i < basicActions.length; _i++) {
                    var basicAction = basicActions[_i];
                    if (basicAction instanceof Plywood.ApplyAction) {
                        value.applies = value.applies.concat(basicAction);
                        value.attributes = value.attributes.concat(new Plywood.AttributeInfo({ name: basicAction.name, type: basicAction.expression.type }));
                    }
                    else {
                        throw new Error('got something strange from breakUpApply');
                    }
                }
            }
            return (new (External.classMap[this.engine])(value));
        };
        External.prototype._addSortAction = function (action) {
            if (this.limit)
                return null;
            if (!this.canHandleSort(action))
                return null;
            var value = this.valueOf();
            value.sort = action;
            return (new (External.classMap[this.engine])(value));
        };
        External.prototype._addLimitAction = function (action) {
            if (!this.canHandleLimit(action))
                return null;
            var value = this.valueOf();
            if (!value.limit || action.limit < value.limit.limit) {
                value.limit = action;
            }
            return (new (External.classMap[this.engine])(value));
        };
        External.prototype.getExistingApplyForExpression = function (expression) {
            var applies = this.applies;
            for (var _i = 0; _i < applies.length; _i++) {
                var apply = applies[_i];
                if (apply.expression.equals(expression))
                    return apply;
            }
            return null;
        };
        External.prototype.isKnownName = function (name) {
            var attributes = this.attributes;
            for (var _i = 0; _i < attributes.length; _i++) {
                var attribute = attributes[_i];
                if (attribute.name === name)
                    return true;
            }
            return false;
        };
        External.prototype.getTempName = function (namesTaken) {
            if (namesTaken === void 0) { namesTaken = []; }
            for (var i = 0; i < 1e6; i++) {
                var name = '_sd_' + i;
                if (namesTaken.indexOf(name) === -1 && !this.isKnownName(name))
                    return name;
            }
            throw new Error('could not find available name');
        };
        External.prototype.sortOnLabel = function () {
            var sort = this.sort;
            if (!sort)
                return false;
            var sortOn = sort.expression.name;
            if (!this.split || !this.split.hasKey(sortOn))
                return false;
            var applies = this.applies;
            for (var _i = 0; _i < applies.length; _i++) {
                var apply = applies[_i];
                if (apply.name === sortOn)
                    return false;
            }
            return true;
        };
        External.prototype.separateAggregates = function (apply) {
            var _this = this;
            var applyExpression = apply.expression;
            if (applyExpression instanceof Plywood.ChainExpression) {
                var actions = applyExpression.actions;
                if (actions[actions.length - 1].isAggregate()) {
                    return [apply];
                }
            }
            var applies = [];
            var namesUsed = [];
            var newExpression = applyExpression.substituteAction(function (action) {
                return action.isAggregate();
            }, function (preEx, action) {
                var aggregateChain = preEx.performAction(action);
                var existingApply = _this.getExistingApplyForExpression(aggregateChain);
                if (existingApply) {
                    return new Plywood.RefExpression({
                        name: existingApply.name,
                        nest: 0,
                        type: existingApply.expression.type
                    });
                }
                else {
                    var name = _this.getTempName(namesUsed);
                    namesUsed.push(name);
                    applies.push(new Plywood.ApplyAction({
                        action: 'apply',
                        name: name,
                        expression: aggregateChain
                    }));
                    return new Plywood.RefExpression({
                        name: name,
                        nest: 0,
                        type: aggregateChain.type
                    });
                }
            }, this);
            applies.push(new Plywood.ApplyAction({
                action: 'apply',
                name: apply.name,
                expression: newExpression
            }));
            return applies;
        };
        External.prototype.inlineDerivedAttributes = function (expression) {
            var derivedAttributes = this.derivedAttributes;
            return expression.substitute(function (ex) {
                return null;
            });
        };
        External.prototype.processApply = function (action) {
            return [action];
        };
        External.prototype.getEmptyTotalDataset = function () {
            var _this = this;
            if (this.mode !== 'total' || this.applies.length)
                return null;
            var dataName = this.dataName;
            return new Plywood.Dataset({ data: [{}] }).apply(dataName, function () {
                return _this.getRaw();
            }, null);
        };
        External.prototype.addNextExternal = function (dataset) {
            var _this = this;
            var dataName = this.dataName;
            switch (this.mode) {
                case 'total':
                    return dataset.apply(dataName, function () {
                        return _this.getRaw();
                    }, null);
                case 'split':
                    var split = this.split;
                    return dataset.apply(dataName, function (d) {
                        return _this.getRaw().addFilter(split.filterFromDatum(d));
                    }, null);
                default:
                    return dataset;
            }
        };
        External.prototype.simulate = function () {
            var datum = {};
            if (this.mode === 'raw') {
                var attributes = this.attributes;
                for (var _i = 0; _i < attributes.length; _i++) {
                    var attribute = attributes[_i];
                    datum[attribute.name] = getSampleValue(attribute.type, null);
                }
            }
            else {
                if (this.mode === 'split') {
                    this.split.mapSplits(function (name, expression) {
                        datum[name] = getSampleValue(expression.type, expression);
                    });
                }
                var applies = this.applies;
                for (var _a = 0; _a < applies.length; _a++) {
                    var apply = applies[_a];
                    datum[apply.name] = getSampleValue(apply.expression.type, apply.expression);
                }
            }
            var dataset = new Plywood.Dataset({ data: [datum] });
            dataset = this.addNextExternal(dataset);
            return dataset;
        };
        External.prototype.getQueryAndPostProcess = function () {
            throw new Error("can not call getQueryAndPostProcess directly");
        };
        External.prototype.queryValues = function () {
            if (!this.requester) {
                return Q.reject(new Error('must have a requester to make queries'));
            }
            try {
                var queryAndPostProcess = this.getQueryAndPostProcess();
            }
            catch (e) {
                return Q.reject(e);
            }
            if (!hasOwnProperty(queryAndPostProcess, 'query') || typeof queryAndPostProcess.postProcess !== 'function') {
                return Q.reject(new Error('no error query or postProcess'));
            }
            var result = this.requester({ query: queryAndPostProcess.query })
                .then(queryAndPostProcess.postProcess);
            if (this.mode !== 'raw') {
                result = result.then(this.addNextExternal.bind(this));
            }
            return result;
        };
        External.prototype.needsIntrospect = function () {
            return !this.attributes;
        };
        External.prototype.getIntrospectQueryAndPostProcess = function () {
            throw new Error("can not call getIntrospectQueryAndPostProcess directly");
        };
        External.prototype.introspect = function () {
            if (this.attributes) {
                return Q(this);
            }
            if (!this.requester) {
                return Q.reject(new Error('must have a requester to introspect'));
            }
            try {
                var queryAndPostProcess = this.getIntrospectQueryAndPostProcess();
            }
            catch (e) {
                return Q.reject(e);
            }
            if (!hasOwnProperty(queryAndPostProcess, 'query') || typeof queryAndPostProcess.postProcess !== 'function') {
                return Q.reject(new Error('no error query or postProcess'));
            }
            var value = this.valueOf();
            var ClassFn = External.classMap[this.engine];
            return this.requester({ query: queryAndPostProcess.query })
                .then(queryAndPostProcess.postProcess)
                .then(function (attributes) {
                if (value.attributeOverrides) {
                    attributes = Plywood.AttributeInfo.applyOverrides(attributes, value.attributeOverrides);
                }
                value.attributes = attributes;
                return (new ClassFn(value));
            });
        };
        External.prototype.getFullType = function () {
            var attributes = this.attributes;
            if (!attributes)
                throw new Error("dataset has not been introspected");
            var remote = [this.engine];
            var myDatasetType = {};
            for (var _i = 0; _i < attributes.length; _i++) {
                var attribute = attributes[_i];
                var attrName = attribute.name;
                myDatasetType[attrName] = {
                    type: attribute.type,
                    remote: remote
                };
            }
            var myFullType = {
                type: 'DATASET',
                datasetType: myDatasetType,
                remote: remote
            };
            return myFullType;
        };
        External.prototype.digest = function (expression, action) {
            if (expression instanceof Plywood.LiteralExpression) {
                var external = expression.value;
                if (external instanceof External) {
                    var newExternal = external.addAction(action);
                    if (!newExternal)
                        return null;
                    return {
                        undigested: null,
                        expression: new Plywood.LiteralExpression({
                            op: 'literal',
                            value: newExternal
                        })
                    };
                }
                else {
                    return null;
                }
            }
            else {
                throw new Error("can not digest " + expression.op);
            }
        };
        External.type = 'EXTERNAL';
        External.classMap = {};
        return External;
    })();
    Plywood.External = External;
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var DUMMY_NAME = '!DUMMY';
    var AGGREGATE_TO_DRUID = {
        count: "count",
        sum: "doubleSum",
        min: "doubleMin",
        max: "doubleMax"
    };
    var TIME_PART_TO_FORMAT = {
        SECOND_OF_MINUTE: "s",
        SECOND_OF_HOUR: "m'*60+'s",
        SECOND_OF_DAY: "H'*60+'m'*60+'s",
        SECOND_OF_WEEK: "e'~*24+H'*60+'m'*60+'s",
        SECOND_OF_MONTH: "d'~*24+H'*60+'m'*60+'s",
        SECOND_OF_YEAR: "D'*24+H'*60+'m'*60+'s",
        MINUTE_OF_HOUR: "m",
        MINUTE_OF_DAY: "H'*60+'m",
        MINUTE_OF_WEEK: "e'~*24+H'*60+'m",
        MINUTE_OF_MONTH: "d'~*24+H'*60+'m",
        MINUTE_OF_YEAR: "D'*24+H'*60+'m",
        HOUR_OF_DAY: "H",
        HOUR_OF_WEEK: "e'~*24+H",
        HOUR_OF_MONTH: "d'~*24+H",
        HOUR_OF_YEAR: "D'*24+H",
        DAY_OF_WEEK: "e'~",
        DAY_OF_MONTH: "d'~",
        DAY_OF_YEAR: "D",
        WEEK_OF_MONTH: null,
        WEEK_OF_YEAR: "w",
        MONTH_OF_YEAR: "M~"
    };
    var TIME_BUCKET_FORMAT = {
        "PT1S": "yyyy-MM-dd'T'HH:mm:ss'Z",
        "PT1M": "yyyy-MM-dd'T'HH:mm'Z",
        "PT1H": "yyyy-MM-dd'T'HH':00Z",
        "P1D": "yyyy-MM-dd'Z",
        "P1M": "yyyy-MM'-01Z",
        "P1Y": "yyyy'-01-01Z"
    };
    function simpleMath(exprStr) {
        if (String(exprStr) === 'null')
            return null;
        var parts = exprStr.split(/(?=[*+~])/);
        var acc = parseInt(parts.shift(), 10);
        for (var _i = 0; _i < parts.length; _i++) {
            var part = parts[_i];
            var v = parseInt(part.substring(1), 10);
            switch (part[0]) {
                case '+':
                    acc += v;
                    break;
                case '*':
                    acc *= v;
                    break;
                case '~':
                    acc--;
                    break;
            }
        }
        return acc;
    }
    function customAggregationsEqual(customA, customB) {
        return JSON.stringify(customA) === JSON.stringify(customB);
    }
    function cleanDatumInPlace(datum) {
        if (hasOwnProperty(datum, DUMMY_NAME)) {
            delete datum[DUMMY_NAME];
        }
    }
    function correctTimeBoundaryResult(result) {
        return Array.isArray(result) && result.length === 1 && typeof result[0].result === 'object';
    }
    function correctTimeseriesResult(result) {
        return Array.isArray(result) && (result.length === 0 || typeof result[0].result === 'object');
    }
    function correctTopNResult(result) {
        return Array.isArray(result) && (result.length === 0 || Array.isArray(result[0].result));
    }
    function correctGroupByResult(result) {
        return Array.isArray(result) && (result.length === 0 || typeof result[0].event === 'object');
    }
    function correctSelectResult(result) {
        return Array.isArray(result) && (result.length === 0 || typeof result[0].result === 'object');
    }
    function timeBoundaryPostProcessFactory(applies) {
        return function (res) {
            if (!correctTimeBoundaryResult(res)) {
                var err = new Error("unexpected result from Druid (timeBoundary)");
                err.result = res;
                throw err;
            }
            var result = res[0].result;
            var datum = {};
            for (var _i = 0; _i < applies.length; _i++) {
                var apply = applies[_i];
                var name_1 = apply.name;
                var aggregate = apply.expression.actions[0].action;
                if (typeof result === 'string') {
                    datum[name_1] = new Date(result);
                }
                else {
                    if (aggregate === 'max') {
                        datum[name_1] = new Date((result['maxIngestedEventTime'] || result['maxTime']));
                    }
                    else {
                        datum[name_1] = new Date((result['minTime']));
                    }
                }
            }
            return new Plywood.Dataset({ data: [datum] });
        };
    }
    function makeZeroDatum(applies) {
        var newDatum = Object.create(null);
        for (var _i = 0; _i < applies.length; _i++) {
            var apply = applies[_i];
            var applyName = apply.name;
            if (applyName[0] === '_')
                continue;
            newDatum[applyName] = 0;
        }
        return newDatum;
    }
    function totalPostProcessFactory(applies) {
        return function (res) {
            if (!correctTimeseriesResult(res)) {
                var err = new Error("unexpected result from Druid (all)");
                err.result = res;
                throw err;
            }
            if (!res.length) {
                return new Plywood.Dataset({ data: [makeZeroDatum(applies)] });
            }
            return new Plywood.Dataset({ data: [res[0].result] });
        };
    }
    function timeseriesNormalizerFactory(timestampLabel) {
        if (timestampLabel === void 0) { timestampLabel = null; }
        return function (res) {
            if (!correctTimeseriesResult(res)) {
                var err = new Error("unexpected result from Druid (timeseries)");
                err.result = res;
                throw err;
            }
            return res.map(function (r) {
                var datum = r.result;
                cleanDatumInPlace(datum);
                if (timestampLabel)
                    datum[timestampLabel] = r.timestamp;
                return datum;
            });
        };
    }
    function topNNormalizer(res) {
        if (!correctTopNResult(res)) {
            var err = new Error("unexpected result from Druid (topN)");
            err.result = res;
            throw err;
        }
        var data = res.length ? res[0].result : [];
        for (var _i = 0; _i < data.length; _i++) {
            var d = data[_i];
            cleanDatumInPlace(d);
        }
        return data;
    }
    function groupByNormalizerFactory(timestampLabel) {
        if (timestampLabel === void 0) { timestampLabel = null; }
        return function (res) {
            if (!correctGroupByResult(res)) {
                var err = new Error("unexpected result from Druid (groupBy)");
                err.result = res;
                throw err;
            }
            return res.map(function (r) {
                var datum = r.event;
                cleanDatumInPlace(datum);
                if (timestampLabel)
                    datum[timestampLabel] = r.timestamp;
                return datum;
            });
        };
    }
    function selectNormalizer(res) {
        if (!correctSelectResult(res)) {
            var err = new Error("unexpected result from Druid (select)");
            err.result = res;
            throw err;
        }
        return res[0].result.events.map(function (event) { return event.event; });
    }
    function postProcessFactory(normalizer, inflaters) {
        return function (res) {
            var data = normalizer(res);
            var n = data.length;
            for (var _i = 0; _i < inflaters.length; _i++) {
                var inflater = inflaters[_i];
                for (var i = 0; i < n; i++) {
                    inflater(data[i], i, data);
                }
            }
            return new Plywood.Dataset({ data: data });
        };
    }
    function simpleMathInflaterFactory(label) {
        return function (d) {
            var v = d[label];
            if ('' + v === "null") {
                d[label] = null;
                return;
            }
            d[label] = simpleMath(v);
        };
    }
    function introspectPostProcessFactory(timeAttribute) {
        return function (res) {
            var attributes = [
                new Plywood.AttributeInfo({ name: timeAttribute, type: 'TIME' })
            ];
            res.dimensions.forEach(function (dimension) {
                if (dimension === timeAttribute)
                    return;
                attributes.push(new Plywood.AttributeInfo({ name: dimension, type: 'STRING' }));
            });
            res.metrics.forEach(function (metric) {
                if (metric === timeAttribute)
                    return;
                attributes.push(new Plywood.AttributeInfo({ name: metric, type: 'NUMBER', filterable: false, splitable: false }));
            });
            return attributes;
        };
    }
    function segmentMetadataPostProcessFactory(timeAttribute) {
        return function (res) {
            var attributes = [];
            var columns = res[0].columns;
            for (var name in columns) {
                if (!hasOwnProperty(columns, name))
                    continue;
                if (name === '__time') {
                    attributes.push(new Plywood.AttributeInfo({ name: timeAttribute, type: 'TIME' }));
                }
                else {
                    if (name === timeAttribute)
                        continue;
                    var columnData = columns[name];
                    if (columnData.type === "STRING") {
                        attributes.push(new Plywood.AttributeInfo({ name: name, type: 'STRING' }));
                    }
                    else {
                        attributes.push(new Plywood.AttributeInfo({ name: name, type: 'NUMBER', filterable: false, splitable: false }));
                    }
                }
            }
            return attributes;
        };
    }
    var DruidExternal = (function (_super) {
        __extends(DruidExternal, _super);
        function DruidExternal(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureEngine("druid");
            this.dataSource = parameters.dataSource;
            this.timeAttribute = parameters.timeAttribute;
            this.customAggregations = parameters.customAggregations;
            if (typeof this.timeAttribute !== 'string')
                throw new Error("must have a timeAttribute");
            this.allowEternity = parameters.allowEternity;
            this.allowSelectQueries = parameters.allowSelectQueries;
            this.exactResultsOnly = parameters.exactResultsOnly;
            this.useSegmentMetadata = parameters.useSegmentMetadata;
            this.context = parameters.context;
            var druidVersion = parameters.druidVersion || '0.8.0';
            if (druidVersion.length !== 5)
                throw new Error('druidVersion length must be 5');
            if (druidVersion < '0.8.0')
                throw new Error('only druidVersions >= 0.8.0 are supported');
            this.druidVersion = druidVersion;
        }
        DruidExternal.fromJS = function (datasetJS) {
            var value = Plywood.External.jsToValue(datasetJS);
            value.dataSource = datasetJS.dataSource;
            value.timeAttribute = datasetJS.timeAttribute;
            value.customAggregations = datasetJS.customAggregations || {};
            value.allowEternity = Boolean(datasetJS.allowEternity);
            value.allowSelectQueries = Boolean(datasetJS.allowSelectQueries);
            value.exactResultsOnly = Boolean(datasetJS.exactResultsOnly);
            value.useSegmentMetadata = Boolean(datasetJS.useSegmentMetadata);
            value.context = datasetJS.context;
            value.druidVersion = datasetJS.druidVersion;
            return new DruidExternal(value);
        };
        DruidExternal.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.dataSource = this.dataSource;
            value.timeAttribute = this.timeAttribute;
            value.customAggregations = this.customAggregations;
            value.allowEternity = this.allowEternity;
            value.allowSelectQueries = this.allowSelectQueries;
            value.exactResultsOnly = this.exactResultsOnly;
            value.useSegmentMetadata = this.useSegmentMetadata;
            value.context = this.context;
            value.druidVersion = this.druidVersion;
            return value;
        };
        DruidExternal.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.dataSource = this.dataSource;
            js.timeAttribute = this.timeAttribute;
            if (Object.keys(this.customAggregations).length)
                js.customAggregations = this.customAggregations;
            if (this.allowEternity)
                js.allowEternity = true;
            if (this.allowSelectQueries)
                js.allowSelectQueries = true;
            if (this.exactResultsOnly)
                js.exactResultsOnly = true;
            if (this.useSegmentMetadata)
                js.useSegmentMetadata = true;
            js.context = this.context;
            js.druidVersion = this.druidVersion;
            return js;
        };
        DruidExternal.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                String(this.dataSource) === String(other.dataSource) &&
                this.timeAttribute === other.timeAttribute &&
                customAggregationsEqual(this.customAggregations, other.customAggregations) &&
                this.allowEternity === other.allowEternity &&
                this.allowSelectQueries === other.allowSelectQueries &&
                this.exactResultsOnly === other.exactResultsOnly &&
                this.useSegmentMetadata === other.useSegmentMetadata &&
                dictEqual(this.context, other.context) &&
                this.druidVersion === other.druidVersion;
        };
        DruidExternal.prototype.getId = function () {
            return _super.prototype.getId.call(this) + ':' + this.dataSource;
        };
        DruidExternal.prototype.canHandleFilter = function (ex) {
            return true;
        };
        DruidExternal.prototype.canHandleTotal = function () {
            return true;
        };
        DruidExternal.prototype.canHandleSplit = function (ex) {
            return true;
        };
        DruidExternal.prototype.canHandleApply = function (ex) {
            return true;
        };
        DruidExternal.prototype.canHandleSort = function (sortAction) {
            var split = this.split;
            if (!split || split.isMultiSplit())
                return true;
            var splitExpression = split.firstSplitExpression();
            var label = split.firstSplitName();
            if (splitExpression instanceof Plywood.ChainExpression) {
                if (splitExpression.actions.length === 1 && splitExpression.actions[0].action === 'timeBucket') {
                    if (sortAction.direction !== 'ascending')
                        return false;
                    var sortExpression = sortAction.expression;
                    if (sortExpression instanceof Plywood.RefExpression) {
                        return sortExpression.name === label;
                    }
                    else {
                        return false;
                    }
                }
                else {
                    return true;
                }
            }
            else {
                return true;
            }
        };
        DruidExternal.prototype.canHandleLimit = function (limitAction) {
            var split = this.split;
            if (!split || split.isMultiSplit())
                return true;
            var splitExpression = split.firstSplitExpression();
            if (splitExpression instanceof Plywood.ChainExpression) {
                if (splitExpression.getExpressionPattern('concat'))
                    return true;
                if (splitExpression.actions.length !== 1)
                    return false;
                return splitExpression.actions[0].action !== 'timeBucket';
            }
            else {
                return true;
            }
        };
        DruidExternal.prototype.canHandleHavingFilter = function (ex) {
            return !this.limit;
        };
        DruidExternal.prototype.versionBefore = function (neededVersion) {
            return this.druidVersion < neededVersion;
        };
        DruidExternal.prototype.getDruidDataSource = function () {
            var dataSource = this.dataSource;
            if (Array.isArray(dataSource)) {
                return {
                    type: "union",
                    dataSources: dataSource
                };
            }
            else {
                return dataSource;
            }
        };
        DruidExternal.prototype.canUseNativeAggregateFilter = function (filter) {
            var _this = this;
            if (filter instanceof Plywood.ChainExpression) {
                var pattern;
                if (pattern = (filter.getExpressionPattern('and') || filter.getExpressionPattern('or'))) {
                    return pattern.every(function (ex) {
                        return _this.canUseNativeAggregateFilter(ex);
                    }, this);
                }
                if (filter.lastAction() instanceof Plywood.NotAction) {
                    return this.canUseNativeAggregateFilter(filter.popAction());
                }
                var actions = filter.actions;
                if (actions.length !== 1)
                    return false;
                var firstAction = actions[0];
                return filter.expression.isOp('ref') &&
                    (firstAction.action === 'is' || firstAction.action === 'in') &&
                    firstAction.expression.isOp('literal');
            }
            return false;
        };
        DruidExternal.prototype.javascriptDruidFilter = function (referenceName, filter) {
            return {
                type: "javascript",
                dimension: referenceName,
                "function": filter.getJSFn('d')
            };
        };
        DruidExternal.prototype.timelessFilterToDruid = function (filter) {
            var _this = this;
            if (filter.type !== 'BOOLEAN')
                throw new Error("must be a BOOLEAN filter");
            var pattern;
            if (pattern = filter.getExpressionPattern('and')) {
                return {
                    type: 'and',
                    fields: pattern.map(this.timelessFilterToDruid, this)
                };
            }
            if (pattern = filter.getExpressionPattern('or')) {
                return {
                    type: 'or',
                    fields: pattern.map(this.timelessFilterToDruid, this)
                };
            }
            if (filter instanceof Plywood.LiteralExpression) {
                if (filter.value === true) {
                    return null;
                }
                else {
                    throw new Error("should never get here");
                }
            }
            else if (filter instanceof Plywood.ChainExpression) {
                var filterAction = filter.lastAction();
                var rhs = filterAction.expression;
                var lhs = filter.popAction();
                var extractionFn = this.expressionToExtractionFn(lhs);
                var referenceName = lhs.getFreeReferences()[0];
                var attributeInfo = this.getAttributesInfo(referenceName);
                if (filterAction instanceof Plywood.NotAction) {
                    return {
                        type: 'not',
                        field: this.timelessFilterToDruid(lhs)
                    };
                }
                if (filterAction instanceof Plywood.IsAction) {
                    if (rhs instanceof Plywood.LiteralExpression) {
                        var druidFilter = {
                            type: "selector",
                            dimension: referenceName,
                            value: attributeInfo.serialize(rhs.value)
                        };
                        if (extractionFn) {
                            druidFilter.type = "extraction";
                            druidFilter.extractionFn = extractionFn;
                        }
                        return druidFilter;
                    }
                    else {
                        throw new Error("can not convert " + filter.toString() + " to Druid filter");
                    }
                }
                if (filterAction instanceof Plywood.InAction) {
                    if (rhs instanceof Plywood.LiteralExpression) {
                        var rhsType = rhs.type;
                        if (rhsType === 'SET/STRING' || rhsType === 'SET/NULL') {
                            var fields = rhs.value.elements.map(function (value) {
                                var druidFilter = {
                                    type: "selector",
                                    dimension: referenceName,
                                    value: attributeInfo.serialize(value)
                                };
                                if (extractionFn) {
                                    if (extractionFn.type === 'javascript') {
                                        return _this.javascriptDruidFilter(referenceName, filter);
                                    }
                                    druidFilter.type = "extraction";
                                    druidFilter.extractionFn = extractionFn;
                                }
                                return druidFilter;
                            });
                            if (fields.length === 1)
                                return fields[0];
                            return { type: "or", fields: fields };
                        }
                        else if (rhsType === 'NUMBER_RANGE') {
                            var range = rhs.value;
                            var r0 = range.start;
                            var r1 = range.end;
                            return {
                                type: "javascript",
                                dimension: referenceName,
                                "function": "function(a) { a = Number(a); return " + r0 + " <= a && a < " + r1 + "; }"
                            };
                        }
                        else if (rhsType === 'TIME_RANGE') {
                            throw new Error("can not time filter on non-primary time dimension");
                        }
                        else {
                            throw new Error("not supported " + rhsType);
                        }
                    }
                    else {
                        throw new Error("can not convert " + filter.toString() + " to Druid filter");
                    }
                }
                if (filterAction instanceof Plywood.MatchAction) {
                    if (lhs instanceof Plywood.RefExpression) {
                        return {
                            type: "regex",
                            dimension: referenceName,
                            pattern: filterAction.regexp
                        };
                    }
                    else {
                        throw new Error("can not convert " + filter.toString() + " to Druid filter");
                    }
                }
                if (filterAction instanceof Plywood.ContainsAction) {
                    if (lhs instanceof Plywood.RefExpression && rhs instanceof Plywood.LiteralExpression) {
                        if (filterAction.compare === Plywood.ContainsAction.IGNORE_CASE) {
                            return {
                                type: "search",
                                dimension: referenceName,
                                query: {
                                    type: "fragment",
                                    values: [rhs.value]
                                }
                            };
                        }
                        else {
                            return this.javascriptDruidFilter(referenceName, filter);
                        }
                    }
                    else {
                        throw new Error("can not convert " + filter.toString() + " to Druid filter");
                    }
                }
            }
            else {
                throw new Error("could not convert filter " + filter.toString() + " to Druid filter");
            }
        };
        DruidExternal.prototype.timeFilterToIntervals = function (filter) {
            if (filter.type !== 'BOOLEAN')
                throw new Error("must be a BOOLEAN filter");
            if (filter instanceof Plywood.LiteralExpression) {
                if (!filter.value)
                    return DruidExternal.FALSE_INTERVAL;
                if (!this.allowEternity)
                    throw new Error('must filter on time unless the allowEternity flag is set');
                return DruidExternal.TRUE_INTERVAL;
            }
            else if (filter instanceof Plywood.ChainExpression) {
                var lhs = filter.expression;
                var actions = filter.actions;
                if (actions.length !== 1)
                    throw new Error("can not convert " + filter.toString() + " to Druid interval");
                var filterAction = actions[0];
                var rhs = filterAction.expression;
                if (filterAction instanceof Plywood.IsAction) {
                    if (lhs instanceof Plywood.RefExpression && rhs instanceof Plywood.LiteralExpression) {
                        return [Plywood.TimeRange.intervalFromDate(rhs.value)];
                    }
                    else {
                        throw new Error("can not convert " + filter.toString() + " to Druid interval");
                    }
                }
                else if (filterAction instanceof Plywood.InAction) {
                    if (lhs instanceof Plywood.RefExpression && rhs instanceof Plywood.LiteralExpression) {
                        var timeRanges;
                        var rhsType = rhs.type;
                        if (rhsType === 'SET/TIME_RANGE') {
                            timeRanges = rhs.value.elements;
                        }
                        else if (rhsType === 'TIME_RANGE') {
                            timeRanges = [rhs.value];
                        }
                        else {
                            throw new Error("not supported " + rhsType + " for time filtering");
                        }
                        return timeRanges.map(function (timeRange) { return timeRange.toInterval(); });
                    }
                    else {
                        throw new Error("can not convert " + filter.toString() + " to Druid interval");
                    }
                }
                else {
                    throw new Error("can not convert " + filter.toString() + " to Druid interval");
                }
            }
            else {
                throw new Error("can not convert " + filter.toString() + " to Druid interval");
            }
        };
        DruidExternal.prototype.filterToDruid = function (filter) {
            if (filter.type !== 'BOOLEAN')
                throw new Error("must be a BOOLEAN filter");
            if (filter.equals(Plywood.Expression.FALSE)) {
                return {
                    intervals: DruidExternal.FALSE_INTERVAL,
                    filter: null
                };
            }
            else {
                var sep = filter.separateViaAnd(this.timeAttribute);
                if (!sep)
                    throw new Error("could not separate time filter in " + filter.toString());
                return {
                    intervals: this.timeFilterToIntervals(sep.included),
                    filter: this.timelessFilterToDruid(sep.excluded)
                };
            }
        };
        DruidExternal.prototype.getRangeBucketingExtractionFn = function (attributeInfo, numberBucket) {
            var regExp = attributeInfo.getMatchingRegExpString();
            if (numberBucket && numberBucket.offset === 0 && numberBucket.size === attributeInfo.rangeSize)
                numberBucket = null;
            var bucketing = '';
            if (numberBucket) {
                bucketing = 's=' + Plywood.continuousFloorExpression('s', 'Math.floor', numberBucket.size, numberBucket.offset) + ';';
            }
            return {
                type: "javascript",
                'function': "function(d) {\nvar m = d.match(" + regExp + ");\nif(!m) return 'null';\nvar s = +m[1];\nif(!(Math.abs(+m[2] - s - " + attributeInfo.rangeSize + ") < 1e-6)) return 'null'; " + bucketing + "\nvar parts = String(Math.abs(s)).split('.');\nparts[0] = ('000000000' + parts[0]).substr(-10);\nreturn (start < 0 ?'-':'') + parts.join('.');\n}"
            };
        };
        DruidExternal.prototype.isTimeRef = function (ex) {
            return ex instanceof Plywood.RefExpression && ex.name === this.timeAttribute;
        };
        DruidExternal.prototype.splitExpressionToGranularityInflater = function (splitExpression, label) {
            if (splitExpression instanceof Plywood.ChainExpression) {
                var splitActions = splitExpression.actions;
                if (this.isTimeRef(splitExpression.expression) && splitActions.length === 1 && splitActions[0].action === 'timeBucket') {
                    var _a = splitActions[0], duration = _a.duration, timezone = _a.timezone;
                    return {
                        granularity: {
                            type: "period",
                            period: duration.toString(),
                            timeZone: timezone.toString()
                        },
                        inflater: Plywood.External.timeRangeInflaterFactory(label, duration, timezone)
                    };
                }
            }
            return null;
        };
        DruidExternal.prototype.expressionToExtractionFn = function (expression) {
            var freeReferences = expression.getFreeReferences();
            if (freeReferences.length !== 1) {
                throw new Error("must have a single reference: " + expression.toString());
            }
            var referenceName = freeReferences[0];
            if (expression instanceof Plywood.RefExpression) {
                var attributeInfo = this.getAttributesInfo(referenceName);
                if (attributeInfo instanceof Plywood.RangeAttributeInfo) {
                    return this.getRangeBucketingExtractionFn(attributeInfo, null);
                }
                if (expression.type === 'BOOLEAN') {
                    return {
                        type: "lookup",
                        lookup: {
                            type: "map",
                            map: {
                                "0": "false",
                                "1": "true",
                                "false": "false",
                                "true": "true"
                            }
                        },
                        injective: false
                    };
                }
                return null;
            }
            if (expression.type === 'BOOLEAN') {
                return {
                    type: "javascript",
                    'function': expression.getJSFn('d')
                };
            }
            if (expression instanceof Plywood.ChainExpression) {
                if (expression.getExpressionPattern('concat')) {
                    return {
                        type: "javascript",
                        'function': expression.getJSFn('d'),
                        injective: true
                    };
                }
                if (!expression.expression.isOp('ref')) {
                    throw new Error("can not convert complex: " + expression.expression.toString());
                }
                var actions = expression.actions;
                if (actions.length !== 1)
                    throw new Error("can not convert expression: " + expression.toString());
                var action = actions[0];
                if (action instanceof Plywood.SubstrAction) {
                    if (this.versionBefore('0.9.0')) {
                        return {
                            type: "javascript",
                            'function': expression.getJSFn('d')
                        };
                    }
                    return {
                        type: "substring",
                        index: action.position,
                        length: action.length
                    };
                }
                if (action instanceof Plywood.ExtractAction) {
                    if (this.versionBefore('0.9.1')) {
                        return {
                            type: "javascript",
                            'function': expression.getJSFn('d')
                        };
                    }
                    return {
                        type: "regex",
                        expr: action.regexp,
                        replaceMissingValue: true
                    };
                }
                if (action instanceof Plywood.LookupAction) {
                    return {
                        type: "lookup",
                        lookup: {
                            type: "namespace",
                            "namespace": action.lookup
                        },
                        injective: false
                    };
                }
                if (action instanceof Plywood.TimeBucketAction) {
                    var format = TIME_BUCKET_FORMAT[action.duration.toString()];
                    if (!format)
                        throw new Error("unsupported part in timeBucket expression " + action.duration.toString());
                    return {
                        type: "timeFormat",
                        format: format,
                        timeZone: action.timezone.toString(),
                        locale: "en-US"
                    };
                }
                if (action instanceof Plywood.TimePartAction) {
                    var format = TIME_PART_TO_FORMAT[action.part];
                    if (!format)
                        throw new Error("unsupported part in timePart expression " + action.part);
                    return {
                        type: "timeFormat",
                        format: format,
                        timeZone: action.timezone.toString(),
                        locale: "en-US"
                    };
                }
                if (action instanceof Plywood.NumberBucketAction) {
                    var attributeInfo = this.getAttributesInfo(referenceName);
                    if (attributeInfo.type === 'NUMBER') {
                        var floorExpression = Plywood.continuousFloorExpression("d", "Math.floor", action.size, action.offset);
                        return {
                            type: "javascript",
                            'function': "function(d){d=Number(d); if(isNaN(d)) return 'null'; return " + floorExpression + ";}"
                        };
                    }
                    if (attributeInfo instanceof Plywood.RangeAttributeInfo) {
                        return this.getRangeBucketingExtractionFn(attributeInfo, action);
                    }
                    if (attributeInfo instanceof Plywood.HistogramAttributeInfo) {
                        if (this.exactResultsOnly) {
                            throw new Error("can not use approximate histograms in exactResultsOnly mode");
                        }
                        throw new Error("histogram splits do not work right now");
                    }
                }
            }
            throw new Error("could not convert " + expression.toString() + " to a Druid extractionFn");
        };
        DruidExternal.prototype.splitExpressionToDimensionInflater = function (splitExpression, label) {
            var extractionFn = this.expressionToExtractionFn(splitExpression);
            var referenceName = splitExpression.getFreeReferences()[0];
            var simpleInflater = Plywood.External.getSimpleInflater(splitExpression, label);
            var dimension = {
                type: "default",
                dimension: referenceName === this.timeAttribute ? '__time' : referenceName,
                outputName: label
            };
            if (extractionFn) {
                dimension.type = "extraction";
                dimension.extractionFn = extractionFn;
            }
            if (splitExpression instanceof Plywood.RefExpression) {
                var attributeInfo = this.getAttributesInfo(referenceName);
                if (attributeInfo instanceof Plywood.RangeAttributeInfo) {
                    return {
                        dimension: dimension,
                        inflater: Plywood.External.numberRangeInflaterFactory(label, attributeInfo.rangeSize)
                    };
                }
                return {
                    dimension: dimension,
                    inflater: simpleInflater
                };
            }
            if (splitExpression.type === 'BOOLEAN' || splitExpression.type === 'STRING') {
                return {
                    dimension: dimension,
                    inflater: simpleInflater
                };
            }
            if (splitExpression instanceof Plywood.ChainExpression) {
                if (splitExpression.getExpressionPattern('concat')) {
                    return {
                        dimension: dimension,
                        inflater: simpleInflater
                    };
                }
                if (!splitExpression.expression.isOp('ref')) {
                    throw new Error("can not convert complex: " + splitExpression.expression.toString());
                }
                var actions = splitExpression.actions;
                if (actions.length !== 1)
                    throw new Error("can not convert expression: " + splitExpression.toString());
                var splitAction = actions[0];
                if (splitAction instanceof Plywood.SubstrAction) {
                    return {
                        dimension: dimension,
                        inflater: simpleInflater
                    };
                }
                if (splitAction instanceof Plywood.TimeBucketAction) {
                    var format = TIME_BUCKET_FORMAT[splitAction.duration.toString()];
                    if (!format)
                        throw new Error("unsupported part in timeBucket expression " + splitAction.duration.toString());
                    return {
                        dimension: dimension,
                        inflater: Plywood.External.timeRangeInflaterFactory(label, splitAction.duration, splitAction.timezone)
                    };
                }
                if (splitAction instanceof Plywood.TimePartAction) {
                    var format = TIME_PART_TO_FORMAT[splitAction.part];
                    if (!format)
                        throw new Error("unsupported part in timePart expression " + splitAction.part);
                    return {
                        dimension: dimension,
                        inflater: simpleMathInflaterFactory(label)
                    };
                }
                if (splitAction instanceof Plywood.NumberBucketAction) {
                    var attributeInfo = this.getAttributesInfo(referenceName);
                    if (attributeInfo.type === 'NUMBER') {
                        var floorExpression = Plywood.continuousFloorExpression("d", "Math.floor", splitAction.size, splitAction.offset);
                        return {
                            dimension: dimension,
                            inflater: Plywood.External.numberRangeInflaterFactory(label, splitAction.size)
                        };
                    }
                    if (attributeInfo instanceof Plywood.RangeAttributeInfo) {
                        return {
                            dimension: dimension,
                            inflater: Plywood.External.numberRangeInflaterFactory(label, splitAction.size)
                        };
                    }
                }
            }
            throw new Error("could not convert " + splitExpression.toString() + " to a Druid Dimension");
        };
        DruidExternal.prototype.splitToDruid = function () {
            var _this = this;
            var split = this.split;
            if (split.isMultiSplit()) {
                var timestampLabel = null;
                var granularity = null;
                var dimensions = [];
                var inflaters = [];
                split.mapSplits(function (name, expression) {
                    if (!granularity && !_this.limit && !_this.sort) {
                        var granularityInflater = _this.splitExpressionToGranularityInflater(expression, name);
                        if (granularityInflater) {
                            timestampLabel = name;
                            granularity = granularityInflater.granularity;
                            inflaters.push(granularityInflater.inflater);
                            return;
                        }
                    }
                    var _a = _this.splitExpressionToDimensionInflater(expression, name), dimension = _a.dimension, inflater = _a.inflater;
                    dimensions.push(dimension);
                    if (inflater) {
                        inflaters.push(inflater);
                    }
                });
                return {
                    queryType: 'groupBy',
                    dimensions: dimensions,
                    granularity: granularity || 'all',
                    postProcess: postProcessFactory(groupByNormalizerFactory(timestampLabel), inflaters)
                };
            }
            var splitExpression = split.firstSplitExpression();
            var label = split.firstSplitName();
            var granularityInflater = this.splitExpressionToGranularityInflater(splitExpression, label);
            if (granularityInflater) {
                return {
                    queryType: 'timeseries',
                    granularity: granularityInflater.granularity,
                    postProcess: postProcessFactory(timeseriesNormalizerFactory(label), [granularityInflater.inflater])
                };
            }
            var dimensionInflater = this.splitExpressionToDimensionInflater(splitExpression, label);
            var inflaters = [dimensionInflater.inflater].filter(Boolean);
            if (this.havingFilter.equals(Plywood.Expression.TRUE) && this.limit && !this.exactResultsOnly) {
                return {
                    queryType: 'topN',
                    dimension: dimensionInflater.dimension,
                    granularity: 'all',
                    postProcess: postProcessFactory(topNNormalizer, inflaters)
                };
            }
            return {
                queryType: 'groupBy',
                dimensions: [dimensionInflater.dimension],
                granularity: 'all',
                postProcess: postProcessFactory(groupByNormalizerFactory(), inflaters)
            };
        };
        DruidExternal.prototype.getAccessTypeForAggregation = function (aggregationType) {
            if (aggregationType === 'hyperUnique' || aggregationType === 'cardinality')
                return 'hyperUniqueCardinality';
            var customAggregations = this.customAggregations;
            for (var customName in customAggregations) {
                if (!hasOwnProperty(customAggregations, customName))
                    continue;
                var customAggregation = customAggregations[customName];
                if (customAggregation.aggregation.type === aggregationType) {
                    return customAggregation.accessType || 'fieldAccess';
                }
            }
            return 'fieldAccess';
        };
        DruidExternal.prototype.getAccessType = function (aggregations, aggregationName) {
            for (var _i = 0; _i < aggregations.length; _i++) {
                var aggregation = aggregations[_i];
                if (aggregation.name === aggregationName) {
                    return this.getAccessTypeForAggregation(aggregation.type);
                }
            }
            throw new Error("aggregation '" + aggregationName + "' not found");
        };
        DruidExternal.prototype.expressionToPostAggregation = function (ex, aggregations) {
            var _this = this;
            if (ex instanceof Plywood.RefExpression) {
                var refName = ex.name;
                return {
                    type: this.getAccessType(aggregations, refName),
                    fieldName: refName
                };
            }
            else if (ex instanceof Plywood.LiteralExpression) {
                if (ex.type !== 'NUMBER')
                    throw new Error("must be a NUMBER type");
                return {
                    type: 'constant',
                    value: ex.value
                };
            }
            else if (ex instanceof Plywood.ChainExpression) {
                var pattern;
                if (pattern = ex.getExpressionPattern('add')) {
                    return {
                        type: 'arithmetic',
                        fn: '+',
                        fields: pattern.map((function (e) { return _this.expressionToPostAggregation(e, aggregations); }), this)
                    };
                }
                if (pattern = ex.getExpressionPattern('subtract')) {
                    return {
                        type: 'arithmetic',
                        fn: '-',
                        fields: pattern.map((function (e) { return _this.expressionToPostAggregation(e, aggregations); }), this)
                    };
                }
                if (pattern = ex.getExpressionPattern('multiply')) {
                    return {
                        type: 'arithmetic',
                        fn: '*',
                        fields: pattern.map((function (e) { return _this.expressionToPostAggregation(e, aggregations); }), this)
                    };
                }
                if (pattern = ex.getExpressionPattern('divide')) {
                    return {
                        type: 'arithmetic',
                        fn: '/',
                        fields: pattern.map((function (e) { return _this.expressionToPostAggregation(e, aggregations); }), this)
                    };
                }
                throw new Error("can not convert chain to post agg: " + ex.toString());
            }
            else {
                throw new Error("can not convert expression to post agg: " + ex.toString());
            }
        };
        DruidExternal.prototype.applyToPostAggregation = function (action, aggregations) {
            var postAgg = this.expressionToPostAggregation(action.expression, aggregations);
            postAgg.name = action.name;
            return postAgg;
        };
        DruidExternal.prototype.makeStandardAggregation = function (name, filterAction, aggregateAction) {
            var fn = aggregateAction.action;
            var attribute = aggregateAction.expression;
            var aggregation = {
                name: name,
                type: AGGREGATE_TO_DRUID[fn]
            };
            if (fn !== 'count') {
                if (attribute instanceof Plywood.RefExpression) {
                    aggregation.fieldName = attribute.name;
                }
                else {
                    throw new Error('can not support complex derived attributes (yet)');
                }
            }
            if (filterAction) {
                if (this.canUseNativeAggregateFilter(filterAction.expression)) {
                    aggregation = {
                        type: "filtered",
                        name: name,
                        filter: this.timelessFilterToDruid(filterAction.expression),
                        aggregator: aggregation
                    };
                }
                else {
                    throw new Error("no support for JS filters (yet)");
                }
            }
            return aggregation;
        };
        DruidExternal.prototype.makeCountDistinctAggregation = function (name, filterAction, action) {
            if (this.exactResultsOnly) {
                throw new Error("approximate query not allowed");
            }
            if (filterAction) {
                throw new Error("filtering on countDistinct aggregator isn't supported");
            }
            var attribute = action.expression;
            if (attribute instanceof Plywood.RefExpression) {
                var attributeInfo = this.getAttributesInfo(attribute.name);
                if (attributeInfo instanceof Plywood.UniqueAttributeInfo) {
                    return {
                        name: name,
                        type: "hyperUnique",
                        fieldName: attribute.name
                    };
                }
                else {
                    return {
                        name: name,
                        type: "cardinality",
                        fieldNames: [attribute.name],
                        byRow: true
                    };
                }
            }
            else {
                throw new Error('can not compute distinctCount on derived attribute');
            }
        };
        DruidExternal.prototype.applyToAggregation = function (action) {
            var applyExpression = action.expression;
            if (applyExpression.op !== 'chain')
                throw new Error("can not convert apply: " + applyExpression.toString());
            var actions = applyExpression.actions;
            var filterAction = null;
            var aggregateAction = null;
            if (actions.length === 1) {
                aggregateAction = actions[0];
            }
            else if (actions.length === 2) {
                filterAction = actions[0];
                aggregateAction = actions[1];
            }
            else {
                throw new Error("can not convert strange apply: " + applyExpression.toString());
            }
            switch (aggregateAction.action) {
                case "count":
                case "sum":
                case "min":
                case "max":
                    return this.makeStandardAggregation(action.name, filterAction, aggregateAction);
                case "countDistinct":
                    return this.makeCountDistinctAggregation(action.name, filterAction, aggregateAction);
                case "quantile":
                    throw new Error("ToDo: add quantile support");
                case "custom":
                    var customAggregationName = aggregateAction.custom;
                    var customAggregation = this.customAggregations[customAggregationName];
                    if (!customAggregation)
                        throw new Error("could not find '" + customAggregationName + "'");
                    var aggregationObj = customAggregation.aggregation;
                    if (typeof aggregationObj.type !== 'string')
                        throw new Error("must have type in custom aggregation '" + customAggregationName + "'");
                    try {
                        aggregationObj = JSON.parse(JSON.stringify(aggregationObj));
                    }
                    catch (e) {
                        throw new Error("must have JSON custom aggregation '" + customAggregationName + "'");
                    }
                    aggregationObj.name = action.name;
                    return aggregationObj;
                default:
                    throw new Error("unsupported aggregate action " + aggregateAction.action);
            }
        };
        DruidExternal.prototype.processApply = function (apply) {
            var _this = this;
            return this.separateAggregates(apply.applyToExpression(function (ex) {
                return _this.inlineDerivedAttributes(ex).decomposeAverage().distribute();
            }));
        };
        DruidExternal.prototype.isAggregateExpression = function (expression) {
            if (expression instanceof Plywood.ChainExpression) {
                var actions = expression.actions;
                if (actions.length === 1) {
                    return actions[0].isAggregate();
                }
                else if (actions.length === 2) {
                    return actions[0].action === 'filter' && actions[1].isAggregate();
                }
                else {
                    return false;
                }
            }
            return false;
        };
        DruidExternal.prototype.getAggregationsAndPostAggregations = function () {
            var _this = this;
            var aggregations = [];
            var postAggregations = [];
            this.applies.forEach(function (apply) {
                var applyName = apply.name;
                if (_this.isAggregateExpression(apply.expression)) {
                    var aggregation = _this.applyToAggregation(apply);
                    aggregations = aggregations.filter(function (a) { return a.name !== applyName; });
                    aggregations.push(aggregation);
                }
                else {
                    var postAggregation = _this.applyToPostAggregation(apply, aggregations);
                    postAggregations = postAggregations.filter(function (a) { return a.name !== applyName; });
                    postAggregations.push(postAggregation);
                }
            });
            return {
                aggregations: aggregations,
                postAggregations: postAggregations
            };
        };
        DruidExternal.prototype.makeHavingComparison = function (agg, op, value) {
            switch (op) {
                case '<':
                    return { type: "lessThan", aggregation: agg, value: value };
                case '>':
                    return { type: "greaterThan", aggregation: agg, value: value };
                case '<=':
                    return { type: 'not', havingSpec: { type: "greaterThan", aggregation: agg, value: value } };
                case '>=':
                    return { type: 'not', havingSpec: { type: "lessThan", aggregation: agg, value: value } };
                default:
                    throw new Error('unknown op: ' + op);
            }
        };
        DruidExternal.prototype.inToHavingFilter = function (agg, range) {
            var havingSpecs = [];
            if (range.start !== null) {
                havingSpecs.push(this.makeHavingComparison(agg, (range.bounds[0] === '[' ? '>=' : '>'), range.start));
            }
            if (range.end !== null) {
                havingSpecs.push(this.makeHavingComparison(agg, (range.bounds[1] === ']' ? '<=' : '<'), range.end));
            }
            return havingSpecs.length === 1 ? havingSpecs[0] : { type: 'or', havingSpecs: havingSpecs };
        };
        DruidExternal.prototype.havingFilterToDruid = function (filter) {
            var _this = this;
            if (filter instanceof Plywood.LiteralExpression) {
                if (filter.value === true) {
                    return null;
                }
                else {
                    throw new Error("should never get here");
                }
            }
            else if (filter instanceof Plywood.ChainExpression) {
                var pattern;
                if (pattern = filter.getExpressionPattern('and')) {
                    return {
                        type: 'and',
                        havingSpecs: pattern.map(this.havingFilterToDruid, this)
                    };
                }
                if (pattern = filter.getExpressionPattern('or')) {
                    return {
                        type: 'or',
                        havingSpecs: pattern.map(this.havingFilterToDruid, this)
                    };
                }
                if (filter.lastAction() instanceof Plywood.NotAction) {
                    return this.havingFilterToDruid(filter.popAction());
                }
                var lhs = filter.expression;
                var actions = filter.actions;
                if (actions.length !== 1)
                    throw new Error("can not convert " + filter.toString() + " to Druid interval");
                var filterAction = actions[0];
                var rhs = filterAction.expression;
                if (filterAction instanceof Plywood.IsAction) {
                    if (lhs instanceof Plywood.RefExpression && rhs instanceof Plywood.LiteralExpression) {
                        return {
                            type: "equalTo",
                            aggregation: lhs.name,
                            value: rhs.value
                        };
                    }
                    else {
                        throw new Error("can not convert " + filter.toString() + " to Druid filter");
                    }
                }
                else if (filterAction instanceof Plywood.InAction) {
                    if (lhs instanceof Plywood.RefExpression && rhs instanceof Plywood.LiteralExpression) {
                        var rhsType = rhs.type;
                        if (rhsType === 'SET/STRING') {
                            return {
                                type: "or",
                                havingSpecs: rhs.value.elements.map(function (value) {
                                    return {
                                        type: "equalTo",
                                        aggregation: lhs.name,
                                        value: value
                                    };
                                })
                            };
                        }
                        else if (rhsType === 'SET/NUMBER_RANGE') {
                            return {
                                type: "or",
                                havingSpecs: rhs.value.elements.map(function (value) {
                                    return _this.inToHavingFilter(lhs.name, value);
                                }, this)
                            };
                        }
                        else if (rhsType === 'NUMBER_RANGE') {
                            return this.inToHavingFilter(lhs.name, rhs.value);
                        }
                        else if (rhsType === 'TIME_RANGE') {
                            throw new Error("can not time filter on non-primary time dimension");
                        }
                        else {
                            throw new Error("not supported " + rhsType);
                        }
                    }
                    else {
                        throw new Error("can not convert " + filter.toString() + " to Druid having filter");
                    }
                }
            }
            else {
                throw new Error("could not convert filter " + filter.toString() + " to Druid filter");
            }
        };
        DruidExternal.prototype.isMinMaxTimeApply = function (apply) {
            var applyExpression = apply.expression;
            if (applyExpression instanceof Plywood.ChainExpression) {
                var actions = applyExpression.actions;
                if (actions.length !== 1)
                    return false;
                var minMaxAction = actions[0];
                return (minMaxAction.action === "min" || minMaxAction.action === "max") &&
                    this.isTimeRef(minMaxAction.expression);
            }
            else {
                return false;
            }
        };
        DruidExternal.prototype.getTimeBoundaryQueryAndPostProcess = function () {
            var druidQuery = {
                queryType: "timeBoundary",
                dataSource: this.getDruidDataSource()
            };
            var applies = this.applies;
            if (applies.length === 1) {
                var loneApplyExpression = applies[0].expression;
                druidQuery.bound = loneApplyExpression.actions[0].action + "Time";
            }
            return {
                query: druidQuery,
                postProcess: timeBoundaryPostProcessFactory(this.applies)
            };
        };
        DruidExternal.prototype.getQueryAndPostProcess = function () {
            var applies = this.applies;
            if (applies && applies.length && applies.every(this.isMinMaxTimeApply, this)) {
                return this.getTimeBoundaryQueryAndPostProcess();
            }
            var druidQuery = {
                queryType: 'timeseries',
                dataSource: this.getDruidDataSource(),
                intervals: null,
                granularity: 'all'
            };
            if (this.context) {
                druidQuery.context = this.context;
            }
            var filterAndIntervals = this.filterToDruid(this.filter);
            druidQuery.intervals = filterAndIntervals.intervals;
            if (filterAndIntervals.filter) {
                druidQuery.filter = filterAndIntervals.filter;
            }
            switch (this.mode) {
                case 'raw':
                    if (!this.allowSelectQueries) {
                        throw new Error("to issues 'select' queries allowSelectQueries flag must be set");
                    }
                    druidQuery.queryType = 'select';
                    druidQuery.dimensions = [];
                    druidQuery.metrics = [];
                    druidQuery.pagingSpec = {
                        "pagingIdentifiers": {},
                        "threshold": this.limit ? this.limit.limit : 10000
                    };
                    return {
                        query: druidQuery,
                        postProcess: postProcessFactory(selectNormalizer, [])
                    };
                case 'total':
                    var aggregationsAndPostAggregations = this.getAggregationsAndPostAggregations();
                    if (aggregationsAndPostAggregations.aggregations.length) {
                        druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
                    }
                    if (aggregationsAndPostAggregations.postAggregations.length) {
                        druidQuery.postAggregations = aggregationsAndPostAggregations.postAggregations;
                    }
                    return {
                        query: druidQuery,
                        postProcess: totalPostProcessFactory(this.applies)
                    };
                case 'split':
                    var aggregationsAndPostAggregations = this.getAggregationsAndPostAggregations();
                    if (aggregationsAndPostAggregations.aggregations.length) {
                        druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
                    }
                    else {
                        druidQuery.aggregations = [{ name: DUMMY_NAME, type: "count" }];
                    }
                    if (aggregationsAndPostAggregations.postAggregations.length) {
                        druidQuery.postAggregations = aggregationsAndPostAggregations.postAggregations;
                    }
                    var splitSpec = this.splitToDruid();
                    druidQuery.queryType = splitSpec.queryType;
                    druidQuery.granularity = splitSpec.granularity;
                    if (splitSpec.dimension)
                        druidQuery.dimension = splitSpec.dimension;
                    if (splitSpec.dimensions)
                        druidQuery.dimensions = splitSpec.dimensions;
                    var postProcess = splitSpec.postProcess;
                    switch (druidQuery.queryType) {
                        case 'timeseries':
                            if (this.sort && (this.sort.direction !== 'ascending' || !this.split.hasKey(this.sort.refName()))) {
                                throw new Error('can not sort within timeseries query');
                            }
                            if (this.limit) {
                                throw new Error('can not limit within timeseries query');
                            }
                            break;
                        case 'topN':
                            var sortAction = this.sort;
                            var metric;
                            if (sortAction) {
                                metric = sortAction.expression.name;
                                if (this.sortOnLabel()) {
                                    metric = { type: 'lexicographic' };
                                }
                                if (sortAction.direction === 'ascending') {
                                    metric = { type: "inverted", metric: metric };
                                }
                            }
                            else {
                                metric = { type: 'lexicographic' };
                            }
                            druidQuery.metric = metric;
                            if (this.limit) {
                                druidQuery.threshold = this.limit.limit;
                            }
                            break;
                        case 'groupBy':
                            var sortAction = this.sort;
                            druidQuery.limitSpec = {
                                type: "default",
                                limit: 500000,
                                columns: [
                                    sortAction ?
                                        { dimension: sortAction.expression.name, direction: sortAction.direction }
                                        : this.split.firstSplitName()
                                ]
                            };
                            if (this.limit) {
                                druidQuery.limitSpec.limit = this.limit.limit;
                            }
                            if (!this.havingFilter.equals(Plywood.Expression.TRUE)) {
                                druidQuery.having = this.havingFilterToDruid(this.havingFilter);
                            }
                            break;
                    }
                    return {
                        query: druidQuery,
                        postProcess: postProcess
                    };
                default:
                    throw new Error("can not get query for: " + this.mode);
            }
        };
        DruidExternal.prototype.getIntrospectQueryAndPostProcess = function () {
            if (this.useSegmentMetadata) {
                return {
                    query: {
                        queryType: 'segmentMetadata',
                        dataSource: this.getDruidDataSource(),
                        merge: true,
                        analysisTypes: []
                    },
                    postProcess: segmentMetadataPostProcessFactory(this.timeAttribute)
                };
            }
            else {
                return {
                    query: {
                        queryType: 'introspect',
                        dataSource: this.getDruidDataSource()
                    },
                    postProcess: introspectPostProcessFactory(this.timeAttribute)
                };
            }
        };
        DruidExternal.type = 'DATASET';
        DruidExternal.TRUE_INTERVAL = ["1000-01-01/3000-01-01"];
        DruidExternal.FALSE_INTERVAL = ["1000-01-01/1000-01-02"];
        return DruidExternal;
    })(Plywood.External);
    Plywood.DruidExternal = DruidExternal;
    Plywood.External.register(DruidExternal);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var mySQLDialect = new Plywood.MySQLDialect();
    function correctResult(result) {
        return Array.isArray(result) && (result.length === 0 || typeof result[0] === 'object');
    }
    function postProcessFactory(split) {
        var inflaters = split ? split.mapSplits(function (label, splitExpression) {
            if (splitExpression instanceof Plywood.ChainExpression) {
                var lastAction = splitExpression.lastAction();
                if (lastAction instanceof Plywood.TimeBucketAction) {
                    return Plywood.External.timeRangeInflaterFactory(label, lastAction.duration, lastAction.timezone);
                }
                if (lastAction instanceof Plywood.NumberBucketAction) {
                    return Plywood.External.numberRangeInflaterFactory(label, lastAction.size);
                }
            }
        }) : [];
        return function (data) {
            if (!correctResult(data)) {
                var err = new Error("unexpected result from MySQL");
                err.result = data;
                throw err;
            }
            var n = data.length;
            for (var _i = 0; _i < inflaters.length; _i++) {
                var inflater = inflaters[_i];
                for (var i = 0; i < n; i++) {
                    inflater(data[i], i, data);
                }
            }
            return new Plywood.Dataset({ data: data });
        };
    }
    function postProcessIntrospect(columns) {
        return columns.map(function (column) {
            var name = column.Field;
            var sqlType = column.Type;
            if (sqlType === "datetime") {
                return new Plywood.AttributeInfo({ name: name, type: 'TIME' });
            }
            else if (sqlType.indexOf("varchar(") === 0) {
                return new Plywood.AttributeInfo({ name: name, type: 'STRING' });
            }
            else if (sqlType.indexOf("int(") === 0 || sqlType.indexOf("bigint(") === 0) {
                return new Plywood.AttributeInfo({ name: name, type: 'NUMBER' });
            }
            else if (sqlType.indexOf("decimal(") === 0) {
                return new Plywood.AttributeInfo({ name: name, type: 'NUMBER' });
            }
        });
    }
    var MySQLExternal = (function (_super) {
        __extends(MySQLExternal, _super);
        function MySQLExternal(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureEngine("mysql");
            this.table = parameters.table;
        }
        MySQLExternal.fromJS = function (datasetJS) {
            var value = Plywood.External.jsToValue(datasetJS);
            value.table = datasetJS.table;
            return new MySQLExternal(value);
        };
        MySQLExternal.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.table = this.table;
            return value;
        };
        MySQLExternal.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.table = this.table;
            return js;
        };
        MySQLExternal.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.table === other.table;
        };
        MySQLExternal.prototype.getId = function () {
            return _super.prototype.getId.call(this) + ':' + this.table;
        };
        MySQLExternal.prototype.canHandleFilter = function (ex) {
            return true;
        };
        MySQLExternal.prototype.canHandleTotal = function () {
            return true;
        };
        MySQLExternal.prototype.canHandleSplit = function (ex) {
            return true;
        };
        MySQLExternal.prototype.canHandleApply = function (ex) {
            return true;
        };
        MySQLExternal.prototype.canHandleSort = function (sortAction) {
            return true;
        };
        MySQLExternal.prototype.canHandleLimit = function (limitAction) {
            return true;
        };
        MySQLExternal.prototype.canHandleHavingFilter = function (ex) {
            return true;
        };
        MySQLExternal.prototype.getQueryAndPostProcess = function () {
            var table = "`" + this.table + "`";
            var query = ['SELECT'];
            switch (this.mode) {
                case 'raw':
                    query.push(this.attributes.map(function (a) { return mySQLDialect.escapeName(a.name); }).join(', '));
                    query.push('FROM ' + table);
                    if (!(this.filter.equals(Plywood.Expression.TRUE))) {
                        query.push('WHERE ' + this.filter.getSQL(mySQLDialect));
                    }
                    if (this.sort) {
                        query.push(this.sort.getSQL('', mySQLDialect));
                    }
                    if (this.limit) {
                        query.push(this.limit.getSQL('', mySQLDialect));
                    }
                    break;
                case 'total':
                    query.push(this.applies.map(function (apply) { return apply.getSQL('', mySQLDialect); }).join(',\n'));
                    query.push('FROM ' + table);
                    if (!(this.filter.equals(Plywood.Expression.TRUE))) {
                        query.push('WHERE ' + this.filter.getSQL(mySQLDialect));
                    }
                    query.push("GROUP BY ''");
                    break;
                case 'split':
                    query.push(this.split.getSelectSQL(mySQLDialect)
                        .concat(this.applies.map(function (apply) { return apply.getSQL('', mySQLDialect); }))
                        .join(',\n'));
                    query.push('FROM ' + table);
                    if (!(this.filter.equals(Plywood.Expression.TRUE))) {
                        query.push('WHERE ' + this.filter.getSQL(mySQLDialect));
                    }
                    query.push(this.split.getShortGroupBySQL());
                    if (!(this.havingFilter.equals(Plywood.Expression.TRUE))) {
                        query.push('HAVING ' + this.havingFilter.getSQL(mySQLDialect));
                    }
                    if (this.sort) {
                        query.push(this.sort.getSQL('', mySQLDialect));
                    }
                    if (this.limit) {
                        query.push(this.limit.getSQL('', mySQLDialect));
                    }
                    break;
                default:
                    throw new Error("can not get query for: " + this.mode);
            }
            return {
                query: query.join('\n'),
                postProcess: postProcessFactory(this.split)
            };
        };
        MySQLExternal.prototype.getIntrospectQueryAndPostProcess = function () {
            return {
                query: "DESCRIBE `" + this.table + "`",
                postProcess: postProcessIntrospect
            };
        };
        MySQLExternal.type = 'DATASET';
        return MySQLExternal;
    })(Plywood.External);
    Plywood.MySQLExternal = MySQLExternal;
    Plywood.External.register(MySQLExternal, 'mysql');
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    function getDataName(ex) {
        if (ex instanceof Plywood.RefExpression) {
            return ex.name;
        }
        else if (ex instanceof Plywood.ChainExpression) {
            return getDataName(ex.expression);
        }
        else {
            return null;
        }
    }
    function mergeRemotes(remotes) {
        var lookup = {};
        for (var _i = 0; _i < remotes.length; _i++) {
            var remote = remotes[_i];
            if (!remote)
                continue;
            for (var _a = 0; _a < remote.length; _a++) {
                var r_1 = remote[_a];
                lookup[r_1] = true;
            }
        }
        var merged = Object.keys(lookup);
        return merged.length ? merged.sort() : null;
    }
    Plywood.mergeRemotes = mergeRemotes;
    function getValue(param) {
        if (param instanceof Plywood.LiteralExpression)
            return param.value;
        return param;
    }
    function getString(param) {
        if (typeof param === 'string')
            return param;
        if (param instanceof Plywood.LiteralExpression && param.type === 'STRING') {
            return param.value;
        }
        throw new Error('could not extract a string out of ' + String(param));
    }
    function getNumber(param) {
        if (typeof param === 'number')
            return param;
        if (param instanceof Plywood.LiteralExpression && param.type === 'NUMBER') {
            return param.value;
        }
        throw new Error('could not extract a number out of ' + String(param));
    }
    function ply(dataset) {
        if (!dataset)
            dataset = new Plywood.Dataset({ data: [{}] });
        return r(dataset);
    }
    Plywood.ply = ply;
    function $(name, nest, type) {
        if (typeof name !== 'string')
            throw new TypeError('name must be a string');
        if (typeof nest === 'string') {
            type = nest;
            nest = 0;
        }
        return new Plywood.RefExpression({
            name: name,
            nest: nest != null ? nest : 0,
            type: type
        });
    }
    Plywood.$ = $;
    function r(value) {
        if (Plywood.External.isExternal(value))
            throw new TypeError('r can not accept externals');
        if (Array.isArray(value))
            value = Plywood.Set.fromJS(value);
        return Plywood.LiteralExpression.fromJS({ op: 'literal', value: value });
    }
    Plywood.r = r;
    function chainVia(op, expressions, zero) {
        switch (expressions.length) {
            case 0: return zero;
            case 1: return expressions[0];
            default:
                var acc = expressions[0];
                for (var i = 1; i < expressions.length; i++) {
                    acc = acc[op](expressions[i]);
                }
                return acc;
        }
    }
    var check;
    var Expression = (function () {
        function Expression(parameters, dummy) {
            if (dummy === void 0) { dummy = null; }
            this.op = parameters.op;
            if (dummy !== dummyObject) {
                throw new TypeError("can not call `new Expression` directly use Expression.fromJS instead");
            }
            if (parameters.simple)
                this.simple = true;
        }
        Expression.isExpression = function (candidate) {
            return Plywood.isInstanceOf(candidate, Expression);
        };
        Expression.parse = function (str) {
            try {
                return expressionParser.parse(str);
            }
            catch (e) {
                throw new Error('Expression parse error: ' + e.message + ' on `' + str + '`');
            }
        };
        Expression.parseSQL = function (str) {
            try {
                return sqlParser.parse(str);
            }
            catch (e) {
                throw new Error('SQL parse error: ' + e.message + ' on `' + str + '`');
            }
        };
        Expression.fromJSLoose = function (param) {
            var expressionJS;
            switch (typeof param) {
                case 'object':
                    if (param === null) {
                        return Expression.NULL;
                    }
                    else if (Expression.isExpression(param)) {
                        return param;
                    }
                    else if (Plywood.isImmutableClass(param)) {
                        if (param.constructor.type) {
                            expressionJS = { op: 'literal', value: param };
                        }
                        else {
                            throw new Error("unknown object");
                        }
                    }
                    else if (param.op) {
                        expressionJS = param;
                    }
                    else if (param.toISOString) {
                        expressionJS = { op: 'literal', value: new Date(param) };
                    }
                    else if (Array.isArray(param)) {
                        expressionJS = { op: 'literal', value: Plywood.Set.fromJS(param) };
                    }
                    else if (hasOwnProperty(param, 'start') && hasOwnProperty(param, 'end')) {
                        expressionJS = { op: 'literal', value: Plywood.Range.fromJS(param) };
                    }
                    else {
                        throw new Error('unknown parameter');
                    }
                    break;
                case 'number':
                case 'boolean':
                    expressionJS = { op: 'literal', value: param };
                    break;
                case 'string':
                    return Expression.parse(param);
                default:
                    throw new Error("unrecognizable expression");
            }
            return Expression.fromJS(expressionJS);
        };
        Expression.inOrIs = function (lhs, value) {
            var literal = new Plywood.LiteralExpression({
                op: 'literal',
                value: value
            });
            var literalType = literal.type;
            var returnExpression = null;
            if (literalType === 'NUMBER_RANGE' || literalType === 'TIME_RANGE' || literalType.indexOf('SET/') === 0) {
                returnExpression = lhs.in(literal);
            }
            else {
                returnExpression = lhs.is(literal);
            }
            return returnExpression.simplify();
        };
        Expression.and = function (expressions) {
            return chainVia('and', expressions, Expression.TRUE);
        };
        Expression.or = function (expressions) {
            return chainVia('or', expressions, Expression.FALSE);
        };
        Expression.add = function (expressions) {
            return chainVia('add', expressions, Expression.ZERO);
        };
        Expression.subtract = function (expressions) {
            return chainVia('subtract', expressions, Expression.ZERO);
        };
        Expression.multiply = function (expressions) {
            return chainVia('multiply', expressions, Expression.ONE);
        };
        Expression.concat = function (expressions) {
            return chainVia('concat', expressions, Expression.EMPTY_STRING);
        };
        Expression.register = function (ex) {
            var op = ex.name.replace('Expression', '').replace(/^\w/, function (s) { return s.toLowerCase(); });
            Expression.classMap[op] = ex;
        };
        Expression.fromJS = function (expressionJS) {
            if (!hasOwnProperty(expressionJS, "op")) {
                throw new Error("op must be defined");
            }
            var op = expressionJS.op;
            if (typeof op !== "string") {
                throw new Error("op must be a string");
            }
            var ClassFn = Expression.classMap[op];
            if (!ClassFn) {
                throw new Error("unsupported expression op '" + op + "'");
            }
            return ClassFn.fromJS(expressionJS);
        };
        Expression.prototype._ensureOp = function (op) {
            if (!this.op) {
                this.op = op;
                return;
            }
            if (this.op !== op) {
                throw new TypeError("incorrect expression op '" + this.op + "' (needs to be: '" + op + "')");
            }
        };
        Expression.prototype.valueOf = function () {
            var value = { op: this.op };
            if (this.simple)
                value.simple = true;
            return value;
        };
        Expression.prototype.toJS = function () {
            return {
                op: this.op
            };
        };
        Expression.prototype.toJSON = function () {
            return this.toJS();
        };
        Expression.prototype.toString = function (indent) {
            return 'BaseExpression';
        };
        Expression.prototype.equals = function (other) {
            return Expression.isExpression(other) &&
                this.op === other.op &&
                this.type === other.type;
        };
        Expression.prototype.canHaveType = function (wantedType) {
            if (!this.type)
                return true;
            if (wantedType === 'SET') {
                return this.type.indexOf('SET/') === 0;
            }
            else {
                return this.type === wantedType;
            }
        };
        Expression.prototype.expressionCount = function () {
            return 1;
        };
        Expression.prototype.isOp = function (op) {
            return this.op === op;
        };
        Expression.prototype.containsOp = function (op) {
            return this.some(function (ex) { return ex.isOp(op) || null; });
        };
        Expression.prototype.hasExternal = function () {
            return this.some(function (ex) {
                if (ex instanceof Plywood.ExternalExpression)
                    return true;
                if (ex instanceof Plywood.RefExpression)
                    return ex.isRemote();
                return null;
            });
        };
        Expression.prototype.getExternalIds = function () {
            var externalIds = [];
            var push = Array.prototype.push;
            this.forEach(function (ex) {
                if (ex.type !== 'DATASET')
                    return;
                if (ex instanceof Plywood.LiteralExpression) {
                    push.apply(externalIds, ex.value.getExternalIds());
                }
                else if (ex instanceof Plywood.RefExpression) {
                    push.apply(externalIds, ex.remote);
                }
            });
            return deduplicateSort(externalIds);
        };
        Expression.prototype.getExternals = function () {
            var externals = [];
            this.forEach(function (ex) {
                if (ex instanceof Plywood.ExternalExpression)
                    externals.push(ex.external);
            });
            return Plywood.mergeExternals([externals]);
        };
        Expression.prototype.getFreeReferences = function () {
            var freeReferences = [];
            this.forEach(function (ex, index, depth, nestDiff) {
                if (ex instanceof Plywood.RefExpression && nestDiff <= ex.nest) {
                    freeReferences.push(repeat('^', ex.nest - nestDiff) + ex.name);
                }
            });
            return deduplicateSort(freeReferences);
        };
        Expression.prototype.getFreeReferenceIndexes = function () {
            var freeReferenceIndexes = [];
            this.forEach(function (ex, index, depth, nestDiff) {
                if (ex instanceof Plywood.RefExpression && nestDiff <= ex.nest) {
                    freeReferenceIndexes.push(index);
                }
            });
            return freeReferenceIndexes;
        };
        Expression.prototype.incrementNesting = function (by) {
            if (by === void 0) { by = 1; }
            var freeReferenceIndexes = this.getFreeReferenceIndexes();
            if (freeReferenceIndexes.length === 0)
                return this;
            return this.substitute(function (ex, index) {
                if (ex instanceof Plywood.RefExpression && freeReferenceIndexes.indexOf(index) !== -1) {
                    return ex.incrementNesting(by);
                }
                return null;
            });
        };
        Expression.prototype.simplify = function () {
            return this;
        };
        Expression.prototype.every = function (iter, thisArg) {
            return this._everyHelper(iter, thisArg, { index: 0 }, 0, 0);
        };
        Expression.prototype._everyHelper = function (iter, thisArg, indexer, depth, nestDiff) {
            var pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
            if (pass != null) {
                return pass;
            }
            else {
                indexer.index++;
            }
            return true;
        };
        Expression.prototype.some = function (iter, thisArg) {
            var _this = this;
            return !this.every(function (ex, index, depth, nestDiff) {
                var v = iter.call(_this, ex, index, depth, nestDiff);
                return (v == null) ? null : !v;
            }, thisArg);
        };
        Expression.prototype.forEach = function (iter, thisArg) {
            var _this = this;
            this.every(function (ex, index, depth, nestDiff) {
                iter.call(_this, ex, index, depth, nestDiff);
                return null;
            }, thisArg);
        };
        Expression.prototype.substitute = function (substitutionFn, thisArg) {
            return this._substituteHelper(substitutionFn, thisArg, { index: 0 }, 0, 0);
        };
        Expression.prototype._substituteHelper = function (substitutionFn, thisArg, indexer, depth, nestDiff) {
            var sub = substitutionFn.call(thisArg, this, indexer.index, depth, nestDiff);
            if (sub) {
                indexer.index += this.expressionCount();
                return sub;
            }
            else {
                indexer.index++;
            }
            return this;
        };
        Expression.prototype.substituteAction = function (actionMatchFn, actionSubstitutionFn, thisArg) {
            var _this = this;
            return this.substitute(function (ex) {
                if (ex instanceof Plywood.ChainExpression) {
                    var actions = ex.actions;
                    for (var i = 0; i < actions.length; i++) {
                        var action = actions[i];
                        if (actionMatchFn.call(_this, action)) {
                            var preEx = ex.expression;
                            if (i) {
                                preEx = new Plywood.ChainExpression({
                                    expression: preEx,
                                    actions: actions.slice(0, i)
                                });
                            }
                            var newEx = actionSubstitutionFn.call(_this, preEx, action);
                            for (var j = i + 1; j < actions.length; j++)
                                newEx = newEx.performAction(actions[j]);
                            return newEx.substituteAction(actionMatchFn, actionSubstitutionFn, _this);
                        }
                    }
                }
                return null;
            }, thisArg);
        };
        Expression.prototype.getFn = function () {
            throw new Error('should never be called directly');
        };
        Expression.prototype.getJS = function (datumVar) {
            throw new Error('should never be called directly');
        };
        Expression.prototype.getJSFn = function (datumVar) {
            if (datumVar === void 0) { datumVar = 'd[]'; }
            return "function(" + datumVar.replace('[]', '') + "){return " + this.getJS(datumVar) + ";}";
        };
        Expression.prototype.getSQL = function (dialect) {
            throw new Error('should never be called directly');
        };
        Expression.prototype.separateViaAnd = function (refName) {
            if (typeof refName !== 'string')
                throw new Error('must have refName');
            if (this.type !== 'BOOLEAN')
                return null;
            var myRef = this.getFreeReferences();
            if (myRef.length > 1 && myRef.indexOf(refName) !== -1)
                return null;
            if (myRef[0] === refName) {
                return {
                    included: this,
                    excluded: Expression.TRUE
                };
            }
            else {
                return {
                    included: Expression.TRUE,
                    excluded: this
                };
            }
        };
        Expression.prototype.breakdownByDataset = function (tempNamePrefix) {
            var nameIndex = 0;
            var singleDatasetActions = [];
            var externals = this.getExternalIds();
            if (externals.length < 2) {
                throw new Error('not a multiple dataset expression');
            }
            var combine = this.substitute(function (ex) {
                var externals = ex.getExternalIds();
                if (externals.length !== 1)
                    return null;
                var existingApply = Plywood.find(singleDatasetActions, function (apply) { return apply.expression.equals(ex); });
                var tempName;
                if (existingApply) {
                    tempName = existingApply.name;
                }
                else {
                    tempName = tempNamePrefix + (nameIndex++);
                    singleDatasetActions.push(new Plywood.ApplyAction({
                        action: 'apply',
                        name: tempName,
                        expression: ex
                    }));
                }
                return new Plywood.RefExpression({
                    op: 'ref',
                    name: tempName,
                    nest: 0
                });
            });
            return {
                combineExpression: combine,
                singleDatasetActions: singleDatasetActions
            };
        };
        Expression.prototype.actionize = function (containingAction) {
            return null;
        };
        Expression.prototype.getExpressionPattern = function (actionType) {
            var actions = this.actionize(actionType);
            return actions ? actions.map(function (action) { return action.expression; }) : null;
        };
        Expression.prototype.firstAction = function () {
            return null;
        };
        Expression.prototype.lastAction = function () {
            return null;
        };
        Expression.prototype.popAction = function () {
            return null;
        };
        Expression.prototype.getLiteralValue = function () {
            return null;
        };
        Expression.prototype.performAction = function (action, markSimple) {
            if (!action)
                throw new Error('must have action');
            return new Plywood.ChainExpression({
                op: 'chain',
                expression: this,
                actions: [action],
                simple: Boolean(markSimple)
            });
        };
        Expression.prototype._performMultiAction = function (action, exs) {
            if (!exs.length)
                throw new Error(action + " action must have at least one argument");
            var ret = this;
            for (var _i = 0; _i < exs.length; _i++) {
                var ex = exs[_i];
                if (!Expression.isExpression(ex))
                    ex = Expression.fromJSLoose(ex);
                ret = ret.performAction(new Plywood.Action.classMap[action]({ expression: ex }));
            }
            return ret;
        };
        Expression.prototype.add = function () {
            var exs = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                exs[_i - 0] = arguments[_i];
            }
            return this._performMultiAction('add', exs);
        };
        Expression.prototype.subtract = function () {
            var exs = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                exs[_i - 0] = arguments[_i];
            }
            return this._performMultiAction('subtract', exs);
        };
        Expression.prototype.negate = function () {
            return Expression.ZERO.subtract(this);
        };
        Expression.prototype.multiply = function () {
            var exs = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                exs[_i - 0] = arguments[_i];
            }
            return this._performMultiAction('multiply', exs);
        };
        Expression.prototype.divide = function () {
            var exs = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                exs[_i - 0] = arguments[_i];
            }
            return this._performMultiAction('divide', exs);
        };
        Expression.prototype.reciprocate = function () {
            return Expression.ONE.divide(this);
        };
        Expression.prototype.is = function (ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.IsAction({ expression: ex }));
        };
        Expression.prototype.isnt = function (ex) {
            return this.is(ex).not();
        };
        Expression.prototype.lessThan = function (ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.LessThanAction({ expression: ex }));
        };
        Expression.prototype.lessThanOrEqual = function (ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.LessThanOrEqualAction({ expression: ex }));
        };
        Expression.prototype.greaterThan = function (ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.GreaterThanAction({ expression: ex }));
        };
        Expression.prototype.greaterThanOrEqual = function (ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.GreaterThanOrEqualAction({ expression: ex }));
        };
        Expression.prototype.contains = function (ex, compare) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            if (compare)
                compare = getString(compare);
            return this.performAction(new Plywood.ContainsAction({ expression: ex, compare: compare }));
        };
        Expression.prototype.match = function (re) {
            return this.performAction(new Plywood.MatchAction({ regexp: getString(re) }));
        };
        Expression.prototype.in = function (ex, snd) {
            if (arguments.length === 2) {
                ex = getValue(ex);
                snd = getValue(snd);
                if (typeof ex === 'string') {
                    ex = new Date(ex);
                    if (isNaN(ex.valueOf()))
                        throw new Error('can not convert start to date');
                }
                if (typeof snd === 'string') {
                    snd = new Date(snd);
                    if (isNaN(snd.valueOf()))
                        throw new Error('can not convert end to date');
                }
                if (typeof ex === 'number' && typeof snd === 'number') {
                    ex = new Plywood.NumberRange({ start: ex, end: snd });
                }
                else if (ex.toISOString && snd.toISOString) {
                    ex = new Plywood.TimeRange({ start: ex, end: snd });
                }
                else {
                    throw new Error('uninterpretable IN parameters');
                }
            }
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.InAction({ expression: ex }));
        };
        Expression.prototype.not = function () {
            return this.performAction(new Plywood.NotAction({}));
        };
        Expression.prototype.and = function () {
            var exs = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                exs[_i - 0] = arguments[_i];
            }
            return this._performMultiAction('and', exs);
        };
        Expression.prototype.or = function () {
            var exs = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                exs[_i - 0] = arguments[_i];
            }
            return this._performMultiAction('or', exs);
        };
        Expression.prototype.substr = function (position, length) {
            return this.performAction(new Plywood.SubstrAction({ position: getNumber(position), length: getNumber(length) }));
        };
        Expression.prototype.extract = function (re) {
            return this.performAction(new Plywood.ExtractAction({ regexp: getString(re) }));
        };
        Expression.prototype.concat = function () {
            var exs = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                exs[_i - 0] = arguments[_i];
            }
            return this._performMultiAction('concat', exs);
        };
        Expression.prototype.lookup = function (lookup) {
            return this.performAction(new Plywood.LookupAction({ lookup: getString(lookup) }));
        };
        Expression.prototype.numberBucket = function (size, offset) {
            if (offset === void 0) { offset = 0; }
            return this.performAction(new Plywood.NumberBucketAction({ size: getNumber(size), offset: getNumber(offset) }));
        };
        Expression.prototype.timeBucket = function (duration, timezone) {
            if (timezone === void 0) { timezone = Plywood.Timezone.UTC; }
            if (!Plywood.Duration.isDuration(duration))
                duration = Plywood.Duration.fromJS(getString(duration));
            if (!Plywood.Timezone.isTimezone(timezone))
                timezone = Plywood.Timezone.fromJS(getString(timezone));
            return this.performAction(new Plywood.TimeBucketAction({ duration: duration, timezone: timezone }));
        };
        Expression.prototype.timePart = function (part, timezone) {
            if (!Plywood.Timezone.isTimezone(timezone))
                timezone = Plywood.Timezone.fromJS(getString(timezone));
            return this.performAction(new Plywood.TimePartAction({ part: getString(part), timezone: timezone }));
        };
        Expression.prototype.timeOffset = function (duration, timezone) {
            if (!Plywood.Duration.isDuration(duration))
                duration = Plywood.Duration.fromJS(getString(duration));
            if (!Plywood.Timezone.isTimezone(timezone))
                timezone = Plywood.Timezone.fromJS(getString(timezone));
            return this.performAction(new Plywood.TimeOffsetAction({ duration: duration, timezone: timezone }));
        };
        Expression.prototype.filter = function (ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.FilterAction({ expression: ex }));
        };
        Expression.prototype.split = function (splits, name, dataName) {
            if (arguments.length === 3 ||
                (arguments.length === 2 && splits && (typeof splits === 'string' || typeof splits.op === 'string'))) {
                name = getString(name);
                var realSplits = Object.create(null);
                realSplits[name] = splits;
                splits = realSplits;
            }
            else {
                dataName = name;
            }
            var parsedSplits = Object.create(null);
            for (var k in splits) {
                if (!hasOwnProperty(splits, k))
                    continue;
                var ex = splits[k];
                parsedSplits[k] = Expression.isExpression(ex) ? ex : Expression.fromJSLoose(ex);
            }
            dataName = dataName ? getString(dataName) : getDataName(this);
            if (!dataName)
                throw new Error("could not guess data name in `split`, please provide one explicitly");
            return this.performAction(new Plywood.SplitAction({ splits: parsedSplits, dataName: dataName }));
        };
        Expression.prototype.apply = function (name, ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.ApplyAction({ name: getString(name), expression: ex }));
        };
        Expression.prototype.sort = function (ex, direction) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.SortAction({ expression: ex, direction: getString(direction) }));
        };
        Expression.prototype.limit = function (limit) {
            return this.performAction(new Plywood.LimitAction({ limit: getNumber(limit) }));
        };
        Expression.prototype.count = function () {
            return this.performAction(new Plywood.CountAction({}));
        };
        Expression.prototype.sum = function (ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.SumAction({ expression: ex }));
        };
        Expression.prototype.min = function (ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.MinAction({ expression: ex }));
        };
        Expression.prototype.max = function (ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.MaxAction({ expression: ex }));
        };
        Expression.prototype.average = function (ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.AverageAction({ expression: ex }));
        };
        Expression.prototype.countDistinct = function (ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.CountDistinctAction({ expression: ex }));
        };
        Expression.prototype.quantile = function (ex, quantile) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.QuantileAction({ expression: ex, quantile: getNumber(quantile) }));
        };
        Expression.prototype.custom = function (custom) {
            return this.performAction(new Plywood.CustomAction({ custom: getString(custom) }));
        };
        Expression.prototype.join = function (ex) {
            if (!Expression.isExpression(ex))
                ex = Expression.fromJSLoose(ex);
            return this.performAction(new Plywood.JoinAction({ expression: ex }));
        };
        Expression.prototype.attach = function (selector, prop) {
            return this.performAction(new Plywood.AttachAction({
                selector: selector,
                prop: prop
            }));
        };
        Expression.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            indexer.index++;
            return typeContext;
        };
        Expression.prototype.referenceCheck = function (context) {
            var datasetType = {};
            for (var k in context) {
                if (!hasOwnProperty(context, k))
                    continue;
                datasetType[k] = Plywood.getFullType(context[k]);
            }
            var typeContext = {
                type: 'DATASET',
                datasetType: datasetType
            };
            var alterations = {};
            this._fillRefSubstitutions(typeContext, { index: 0 }, alterations);
            if (!Object.keys(alterations).length)
                return this;
            return this.substitute(function (ex, index) { return alterations[index] || null; });
        };
        Expression.prototype.resolve = function (context, ifNotFound) {
            if (ifNotFound === void 0) { ifNotFound = 'throw'; }
            return this.substitute(function (ex, index, depth, nestDiff) {
                if (ex instanceof Plywood.RefExpression) {
                    var nest = ex.nest;
                    if (nestDiff === nest) {
                        var foundValue = null;
                        var valueFound = false;
                        if (hasOwnProperty(context, ex.name)) {
                            foundValue = context[ex.name];
                            valueFound = true;
                        }
                        else {
                            valueFound = false;
                        }
                        if (valueFound) {
                            return Plywood.External.isExternal(foundValue) ?
                                new Plywood.ExternalExpression({ external: foundValue }) :
                                new Plywood.LiteralExpression({ value: foundValue });
                        }
                        else if (ifNotFound === 'throw') {
                            throw new Error('could not resolve ' + ex.toString() + ' because is was not in the context');
                        }
                        else if (ifNotFound === 'null') {
                            return new Plywood.LiteralExpression({ value: null });
                        }
                        else if (ifNotFound === 'leave') {
                            return ex;
                        }
                    }
                    else if (nestDiff < nest) {
                        throw new Error('went too deep during resolve on: ' + ex.toString());
                    }
                }
                return null;
            });
        };
        Expression.prototype.resolved = function () {
            return this.every(function (ex) {
                return (ex instanceof Plywood.RefExpression) ? ex.nest === 0 : null;
            });
        };
        Expression.prototype.decomposeAverage = function (countEx) {
            return this.substituteAction(function (action) {
                return action.action === 'average';
            }, function (preEx, action) {
                var expression = action.expression;
                return preEx.sum(expression).divide(countEx ? preEx.sum(countEx) : preEx.count());
            });
        };
        Expression.prototype.distribute = function () {
            return this.substituteAction(function (action) {
                return action.canDistribute();
            }, function (preEx, action) {
                var distributed = action.distribute(preEx);
                if (!distributed)
                    throw new Error('distribute returned null');
                return distributed;
            });
        };
        Expression.prototype._computeResolvedSimulate = function (simulatedQueries) {
            throw new Error("can not call this directly");
        };
        Expression.prototype.simulateQueryPlan = function (context) {
            if (context === void 0) { context = {}; }
            if (!Plywood.datumHasExternal(context) && !this.hasExternal()) {
                return [];
            }
            var simulatedQueries = [];
            var readyExpression = this.referenceCheck(context).resolve(context).simplify();
            if (readyExpression instanceof Plywood.ExternalExpression) {
                readyExpression = readyExpression.unsuppress();
            }
            readyExpression._computeResolvedSimulate(simulatedQueries);
            return simulatedQueries;
        };
        Expression.prototype._computeResolved = function () {
            throw new Error("can not call this directly");
        };
        Expression.prototype.compute = function (context) {
            var _this = this;
            if (context === void 0) { context = {}; }
            if (!Plywood.datumHasExternal(context) && !this.hasExternal()) {
                return Q.fcall(function () {
                    var referenceChecked = _this.referenceCheck(context);
                    return referenceChecked.getFn()(context, null);
                });
            }
            var ex = this;
            return Plywood.introspectDatum(context).then(function (introspectedContext) {
                var readyExpression = ex.referenceCheck(introspectedContext).resolve(introspectedContext).simplify();
                if (readyExpression instanceof Plywood.ExternalExpression) {
                    readyExpression = readyExpression.unsuppress();
                }
                return readyExpression._computeResolved();
            });
        };
        Expression.classMap = {};
        return Expression;
    })();
    Plywood.Expression = Expression;
    check = Expression;
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var LiteralExpression = (function (_super) {
        __extends(LiteralExpression, _super);
        function LiteralExpression(parameters) {
            _super.call(this, parameters, dummyObject);
            var value = parameters.value;
            this.value = value;
            this._ensureOp("literal");
            if (typeof this.value === 'undefined') {
                throw new TypeError("must have a `value`");
            }
            this.type = Plywood.getValueType(value);
            this.simple = true;
        }
        LiteralExpression.fromJS = function (parameters) {
            var value = {
                op: parameters.op,
                type: parameters.type
            };
            var v = parameters.value;
            if (Plywood.isImmutableClass(v)) {
                value.value = v;
            }
            else {
                value.value = Plywood.valueFromJS(v, parameters.type);
            }
            return new LiteralExpression(value);
        };
        LiteralExpression.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.value = this.value;
            if (this.type)
                value.type = this.type;
            return value;
        };
        LiteralExpression.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            if (this.value && this.value.toJS) {
                js.value = this.value.toJS();
                js.type = (this.type.indexOf('SET/') === 0) ? 'SET' : this.type;
            }
            else {
                js.value = this.value;
            }
            return js;
        };
        LiteralExpression.prototype.toString = function () {
            var value = this.value;
            if (value instanceof Plywood.Dataset && value.basis()) {
                return 'ply()';
            }
            else if (this.type === 'STRING') {
                return JSON.stringify(value);
            }
            else {
                return String(value);
            }
        };
        LiteralExpression.prototype.getFn = function () {
            var value = this.value;
            return function () { return value; };
        };
        LiteralExpression.prototype.getJS = function (datumVar) {
            return JSON.stringify(this.value);
        };
        LiteralExpression.prototype.getSQL = function (dialect) {
            var value = this.value;
            switch (this.type) {
                case 'STRING':
                    return dialect.escapeLiteral(value);
                case 'BOOLEAN':
                    return dialect.booleanToSQL(value);
                case 'NUMBER':
                    return dialect.numberToSQL(value);
                case 'NUMBER_RANGE':
                    return dialect.numberToSQL(value.start) + "/" + dialect.numberToSQL(value.end);
                case 'TIME':
                    return dialect.timeToSQL(value);
                case 'TIME_RANGE':
                    return dialect.timeToSQL(value.start) + "/" + dialect.timeToSQL(value.end);
                case 'SET/STRING':
                    return '(' + value.elements.map(function (v) { return dialect.escapeLiteral(v); }).join(',') + ')';
                default:
                    throw new Error("currently unsupported type: " + this.type);
            }
        };
        LiteralExpression.prototype.equals = function (other) {
            if (!_super.prototype.equals.call(this, other) || this.type !== other.type)
                return false;
            if (this.value) {
                if (this.value.equals) {
                    return this.value.equals(other.value);
                }
                else if (this.value.toISOString && other.value.toISOString) {
                    return this.value.valueOf() === other.value.valueOf();
                }
                else {
                    return this.value === other.value;
                }
            }
            else {
                return this.value === other.value;
            }
        };
        LiteralExpression.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            indexer.index++;
            if (this.type == 'DATASET') {
                var newTypeContext = this.value.getFullType();
                newTypeContext.parent = typeContext;
                return newTypeContext;
            }
            else {
                return { type: this.type };
            }
        };
        LiteralExpression.prototype.getLiteralValue = function () {
            return this.value;
        };
        LiteralExpression.prototype._computeResolvedSimulate = function (simulatedQueries) {
            return this.value;
        };
        LiteralExpression.prototype._computeResolved = function () {
            return Q(this.value);
        };
        return LiteralExpression;
    })(Plywood.Expression);
    Plywood.LiteralExpression = LiteralExpression;
    Plywood.Expression.NULL = new LiteralExpression({ value: null });
    Plywood.Expression.ZERO = new LiteralExpression({ value: 0 });
    Plywood.Expression.ONE = new LiteralExpression({ value: 1 });
    Plywood.Expression.FALSE = new LiteralExpression({ value: false });
    Plywood.Expression.TRUE = new LiteralExpression({ value: true });
    Plywood.Expression.EMPTY_STRING = new LiteralExpression({ value: '' });
    Plywood.Expression.register(LiteralExpression);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    Plywood.POSSIBLE_TYPES = {
        'NULL': 1,
        'BOOLEAN': 1,
        'NUMBER': 1,
        'TIME': 1,
        'STRING': 1,
        'NUMBER_RANGE': 1,
        'TIME_RANGE': 1,
        'MARK': 1,
        'SET': 1,
        'SET/NULL': 1,
        'SET/BOOLEAN': 1,
        'SET/NUMBER': 1,
        'SET/TIME': 1,
        'SET/STRING': 1,
        'SET/NUMBER_RANGE': 1,
        'SET/TIME_RANGE': 1,
        'DATASET': 1
    };
    var GENERATIONS_REGEXP = /^\^+/;
    var TYPE_REGEXP = /:([A-Z\/_]+)$/;
    var RefExpression = (function (_super) {
        __extends(RefExpression, _super);
        function RefExpression(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureOp("ref");
            var name = parameters.name;
            if (typeof name !== 'string' || name.length === 0) {
                throw new TypeError("must have a nonempty `name`");
            }
            this.name = name;
            var nest = parameters.nest;
            if (typeof nest !== 'number') {
                throw new TypeError("must have nest");
            }
            if (nest < 0) {
                throw new Error("nest must be non-negative");
            }
            this.nest = nest;
            var myType = parameters.type;
            if (myType) {
                if (!hasOwnProperty(Plywood.POSSIBLE_TYPES, myType)) {
                    throw new TypeError("unsupported type '" + myType + "'");
                }
                this.type = myType;
            }
            if (parameters.remote)
                this.remote = parameters.remote;
            this.simple = true;
        }
        RefExpression.fromJS = function (parameters) {
            var value;
            if (hasOwnProperty(parameters, 'nest')) {
                value = parameters;
            }
            else {
                value = {
                    op: 'ref',
                    nest: 0,
                    name: parameters.name,
                    type: parameters.type
                };
            }
            return new RefExpression(value);
        };
        RefExpression.parse = function (str) {
            var refValue = { op: 'ref' };
            var match;
            match = str.match(GENERATIONS_REGEXP);
            if (match) {
                var nest = match[0].length;
                refValue.nest = nest;
                str = str.substr(nest);
            }
            else {
                refValue.nest = 0;
            }
            match = str.match(TYPE_REGEXP);
            if (match) {
                refValue.type = match[1];
                str = str.substr(0, str.length - match[0].length);
            }
            if (str[0] === '{' && str[str.length - 1] === '}') {
                str = str.substr(1, str.length - 2);
            }
            refValue.name = str;
            return new RefExpression(refValue);
        };
        RefExpression.toSimpleName = function (variableName) {
            if (!RefExpression.SIMPLE_NAME_REGEXP.test(variableName))
                throw new Error('fail');
            return variableName;
        };
        RefExpression.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.name = this.name;
            value.nest = this.nest;
            if (this.type)
                value.type = this.type;
            if (this.remote)
                value.remote = this.remote;
            return value;
        };
        RefExpression.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.name = this.name;
            if (this.nest)
                js.nest = this.nest;
            if (this.type)
                js.type = this.type;
            return js;
        };
        RefExpression.prototype.toString = function () {
            var str = this.name;
            if (!RefExpression.SIMPLE_NAME_REGEXP.test(str)) {
                str = '{' + str + '}';
            }
            if (this.nest) {
                str = repeat('^', this.nest) + str;
            }
            if (this.type) {
                str += ':' + this.type;
            }
            return '$' + str;
        };
        RefExpression.prototype.getFn = function () {
            var name = this.name;
            var nest = this.nest;
            return function (d, c) {
                if (nest) {
                    return c[name];
                }
                else {
                    if (hasOwnProperty(d, name)) {
                        return d[name];
                    }
                    else {
                        return null;
                    }
                }
            };
        };
        RefExpression.prototype.getJS = function (datumVar) {
            if (this.nest)
                throw new Error("can not call getJS on unresolved expression");
            var name = this.name;
            if (datumVar) {
                return datumVar.replace('[]', "[" + JSON.stringify(name) + "]");
            }
            else {
                return RefExpression.toSimpleName(name);
            }
        };
        RefExpression.prototype.getSQL = function (dialect, minimal) {
            if (minimal === void 0) { minimal = false; }
            if (this.nest)
                throw new Error("can not call getSQL on unresolved expression: " + this.toString());
            return dialect.escapeName(this.name);
        };
        RefExpression.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.name === other.name &&
                this.nest === other.nest;
        };
        RefExpression.prototype.isRemote = function () {
            return Boolean(this.remote && this.remote.length);
        };
        RefExpression.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            var myIndex = indexer.index;
            indexer.index++;
            var nest = this.nest;
            var myTypeContext = typeContext;
            while (nest--) {
                myTypeContext = myTypeContext.parent;
                if (!myTypeContext)
                    throw new Error('went too deep on ' + this.toString());
            }
            var nestDiff = 0;
            while (myTypeContext && !myTypeContext.datasetType[this.name]) {
                myTypeContext = myTypeContext.parent;
                nestDiff++;
            }
            if (!myTypeContext) {
                throw new Error('could not resolve ' + this.toString());
            }
            var myFullType = myTypeContext.datasetType[this.name];
            var myType = myFullType.type;
            var myRemote = myFullType.remote;
            if (this.type && this.type !== myType) {
                throw new TypeError("type mismatch in " + this.toString() + " (has: " + this.type + " needs: " + myType + ")");
            }
            if (!this.type || nestDiff > 0 || String(this.remote) !== String(myRemote)) {
                alterations[myIndex] = new RefExpression({
                    name: this.name,
                    nest: this.nest + nestDiff,
                    type: myType,
                    remote: myRemote
                });
            }
            if (myType === 'DATASET') {
                return {
                    parent: typeContext,
                    type: 'DATASET',
                    datasetType: myFullType.datasetType,
                    remote: myFullType.remote
                };
            }
            return myFullType;
        };
        RefExpression.prototype.incrementNesting = function (by) {
            if (by === void 0) { by = 1; }
            var value = this.valueOf();
            value.nest = by + value.nest;
            return new RefExpression(value);
        };
        RefExpression.SIMPLE_NAME_REGEXP = /^([a-z_]\w*)$/i;
        return RefExpression;
    })(Plywood.Expression);
    Plywood.RefExpression = RefExpression;
    Plywood.Expression.register(RefExpression);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var ExternalExpression = (function (_super) {
        __extends(ExternalExpression, _super);
        function ExternalExpression(parameters) {
            _super.call(this, parameters, dummyObject);
            this.external = parameters.external;
            if (!this.external)
                throw new Error('must have an external');
            this._ensureOp('external');
            this.type = 'DATASET';
            this.simple = true;
        }
        ExternalExpression.fromJS = function (parameters) {
            var value = {
                op: parameters.op
            };
            value.external = Plywood.External.fromJS(parameters.external);
            return new ExternalExpression(value);
        };
        ExternalExpression.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.external = this.external;
            return value;
        };
        ExternalExpression.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.external = this.external.toJS();
            return js;
        };
        ExternalExpression.prototype.toString = function () {
            return "E:" + this.external.toString();
        };
        ExternalExpression.prototype.getFn = function () {
            throw new Error('should not call getFn on External');
        };
        ExternalExpression.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.external.equals(other.external);
        };
        ExternalExpression.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            indexer.index++;
            var newTypeContext = this.external.getFullType();
            newTypeContext.parent = typeContext;
            return newTypeContext;
        };
        ExternalExpression.prototype._computeResolvedSimulate = function (simulatedQueries) {
            var external = this.external;
            if (external.suppress)
                return external;
            simulatedQueries.push(external.getQueryAndPostProcess().query);
            return external.simulate();
        };
        ExternalExpression.prototype._computeResolved = function () {
            var external = this.external;
            if (external.suppress)
                return Q(external);
            return external.queryValues();
        };
        ExternalExpression.prototype.unsuppress = function () {
            var value = this.valueOf();
            value.external = this.external.show();
            return new ExternalExpression(value);
        };
        ExternalExpression.prototype.addAction = function (action) {
            var newExternal = this.external.addAction(action);
            if (!newExternal)
                return;
            return new ExternalExpression({ external: newExternal });
        };
        ExternalExpression.prototype.makeTotal = function (dataName) {
            var newExternal = this.external.makeTotal(dataName);
            if (!newExternal)
                return null;
            return new ExternalExpression({ external: newExternal });
        };
        ExternalExpression.prototype.getEmptyLiteral = function () {
            var external = this.external;
            var emptyTotalDataset = external.getEmptyTotalDataset();
            if (!emptyTotalDataset)
                return null;
            return Plywood.r(emptyTotalDataset);
        };
        return ExternalExpression;
    })(Plywood.Expression);
    Plywood.ExternalExpression = ExternalExpression;
    Plywood.Expression.register(ExternalExpression);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var ChainExpression = (function (_super) {
        __extends(ChainExpression, _super);
        function ChainExpression(parameters) {
            _super.call(this, parameters, dummyObject);
            var expression = parameters.expression;
            this.expression = expression;
            var actions = parameters.actions;
            if (!actions.length)
                throw new Error('can not have empty actions');
            this.actions = actions;
            this._ensureOp('chain');
            var type = expression.type;
            for (var _i = 0; _i < actions.length; _i++) {
                var action = actions[_i];
                type = action.getOutputType(type);
            }
            this.type = type;
        }
        ChainExpression.fromJS = function (parameters) {
            var value = {
                op: parameters.op
            };
            value.expression = Plywood.Expression.fromJS(parameters.expression);
            if (hasOwnProperty(parameters, 'action')) {
                value.actions = [Plywood.Action.fromJS(parameters.action)];
            }
            else {
                if (!Array.isArray(parameters.actions))
                    throw new Error('chain `actions` must be an array');
                value.actions = parameters.actions.map(Plywood.Action.fromJS);
            }
            return new ChainExpression(value);
        };
        ChainExpression.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.expression = this.expression;
            value.actions = this.actions;
            return value;
        };
        ChainExpression.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.expression = this.expression.toJS();
            var actions = this.actions;
            if (actions.length === 1) {
                js.action = actions[0].toJS();
            }
            else {
                js.actions = actions.map(function (action) { return action.toJS(); });
            }
            return js;
        };
        ChainExpression.prototype.toString = function (indent) {
            var expression = this.expression;
            var actions = this.actions;
            var joinStr = '.';
            var nextIndent = null;
            if (indent != null && (actions.length > 1 || expression.type === 'DATASET')) {
                joinStr = '\n' + repeat(' ', indent) + joinStr;
                nextIndent = indent + 2;
            }
            return [expression.toString()]
                .concat(actions.map(function (action) { return action.toString(nextIndent); }))
                .join(joinStr);
        };
        ChainExpression.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.expression.equals(other.expression) &&
                higherArraysEqual(this.actions, other.actions);
        };
        ChainExpression.prototype.expressionCount = function () {
            var expressionCount = 1 + this.expression.expressionCount();
            var actions = this.actions;
            for (var _i = 0; _i < actions.length; _i++) {
                var action = actions[_i];
                expressionCount += action.expressionCount();
            }
            return expressionCount;
        };
        ChainExpression.prototype.getFn = function () {
            var fn = this.expression.getFn();
            var actions = this.actions;
            for (var _i = 0; _i < actions.length; _i++) {
                var action = actions[_i];
                fn = action.getFn(fn);
            }
            return fn;
        };
        ChainExpression.prototype.getJS = function (datumVar) {
            var expression = this.expression;
            var actions = this.actions;
            var js = expression.getJS(datumVar);
            for (var _i = 0; _i < actions.length; _i++) {
                var action = actions[_i];
                js = action.getJS(js, datumVar);
            }
            return js;
        };
        ChainExpression.prototype.getSQL = function (dialect) {
            var expression = this.expression;
            var actions = this.actions;
            var sql = expression.getSQL(dialect);
            for (var _i = 0; _i < actions.length; _i++) {
                var action = actions[_i];
                sql = action.getSQL(sql, dialect);
            }
            return sql;
        };
        ChainExpression.prototype.getSingleAction = function (neededAction) {
            var actions = this.actions;
            if (actions.length !== 1)
                return null;
            var singleAction = actions[0];
            if (neededAction && singleAction.action !== neededAction)
                return null;
            return singleAction;
        };
        ChainExpression.prototype.foldIntoExternal = function () {
            var externalExpression = this.expression;
            var actions = this.actions;
            if (externalExpression instanceof Plywood.ExternalExpression) {
                var undigestedActions = [];
                for (var _i = 0; _i < actions.length; _i++) {
                    var action = actions[_i];
                    var newExternal = externalExpression.addAction(action);
                    if (newExternal) {
                        externalExpression = newExternal;
                    }
                    else {
                        undigestedActions.push(action);
                    }
                }
                var emptyLiteral = externalExpression.getEmptyLiteral();
                if (emptyLiteral) {
                    externalExpression = emptyLiteral;
                }
                if (undigestedActions.length) {
                    return new ChainExpression({
                        expression: externalExpression,
                        actions: undigestedActions,
                        simple: true
                    });
                }
                else {
                    return externalExpression;
                }
            }
            return this;
        };
        ChainExpression.prototype.simplify = function () {
            if (this.simple)
                return this;
            var simpleExpression = this.expression.simplify();
            var actions = this.actions;
            if (simpleExpression instanceof ChainExpression) {
                return new ChainExpression({
                    expression: simpleExpression.expression,
                    actions: simpleExpression.actions.concat(actions)
                }).simplify();
            }
            for (var _i = 0; _i < actions.length; _i++) {
                var action = actions[_i];
                simpleExpression = action.performOnSimple(simpleExpression);
            }
            if (simpleExpression instanceof ChainExpression) {
                return simpleExpression.foldIntoExternal();
            }
            else {
                return simpleExpression;
            }
        };
        ChainExpression.prototype._everyHelper = function (iter, thisArg, indexer, depth, nestDiff) {
            var pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
            if (pass != null) {
                return pass;
            }
            else {
                indexer.index++;
            }
            depth++;
            var expression = this.expression;
            if (!expression._everyHelper(iter, thisArg, indexer, depth, nestDiff))
                return false;
            var actions = this.actions;
            var every = true;
            for (var _i = 0; _i < actions.length; _i++) {
                var action = actions[_i];
                if (every) {
                    every = action._everyHelper(iter, thisArg, indexer, depth, nestDiff);
                }
                else {
                    indexer.index += action.expressionCount();
                }
            }
            return every;
        };
        ChainExpression.prototype._substituteHelper = function (substitutionFn, thisArg, indexer, depth, nestDiff) {
            var sub = substitutionFn.call(thisArg, this, indexer.index, depth, nestDiff);
            if (sub) {
                indexer.index += this.expressionCount();
                return sub;
            }
            else {
                indexer.index++;
            }
            depth++;
            var expression = this.expression;
            var subExpression = expression._substituteHelper(substitutionFn, thisArg, indexer, depth, nestDiff);
            var actions = this.actions;
            var subActions = actions.map(function (action) {
                var subbedAction = action._substituteHelper(substitutionFn, thisArg, indexer, depth, nestDiff);
                return subbedAction;
            });
            if (expression === subExpression && arraysEqual(actions, subActions))
                return this;
            var value = this.valueOf();
            value.expression = subExpression;
            value.actions = subActions;
            delete value.simple;
            return new ChainExpression(value);
        };
        ChainExpression.prototype.performAction = function (action, markSimple) {
            if (!action)
                throw new Error('must have action');
            return new ChainExpression({
                expression: this.expression,
                actions: this.actions.concat(action),
                simple: Boolean(markSimple)
            });
        };
        ChainExpression.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            indexer.index++;
            var currentContext = typeContext;
            var outputContext = this.expression._fillRefSubstitutions(currentContext, indexer, alterations);
            currentContext = outputContext.type === 'DATASET' ? outputContext : typeContext;
            var actions = this.actions;
            for (var _i = 0; _i < actions.length; _i++) {
                var action = actions[_i];
                outputContext = action._fillRefSubstitutions(currentContext, indexer, alterations);
                currentContext = outputContext.type === 'DATASET' ? outputContext : typeContext;
            }
            return outputContext;
        };
        ChainExpression.prototype.actionize = function (containingAction) {
            var actions = this.actions;
            var k = actions.length - 1;
            for (; k >= 0; k--) {
                if (actions[k].action !== containingAction)
                    break;
            }
            k++;
            if (k === actions.length)
                return null;
            var newExpression;
            if (k === 0) {
                newExpression = this.expression;
            }
            else {
                var value = this.valueOf();
                value.actions = actions.slice(0, k);
                newExpression = new ChainExpression(value);
            }
            return [
                new Plywood.Action.classMap[containingAction]({
                    expression: newExpression
                })
            ].concat(actions.slice(k));
        };
        ChainExpression.prototype.firstAction = function () {
            return this.actions[0] || null;
        };
        ChainExpression.prototype.lastAction = function () {
            var actions = this.actions;
            return actions[actions.length - 1] || null;
        };
        ChainExpression.prototype.popAction = function () {
            if (arguments.length) {
                console.error('popAction no longer takes any arguments, check lastAction instead');
            }
            var actions = this.actions;
            if (!actions.length)
                return null;
            actions = actions.slice(0, -1);
            if (!actions.length)
                return this.expression;
            var value = this.valueOf();
            value.actions = actions;
            return new ChainExpression(value);
        };
        ChainExpression.prototype._computeResolvedSimulate = function (simulatedQueries) {
            var actions = this.actions;
            function execAction(i, dataset) {
                var action = actions[i];
                var actionExpression = action.expression;
                if (action instanceof Plywood.FilterAction) {
                    return dataset.filter(action.expression.getFn(), null);
                }
                else if (action instanceof Plywood.ApplyAction) {
                    if (actionExpression.hasExternal()) {
                        return dataset.apply(action.name, function (d) {
                            var simpleActionExpression = actionExpression.resolve(d);
                            simpleActionExpression = simpleActionExpression.simplify();
                            return simpleActionExpression._computeResolvedSimulate(simulatedQueries);
                        }, null);
                    }
                    else {
                        return dataset.apply(action.name, actionExpression.getFn(), null);
                    }
                }
                else if (action instanceof Plywood.SortAction) {
                    return dataset.sort(actionExpression.getFn(), action.direction, null);
                }
                else if (action instanceof Plywood.LimitAction) {
                    return dataset.limit(action.limit);
                }
            }
            var value = this.expression._computeResolvedSimulate(simulatedQueries);
            for (var i = 0; i < actions.length; i++) {
                value = execAction(i, value);
            }
            return value;
        };
        ChainExpression.prototype._computeResolved = function () {
            var actions = this.actions;
            function execAction(i) {
                return function (dataset) {
                    var action = actions[i];
                    var actionExpression = action.expression;
                    if (action instanceof Plywood.FilterAction) {
                        return dataset.filter(action.expression.getFn(), null);
                    }
                    else if (action instanceof Plywood.ApplyAction) {
                        if (actionExpression.hasExternal()) {
                            return dataset.applyPromise(action.name, function (d) {
                                return actionExpression.resolve(d).simplify()._computeResolved();
                            }, null);
                        }
                        else {
                            return dataset.apply(action.name, actionExpression.getFn(), null);
                        }
                    }
                    else if (action instanceof Plywood.SortAction) {
                        return dataset.sort(actionExpression.getFn(), action.direction, null);
                    }
                    else if (action instanceof Plywood.LimitAction) {
                        return dataset.limit(action.limit);
                    }
                };
            }
            var promise = this.expression._computeResolved();
            for (var i = 0; i < actions.length; i++) {
                promise = promise.then(execAction(i));
            }
            return promise;
        };
        ChainExpression.prototype.separateViaAnd = function (refName) {
            if (typeof refName !== 'string')
                throw new Error('must have refName');
            if (!this.simple)
                return this.simplify().separateViaAnd(refName);
            var andExpressions = this.getExpressionPattern('and');
            if (!andExpressions) {
                return _super.prototype.separateViaAnd.call(this, refName);
            }
            var includedExpressions = [];
            var excludedExpressions = [];
            for (var _i = 0; _i < andExpressions.length; _i++) {
                var operand = andExpressions[_i];
                var sep = operand.separateViaAnd(refName);
                if (sep === null)
                    return null;
                includedExpressions.push(sep.included);
                excludedExpressions.push(sep.excluded);
            }
            return {
                included: Plywood.Expression.and(includedExpressions).simplify(),
                excluded: Plywood.Expression.and(excludedExpressions).simplify()
            };
        };
        return ChainExpression;
    })(Plywood.Expression);
    Plywood.ChainExpression = ChainExpression;
    Plywood.Expression.register(ChainExpression);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var checkAction;
    var Action = (function () {
        function Action(parameters, dummy) {
            if (dummy === void 0) { dummy = null; }
            if (dummy !== dummyObject) {
                throw new TypeError("can not call `new Action` directly use Action.fromJS instead");
            }
            this.action = parameters.action;
            this.expression = parameters.expression;
            this.simple = parameters.simple;
        }
        Action.jsToValue = function (parameters) {
            var value = {
                action: parameters.action
            };
            if (parameters.expression) {
                value.expression = Plywood.Expression.fromJS(parameters.expression);
            }
            return value;
        };
        Action.actionsDependOn = function (actions, name) {
            for (var _i = 0; _i < actions.length; _i++) {
                var action = actions[_i];
                var freeReferences = action.getFreeReferences();
                if (freeReferences.indexOf(name) !== -1)
                    return true;
                if (action.name === name)
                    return false;
            }
            return false;
        };
        Action.isAction = function (candidate) {
            return Plywood.isInstanceOf(candidate, Action);
        };
        Action.register = function (act) {
            var action = act.name.replace('Action', '').replace(/^\w/, function (s) { return s.toLowerCase(); });
            Action.classMap[action] = act;
        };
        Action.fromJS = function (actionJS) {
            if (!hasOwnProperty(actionJS, "action")) {
                throw new Error("action must be defined");
            }
            var action = actionJS.action;
            if (typeof action !== "string") {
                throw new Error("action must be a string");
            }
            var ClassFn = Action.classMap[action];
            if (!ClassFn) {
                throw new Error("unsupported action '" + action + "'");
            }
            return ClassFn.fromJS(actionJS);
        };
        Action.prototype._ensureAction = function (action) {
            if (!this.action) {
                this.action = action;
                return;
            }
            if (this.action !== action) {
                throw new TypeError("incorrect action '" + this.action + "' (needs to be: '" + action + "')");
            }
        };
        Action.prototype._toStringParameters = function (expressionString) {
            return expressionString ? [expressionString] : [];
        };
        Action.prototype.toString = function (indent) {
            var expression = this.expression;
            var spacer = '';
            var joinStr = indent != null ? ', ' : ',';
            var nextIndent = null;
            if (indent != null && expression && expression.type === 'DATASET') {
                var space = repeat(' ', indent);
                spacer = '\n' + space;
                joinStr = ',\n' + space;
                nextIndent = indent + 2;
            }
            return [
                this.action,
                '(',
                spacer,
                this._toStringParameters(expression ? expression.toString(nextIndent) : null).join(joinStr),
                spacer,
                ')'
            ].join('');
        };
        Action.prototype.valueOf = function () {
            var value = {
                action: this.action
            };
            if (this.expression)
                value.expression = this.expression;
            if (this.simple)
                value.simple = true;
            return value;
        };
        Action.prototype.toJS = function () {
            var js = {
                action: this.action
            };
            if (this.expression) {
                js.expression = this.expression.toJS();
            }
            return js;
        };
        Action.prototype.toJSON = function () {
            return this.toJS();
        };
        Action.prototype.equals = function (other) {
            return Action.isAction(other) &&
                this.action === other.action &&
                Boolean(this.expression) === Boolean(other.expression) &&
                (!this.expression || this.expression.equals(other.expression));
        };
        Action.prototype.isAggregate = function () {
            return false;
        };
        Action.prototype._checkInputType = function (inputType, neededType) {
            if (inputType && inputType !== 'NULL' && neededType && inputType !== neededType) {
                throw new Error(this.action + " must have input of type " + neededType + " (is " + inputType + ")");
            }
        };
        Action.prototype._checkInputTypes = function (inputType) {
            var neededTypes = [];
            for (var _i = 1; _i < arguments.length; _i++) {
                neededTypes[_i - 1] = arguments[_i];
            }
            if (inputType && inputType !== 'NULL' && neededTypes.indexOf(inputType) === -1) {
                throw new Error(this.action + " must have input of type " + neededTypes.join(' or ') + " (is " + inputType + ")");
            }
        };
        Action.prototype._checkExpressionType = function (neededType) {
            var expressionType = this.expression.type;
            if (expressionType && expressionType !== 'NULL' && expressionType !== neededType) {
                throw new Error(this.action + " must have expression of type " + neededType + " (is " + expressionType + ")");
            }
        };
        Action.prototype._checkExpressionTypes = function () {
            var neededTypes = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                neededTypes[_i - 0] = arguments[_i];
            }
            var expressionType = this.expression.type;
            if (expressionType && expressionType !== 'NULL' && neededTypes.indexOf(expressionType) === -1) {
                throw new Error(this.action + " must have expression of type " + neededTypes.join(' or ') + " (is " + expressionType + ")");
            }
        };
        Action.prototype.getOutputType = function (inputType) {
            throw new Error('must implement type checker');
        };
        Action.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            var expression = this.expression;
            if (expression)
                expression._fillRefSubstitutions(typeContext, indexer, alterations);
            return typeContext;
        };
        Action.prototype._getFnHelper = function (inputFn, expressionFn) {
            var action = this.action;
            return function (d, c) {
                var inV = inputFn(d, c);
                return inV ? inV[action](expressionFn, Plywood.foldContext(d, c)) : null;
            };
        };
        Action.prototype.getFn = function (inputFn) {
            var expression = this.expression;
            var expressionFn = expression ? expression.getFn() : null;
            return this._getFnHelper(inputFn, expressionFn);
        };
        Action.prototype._getJSHelper = function (inputJS, expressionJS) {
            return inputJS + '.' + this.action + '(' + (expressionJS || '') + ')';
        };
        Action.prototype.getJS = function (inputJS, datumVar) {
            var expression = this.expression;
            var expressionJS = expression ? expression.getJS(datumVar) : null;
            return this._getJSHelper(inputJS, expressionJS);
        };
        Action.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            throw new Error('can not call this directly');
        };
        Action.prototype.getSQL = function (inputSQL, dialect) {
            var expression = this.expression;
            var expressionSQL = expression ? expression.getSQL(dialect) : null;
            return this._getSQLHelper(dialect, inputSQL, expressionSQL);
        };
        Action.prototype.expressionCount = function () {
            return this.expression ? this.expression.expressionCount() : 0;
        };
        Action.prototype.fullyDefined = function () {
            var expression = this.expression;
            return !expression || expression.isOp('literal');
        };
        Action.prototype._specialSimplify = function (simpleExpression) {
            return null;
        };
        Action.prototype.simplify = function () {
            if (this.simple)
                return this;
            var expression = this.expression;
            var simpleExpression = expression ? expression.simplify() : null;
            var special = this._specialSimplify(simpleExpression);
            if (special)
                return special;
            var value = this.valueOf();
            if (simpleExpression) {
                value.expression = simpleExpression;
            }
            value.simple = true;
            return new Action.classMap[this.action](value);
        };
        Action.prototype._removeAction = function () {
            return false;
        };
        Action.prototype._nukeExpression = function () {
            return null;
        };
        Action.prototype._distributeAction = function () {
            return null;
        };
        Action.prototype._performOnLiteral = function (literalExpression) {
            return null;
        };
        Action.prototype._performOnRef = function (refExpression) {
            return null;
        };
        Action.prototype._foldWithPrevAction = function (prevAction) {
            return null;
        };
        Action.prototype._putBeforeAction = function (lastAction) {
            return null;
        };
        Action.prototype._performOnChain = function (chainExpression) {
            return null;
        };
        Action.prototype.performOnSimple = function (simpleExpression) {
            if (!this.simple)
                return this.simplify().performOnSimple(simpleExpression);
            if (!simpleExpression.simple)
                throw new Error('must get a simple expression');
            if (this._removeAction())
                return simpleExpression;
            var nukedExpression = this._nukeExpression();
            if (nukedExpression)
                return nukedExpression;
            var distributedActions = this._distributeAction();
            if (distributedActions) {
                for (var _i = 0; _i < distributedActions.length; _i++) {
                    var distributedAction = distributedActions[_i];
                    simpleExpression = distributedAction.performOnSimple(simpleExpression);
                }
                return simpleExpression;
            }
            if (simpleExpression instanceof Plywood.LiteralExpression) {
                if (this.fullyDefined()) {
                    return new Plywood.LiteralExpression({
                        value: this.getFn(simpleExpression.getFn())(null, null)
                    });
                }
                var special = this._performOnLiteral(simpleExpression);
                if (special)
                    return special;
            }
            else if (simpleExpression instanceof Plywood.RefExpression) {
                var special = this._performOnRef(simpleExpression);
                if (special)
                    return special;
            }
            else if (simpleExpression instanceof Plywood.ChainExpression) {
                var actions = simpleExpression.actions;
                var lastAction = actions[actions.length - 1];
                var foldedAction = this._foldWithPrevAction(lastAction);
                if (foldedAction) {
                    return foldedAction.performOnSimple(simpleExpression.popAction());
                }
                var beforeAction = this._putBeforeAction(lastAction);
                if (beforeAction) {
                    return lastAction.performOnSimple(beforeAction.performOnSimple(simpleExpression.popAction()));
                }
                var special = this._performOnChain(simpleExpression);
                if (special)
                    return special;
            }
            return simpleExpression.performAction(this, true);
        };
        Action.prototype.getExpressions = function () {
            return this.expression ? [this.expression] : [];
        };
        Action.prototype.getFreeReferences = function () {
            var freeReferences = [];
            this.getExpressions().forEach(function (ex) {
                freeReferences = freeReferences.concat(ex.getFreeReferences());
            });
            return deduplicateSort(freeReferences);
        };
        Action.prototype._everyHelper = function (iter, thisArg, indexer, depth, nestDiff) {
            var nestDiffNext = nestDiff + Number(this.isNester());
            return this.getExpressions().every(function (ex) { return ex._everyHelper(iter, thisArg, indexer, depth, nestDiffNext); });
        };
        Action.prototype.substitute = function (substitutionFn, thisArg) {
            return this._substituteHelper(substitutionFn, thisArg, { index: 0 }, 0, 0);
        };
        Action.prototype._substituteHelper = function (substitutionFn, thisArg, indexer, depth, nestDiff) {
            var expression = this.expression;
            if (!expression)
                return this;
            var subExpression = expression._substituteHelper(substitutionFn, thisArg, indexer, depth, nestDiff + Number(this.isNester()));
            if (expression === subExpression)
                return this;
            var value = this.valueOf();
            value.expression = subExpression;
            return new (Action.classMap[this.action])(value);
        };
        Action.prototype.canDistribute = function () {
            return false;
        };
        Action.prototype.distribute = function (preEx) {
            return null;
        };
        Action.prototype.applyToExpression = function (transformation) {
            var expression = this.expression;
            if (!expression)
                return this;
            var newExpression = transformation(expression);
            if (newExpression === expression)
                return this;
            var value = this.valueOf();
            value.expression = newExpression;
            return new (Action.classMap[this.action])(value);
        };
        Action.prototype.isNester = function () {
            return false;
        };
        Action.prototype.getLiteralValue = function () {
            var expression = this.expression;
            if (expression instanceof Plywood.LiteralExpression) {
                return expression.value;
            }
            return null;
        };
        Action.classMap = {};
        return Action;
    })();
    Plywood.Action = Action;
    checkAction = Action;
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var AddAction = (function (_super) {
        __extends(AddAction, _super);
        function AddAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("add");
            this._checkExpressionType('NUMBER');
        }
        AddAction.fromJS = function (parameters) {
            return new AddAction(Plywood.Action.jsToValue(parameters));
        };
        AddAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'NUMBER');
            return 'NUMBER';
        };
        AddAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
            return {
                type: 'NUMBER',
                remote: typeContext.remote
            };
        };
        AddAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                return (inputFn(d, c) || 0) + (expressionFn(d, c) || 0);
            };
        };
        AddAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return '(' + inputJS + '+' + expressionJS + ')';
        };
        AddAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return '(' + inputSQL + '+' + expressionSQL + ')';
        };
        AddAction.prototype._removeAction = function () {
            return this.expression.equals(Plywood.Expression.ZERO);
        };
        AddAction.prototype._distributeAction = function () {
            return this.expression.actionize(this.action);
        };
        AddAction.prototype._performOnLiteral = function (literalExpression) {
            if (literalExpression.equals(Plywood.Expression.ZERO)) {
                return this.expression;
            }
        };
        AddAction.prototype._foldWithPrevAction = function (prevAction) {
            if (prevAction instanceof AddAction) {
                var prevValue = prevAction.expression.getLiteralValue();
                var myValue = this.expression.getLiteralValue();
                if (typeof prevValue === 'number' && typeof myValue === 'number') {
                    return new AddAction({
                        expression: Plywood.r(prevValue + myValue)
                    });
                }
            }
            return null;
        };
        return AddAction;
    })(Plywood.Action);
    Plywood.AddAction = AddAction;
    Plywood.Action.register(AddAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    function mergeAnd(ex1, ex2) {
        if (!ex1.isOp('chain') ||
            !ex2.isOp('chain') ||
            !ex1.expression.isOp('ref') ||
            !ex2.expression.isOp('ref') ||
            !arraysEqual(ex1.getFreeReferences(), ex2.getFreeReferences()))
            return null;
        var ex1Actions = ex1.actions;
        var ex2Actions = ex2.actions;
        if (ex1Actions.length !== 1 || ex2Actions.length !== 1)
            return null;
        var firstActionExpression1 = ex1Actions[0].expression;
        var firstActionExpression2 = ex2Actions[0].expression;
        if (!firstActionExpression1.isOp('literal') || !firstActionExpression2.isOp('literal'))
            return null;
        var intersect = Plywood.Set.generalIntersect(firstActionExpression1.getLiteralValue(), firstActionExpression2.getLiteralValue());
        if (intersect === null)
            return null;
        return Plywood.Expression.inOrIs(ex1.expression, intersect);
    }
    var AndAction = (function (_super) {
        __extends(AndAction, _super);
        function AndAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("and");
        }
        AndAction.fromJS = function (parameters) {
            return new AndAction(Plywood.Action.jsToValue(parameters));
        };
        AndAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'BOOLEAN');
            return 'BOOLEAN';
        };
        AndAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) { return inputFn(d, c) && expressionFn(d, c); };
        };
        AndAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return '(' + inputJS + '&&' + expressionJS + ')';
        };
        AndAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return '(' + inputSQL + ' AND ' + expressionSQL + ')';
        };
        AndAction.prototype._removeAction = function () {
            return this.expression.equals(Plywood.Expression.TRUE);
        };
        AndAction.prototype._nukeExpression = function () {
            if (this.expression.equals(Plywood.Expression.FALSE))
                return Plywood.Expression.FALSE;
            return null;
        };
        AndAction.prototype._distributeAction = function () {
            return this.expression.actionize(this.action);
        };
        AndAction.prototype._performOnLiteral = function (literalExpression) {
            if (literalExpression.equals(Plywood.Expression.TRUE)) {
                return this.expression;
            }
            if (literalExpression.equals(Plywood.Expression.FALSE)) {
                return Plywood.Expression.FALSE;
            }
            return null;
        };
        AndAction.prototype._performOnChain = function (chainExpression) {
            var expression = this.expression;
            var andExpressions = chainExpression.getExpressionPattern('and');
            if (andExpressions) {
                for (var i = 0; i < andExpressions.length; i++) {
                    var andExpression = andExpressions[i];
                    var mergedExpression = mergeAnd(andExpression, expression);
                    if (mergedExpression) {
                        andExpressions[i] = mergedExpression;
                        return Plywood.Expression.and(andExpressions).simplify();
                    }
                }
            }
            else {
                return mergeAnd(chainExpression, expression);
            }
        };
        return AndAction;
    })(Plywood.Action);
    Plywood.AndAction = AndAction;
    Plywood.Action.register(AndAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var ApplyAction = (function (_super) {
        __extends(ApplyAction, _super);
        function ApplyAction(parameters) {
            if (parameters === void 0) { parameters = {}; }
            _super.call(this, parameters, dummyObject);
            this.name = parameters.name;
            this._ensureAction("apply");
        }
        ApplyAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.name = parameters.name;
            return new ApplyAction(value);
        };
        ApplyAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.name = this.name;
            return value;
        };
        ApplyAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.name = this.name;
            return js;
        };
        ApplyAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'DATASET';
        };
        ApplyAction.prototype._toStringParameters = function (expressionString) {
            return [this.name, expressionString];
        };
        ApplyAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.name === other.name;
        };
        ApplyAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            typeContext.datasetType[this.name] = this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
            return typeContext;
        };
        ApplyAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            var name = this.name;
            return function (d, c) {
                var inV = inputFn(d, c);
                return inV ? inV.apply(name, expressionFn, Plywood.foldContext(d, c)) : null;
            };
        };
        ApplyAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return expressionSQL + " AS " + dialect.escapeLiteral(this.name);
        };
        ApplyAction.prototype.isSimpleAggregate = function () {
            var expression = this.expression;
            if (expression instanceof Plywood.ChainExpression) {
                var actions = expression.actions;
                return actions.length === 1 && actions[0].isAggregate();
            }
            return false;
        };
        ApplyAction.prototype.isNester = function () {
            return true;
        };
        ApplyAction.prototype._putBeforeAction = function (lastAction) {
            if (this.isSimpleAggregate() && lastAction instanceof ApplyAction && !lastAction.isSimpleAggregate()) {
                return this;
            }
            return null;
        };
        ApplyAction.prototype._performOnLiteral = function (literalExpression) {
            var dataset = literalExpression.value;
            var myExpression = this.expression;
            if (dataset.basis()) {
                if (myExpression instanceof Plywood.ExternalExpression) {
                    var newTotalExpression = myExpression.makeTotal(this.name);
                    if (newTotalExpression)
                        return newTotalExpression;
                }
                else {
                    var externals = myExpression.getExternals();
                    if (externals.length === 1) {
                        var newExternal = externals[0].makeTotal('main');
                        if (!newExternal)
                            return null;
                        return this.performOnSimple(new Plywood.ExternalExpression({
                            external: newExternal
                        }));
                    }
                    else if (externals.length > 1) {
                        throw new Error('not done yet');
                    }
                }
            }
            return null;
        };
        return ApplyAction;
    })(Plywood.Action);
    Plywood.ApplyAction = ApplyAction;
    Plywood.Action.register(ApplyAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var AttachAction = (function (_super) {
        __extends(AttachAction, _super);
        function AttachAction(parameters) {
            if (parameters === void 0) { parameters = {}; }
            _super.call(this, parameters, dummyObject);
            this.selector = parameters.selector;
            this.prop = parameters.prop;
            this._ensureAction("attach");
        }
        AttachAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.selector = parameters.selector;
            value.prop = parameters.prop;
            return new AttachAction(value);
        };
        AttachAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.selector = this.selector;
            value.prop = this.prop;
            return value;
        };
        AttachAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.selector = this.selector;
            js.prop = this.prop;
            return js;
        };
        AttachAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'MARK');
            return 'MARK';
        };
        AttachAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            return {
                type: 'MARK',
                remote: typeContext.remote
            };
        };
        AttachAction.prototype._toStringParameters = function (expressionString) {
            return [this.selector, '{}'];
        };
        AttachAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.selector === other.selector;
        };
        AttachAction.prototype.getFn = function (inputFn) {
            var selector = this.selector;
            var prop = this.prop;
            return function (d, c) {
                var inV = inputFn(d, c);
                return inV ? inV.attach(selector, prop) : null;
            };
        };
        return AttachAction;
    })(Plywood.Action);
    Plywood.AttachAction = AttachAction;
    Plywood.Action.register(AttachAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var AverageAction = (function (_super) {
        __extends(AverageAction, _super);
        function AverageAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("average");
        }
        AverageAction.fromJS = function (parameters) {
            return new AverageAction(Plywood.Action.jsToValue(parameters));
        };
        AverageAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'NUMBER';
        };
        AverageAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
            return {
                type: 'NUMBER',
                remote: typeContext.remote
            };
        };
        AverageAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return 'AVG(' + expressionSQL + ')';
        };
        AverageAction.prototype.isAggregate = function () {
            return true;
        };
        AverageAction.prototype.isNester = function () {
            return true;
        };
        return AverageAction;
    })(Plywood.Action);
    Plywood.AverageAction = AverageAction;
    Plywood.Action.register(AverageAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var ConcatAction = (function (_super) {
        __extends(ConcatAction, _super);
        function ConcatAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("concat");
            this._checkExpressionType('STRING');
        }
        ConcatAction.fromJS = function (parameters) {
            return new ConcatAction(Plywood.Action.jsToValue(parameters));
        };
        ConcatAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'STRING');
            return 'STRING';
        };
        ConcatAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                return (inputFn(d, c) || '') + (expressionFn(d, c) || '');
            };
        };
        ConcatAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return '(' + inputJS + '+' + expressionJS + ')';
        };
        ConcatAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return 'CONCAT(' + inputSQL + ',' + expressionSQL + ')';
        };
        ConcatAction.prototype._removeAction = function () {
            return this.expression.equals(Plywood.Expression.EMPTY_STRING);
        };
        ConcatAction.prototype._performOnLiteral = function (literalExpression) {
            if (literalExpression.equals(Plywood.Expression.EMPTY_STRING)) {
                return this.expression;
            }
        };
        ConcatAction.prototype._foldWithPrevAction = function (prevAction) {
            if (prevAction instanceof ConcatAction) {
                var prevValue = prevAction.expression.getLiteralValue();
                var myValue = this.expression.getLiteralValue();
                if (typeof prevValue === 'string' && typeof myValue === 'string') {
                    return new ConcatAction({
                        expression: Plywood.r(prevValue + myValue)
                    });
                }
            }
            return null;
        };
        return ConcatAction;
    })(Plywood.Action);
    Plywood.ConcatAction = ConcatAction;
    Plywood.Action.register(ConcatAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var ContainsAction = (function (_super) {
        __extends(ContainsAction, _super);
        function ContainsAction(parameters) {
            _super.call(this, parameters, dummyObject);
            var compare = parameters.compare;
            if (!compare) {
                compare = ContainsAction.NORMAL;
            }
            else if (compare !== ContainsAction.NORMAL && compare !== ContainsAction.IGNORE_CASE) {
                throw new Error("compare must be '" + ContainsAction.NORMAL + "' or '" + ContainsAction.IGNORE_CASE + "'");
            }
            this.compare = compare;
            this._ensureAction("contains");
            this._checkExpressionType('STRING');
        }
        ContainsAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.compare = parameters.compare;
            return new ContainsAction(value);
        };
        ContainsAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.compare = this.compare;
            return value;
        };
        ContainsAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.compare = this.compare;
            return js;
        };
        ContainsAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.compare === other.compare;
        };
        ContainsAction.prototype.getOutputType = function (inputType) {
            this._checkInputTypes(inputType, 'BOOLEAN', 'STRING');
            return 'BOOLEAN';
        };
        ContainsAction.prototype._toStringParameters = function (expressionString) {
            return [expressionString, this.compare];
        };
        ContainsAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            if (this.compare === ContainsAction.NORMAL) {
                return function (d, c) {
                    return String(inputFn(d, c)).indexOf(expressionFn(d, c)) > -1;
                };
            }
            else {
                return function (d, c) {
                    return String(inputFn(d, c)).toLowerCase().indexOf(String(expressionFn(d, c)).toLowerCase()) > -1;
                };
            }
        };
        ContainsAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            if (this.compare === ContainsAction.NORMAL) {
                return "(''+" + inputJS + ").indexOf(" + expressionJS + ")>-1";
            }
            else {
                return "(''+" + inputJS + ").toLowerCase().indexOf(String(" + expressionJS + ").toLowerCase())>-1";
            }
        };
        ContainsAction.prototype.getSQL = function (inputSQL, dialect) {
            var expression = this.expression;
            if (expression instanceof Plywood.LiteralExpression) {
                return inputSQL + " LIKE \"%" + expression.value + "%\"";
            }
            else {
                throw new Error("can not express " + this.toString() + " in SQL");
            }
        };
        ContainsAction.NORMAL = 'normal';
        ContainsAction.IGNORE_CASE = 'ignoreCase';
        return ContainsAction;
    })(Plywood.Action);
    Plywood.ContainsAction = ContainsAction;
    Plywood.Action.register(ContainsAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var CountAction = (function (_super) {
        __extends(CountAction, _super);
        function CountAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("count");
        }
        CountAction.fromJS = function (parameters) {
            return new CountAction(Plywood.Action.jsToValue(parameters));
        };
        CountAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'NUMBER';
        };
        CountAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            return {
                type: 'NUMBER',
                remote: typeContext.remote
            };
        };
        CountAction.prototype.getFn = function (inputFn) {
            return function (d, c) {
                var inV = inputFn(d, c);
                return inV ? inV.count() : 0;
            };
        };
        CountAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return 'COUNT(*)';
        };
        CountAction.prototype.isAggregate = function () {
            return true;
        };
        return CountAction;
    })(Plywood.Action);
    Plywood.CountAction = CountAction;
    Plywood.Action.register(CountAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var CountDistinctAction = (function (_super) {
        __extends(CountDistinctAction, _super);
        function CountDistinctAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("countDistinct");
        }
        CountDistinctAction.fromJS = function (parameters) {
            return new CountDistinctAction(Plywood.Action.jsToValue(parameters));
        };
        CountDistinctAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'NUMBER';
        };
        CountDistinctAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
            return {
                type: 'NUMBER',
                remote: typeContext.remote
            };
        };
        CountDistinctAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return 'COUNT(DISTINCT ' + expressionSQL + ')';
        };
        CountDistinctAction.prototype.isAggregate = function () {
            return true;
        };
        CountDistinctAction.prototype.isNester = function () {
            return true;
        };
        return CountDistinctAction;
    })(Plywood.Action);
    Plywood.CountDistinctAction = CountDistinctAction;
    Plywood.Action.register(CountDistinctAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var CustomAction = (function (_super) {
        __extends(CustomAction, _super);
        function CustomAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this.custom = parameters.custom;
            this._ensureAction("custom");
        }
        CustomAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.custom = parameters.custom;
            return new CustomAction(value);
        };
        CustomAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.custom = this.custom;
            return value;
        };
        CustomAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.custom = this.custom;
            return js;
        };
        CustomAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'NUMBER';
        };
        CustomAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            return {
                type: 'NUMBER',
                remote: typeContext.remote
            };
        };
        CustomAction.prototype.getFn = function (inputFn) {
            throw new Error('can not getFn on custom action');
        };
        CustomAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            throw new Error('custom action not implemented');
        };
        CustomAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.custom === other.custom;
        };
        CustomAction.prototype.isAggregate = function () {
            return true;
        };
        return CustomAction;
    })(Plywood.Action);
    Plywood.CustomAction = CustomAction;
    Plywood.Action.register(CustomAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var DivideAction = (function (_super) {
        __extends(DivideAction, _super);
        function DivideAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("divide");
            this._checkExpressionType('NUMBER');
        }
        DivideAction.fromJS = function (parameters) {
            return new DivideAction(Plywood.Action.jsToValue(parameters));
        };
        DivideAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'NUMBER');
            return 'NUMBER';
        };
        DivideAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
            return {
                type: 'NUMBER',
                remote: typeContext.remote
            };
        };
        DivideAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                var v = (inputFn(d, c) || 0) / (expressionFn(d, c) || 0);
                return isNaN(v) ? null : v;
            };
        };
        DivideAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return '(' + inputJS + '/' + expressionJS + ')';
        };
        DivideAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return '(' + inputSQL + '/' + expressionSQL + ')';
        };
        DivideAction.prototype._removeAction = function () {
            return this.expression.equals(Plywood.Expression.ONE);
        };
        return DivideAction;
    })(Plywood.Action);
    Plywood.DivideAction = DivideAction;
    Plywood.Action.register(DivideAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var ExtractAction = (function (_super) {
        __extends(ExtractAction, _super);
        function ExtractAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this.regexp = parameters.regexp;
            this._ensureAction("extract");
        }
        ExtractAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.regexp = parameters.regexp;
            return new ExtractAction(value);
        };
        ExtractAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'STRING');
            return 'STRING';
        };
        ExtractAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.regexp = this.regexp;
            return value;
        };
        ExtractAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.regexp = this.regexp;
            return js;
        };
        ExtractAction.prototype._toStringParameters = function (expressionString) {
            return [this.regexp];
        };
        ExtractAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.regexp === other.regexp;
        };
        ExtractAction.prototype._getFnHelper = function (inputFn) {
            var re = new RegExp(this.regexp);
            return function (d, c) {
                return (String(inputFn(d, c)).match(re) || [])[1] || null;
            };
        };
        ExtractAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return "((''+" + inputJS + ").match(/" + this.regexp + "/) || [])[1] || null";
        };
        ExtractAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            throw new Error('not implemented yet');
        };
        return ExtractAction;
    })(Plywood.Action);
    Plywood.ExtractAction = ExtractAction;
    Plywood.Action.register(ExtractAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var FilterAction = (function (_super) {
        __extends(FilterAction, _super);
        function FilterAction(parameters) {
            if (parameters === void 0) { parameters = {}; }
            _super.call(this, parameters, dummyObject);
            this._ensureAction("filter");
            this._checkExpressionType('BOOLEAN');
        }
        FilterAction.fromJS = function (parameters) {
            return new FilterAction({
                action: parameters.action,
                name: parameters.name,
                expression: Plywood.Expression.fromJS(parameters.expression)
            });
        };
        FilterAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'DATASET';
        };
        FilterAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return inputSQL + " WHERE " + expressionSQL;
        };
        FilterAction.prototype.isNester = function () {
            return true;
        };
        FilterAction.prototype._foldWithPrevAction = function (prevAction) {
            if (prevAction instanceof FilterAction) {
                return new FilterAction({
                    expression: prevAction.expression.and(this.expression)
                });
            }
            return null;
        };
        FilterAction.prototype._putBeforeAction = function (lastAction) {
            if (lastAction instanceof Plywood.ApplyAction) {
                var freeReferences = this.getFreeReferences();
                return freeReferences.indexOf(lastAction.name) === -1 ? this : null;
            }
            if (lastAction instanceof Plywood.SplitAction) {
                var splits = lastAction.splits;
                return new FilterAction({
                    expression: this.expression.substitute(function (ex) {
                        if (ex instanceof Plywood.RefExpression && splits[ex.name])
                            return splits[ex.name];
                    })
                });
            }
            if (lastAction instanceof Plywood.SortAction) {
                return this;
            }
            return null;
        };
        return FilterAction;
    })(Plywood.Action);
    Plywood.FilterAction = FilterAction;
    Plywood.Action.register(FilterAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var GreaterThanAction = (function (_super) {
        __extends(GreaterThanAction, _super);
        function GreaterThanAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("greaterThan");
            this._checkExpressionTypes('NUMBER', 'TIME');
        }
        GreaterThanAction.fromJS = function (parameters) {
            return new GreaterThanAction(Plywood.Action.jsToValue(parameters));
        };
        GreaterThanAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, this.expression.type);
            return 'BOOLEAN';
        };
        GreaterThanAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                return inputFn(d, c) > expressionFn(d, c);
            };
        };
        GreaterThanAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return "(" + inputJS + ">" + expressionJS + ")";
        };
        GreaterThanAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return "(" + inputSQL + ">" + expressionSQL + ")";
        };
        GreaterThanAction.prototype._specialSimplify = function (simpleExpression) {
            var expression = this.expression;
            if (expression instanceof Plywood.LiteralExpression) {
                return new Plywood.InAction({
                    expression: new Plywood.LiteralExpression({
                        value: Plywood.Range.fromJS({ start: expression.value, end: null, bounds: '()' })
                    })
                });
            }
            return null;
        };
        GreaterThanAction.prototype._performOnLiteral = function (literalExpression) {
            return (new Plywood.InAction({
                expression: new Plywood.LiteralExpression({
                    value: Plywood.Range.fromJS({ start: null, end: literalExpression.value, bounds: '()' })
                })
            })).performOnSimple(this.expression);
        };
        return GreaterThanAction;
    })(Plywood.Action);
    Plywood.GreaterThanAction = GreaterThanAction;
    Plywood.Action.register(GreaterThanAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var GreaterThanOrEqualAction = (function (_super) {
        __extends(GreaterThanOrEqualAction, _super);
        function GreaterThanOrEqualAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("greaterThanOrEqual");
            this._checkExpressionTypes('NUMBER', 'TIME');
        }
        GreaterThanOrEqualAction.fromJS = function (parameters) {
            return new GreaterThanOrEqualAction(Plywood.Action.jsToValue(parameters));
        };
        GreaterThanOrEqualAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, this.expression.type);
            return 'BOOLEAN';
        };
        GreaterThanOrEqualAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                return inputFn(d, c) >= expressionFn(d, c);
            };
        };
        GreaterThanOrEqualAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return "(" + inputJS + ">=" + expressionJS + ")";
        };
        GreaterThanOrEqualAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return "(" + inputSQL + ">=" + expressionSQL + ")";
        };
        GreaterThanOrEqualAction.prototype._specialSimplify = function (simpleExpression) {
            var expression = this.expression;
            if (expression instanceof Plywood.LiteralExpression) {
                return new Plywood.InAction({
                    expression: new Plywood.LiteralExpression({
                        value: Plywood.Range.fromJS({ start: expression.value, end: null, bounds: '[)' })
                    })
                });
            }
            return null;
        };
        GreaterThanOrEqualAction.prototype._performOnLiteral = function (literalExpression) {
            return (new Plywood.InAction({
                expression: new Plywood.LiteralExpression({
                    value: Plywood.Range.fromJS({ start: null, end: literalExpression.value, bounds: '(]' })
                })
            })).performOnSimple(this.expression);
        };
        return GreaterThanOrEqualAction;
    })(Plywood.Action);
    Plywood.GreaterThanOrEqualAction = GreaterThanOrEqualAction;
    Plywood.Action.register(GreaterThanOrEqualAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var InAction = (function (_super) {
        __extends(InAction, _super);
        function InAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("in");
        }
        InAction.fromJS = function (parameters) {
            return new InAction(Plywood.Action.jsToValue(parameters));
        };
        InAction.prototype.getOutputType = function (inputType) {
            var expression = this.expression;
            if (inputType) {
                if (!(expression.canHaveType('SET')
                    || (inputType === 'NUMBER' && expression.canHaveType('NUMBER_RANGE'))
                    || (inputType === 'TIME' && expression.canHaveType('TIME_RANGE')))) {
                    throw new TypeError("in action has a bad type combination " + inputType + " in " + expression.type);
                }
            }
            else {
                if (!(expression.canHaveType('NUMBER_RANGE') || expression.canHaveType('TIME_RANGE') || expression.canHaveType('SET'))) {
                    throw new TypeError("in action has invalid expression type " + expression.type);
                }
            }
            return 'BOOLEAN';
        };
        InAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                var inV = inputFn(d, c);
                var exV = expressionFn(d, c);
                if (!exV)
                    return null;
                return exV.contains(inV);
            };
        };
        InAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            var expression = this.expression;
            var expressionType = expression.type;
            switch (expressionType) {
                case 'NUMBER_RANGE':
                    if (expression instanceof Plywood.LiteralExpression) {
                        var numberRange = expression.value;
                        return dialect.inExpression(inputSQL, dialect.numberToSQL(numberRange.start), dialect.numberToSQL(numberRange.end), numberRange.bounds);
                    }
                    throw new Error('not implemented yet');
                case 'TIME_RANGE':
                    if (expression instanceof Plywood.LiteralExpression) {
                        var timeRange = expression.value;
                        return dialect.inExpression(inputSQL, dialect.timeToSQL(timeRange.start), dialect.timeToSQL(timeRange.end), timeRange.bounds);
                    }
                    throw new Error('not implemented yet');
                case 'SET/STRING':
                    return inputSQL + " IN " + expressionSQL;
                default:
                    throw new Error('not implemented yet');
            }
        };
        InAction.prototype._nukeExpression = function () {
            var expression = this.expression;
            if (expression instanceof Plywood.LiteralExpression &&
                expression.type.indexOf('SET/') === 0 &&
                expression.value.empty())
                return Plywood.Expression.FALSE;
            return null;
        };
        return InAction;
    })(Plywood.Action);
    Plywood.InAction = InAction;
    Plywood.Action.register(InAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var IsAction = (function (_super) {
        __extends(IsAction, _super);
        function IsAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("is");
        }
        IsAction.fromJS = function (parameters) {
            return new IsAction(Plywood.Action.jsToValue(parameters));
        };
        IsAction.prototype.getOutputType = function (inputType) {
            var expressionType = this.expression.type;
            if (expressionType !== 'NULL')
                this._checkInputType(inputType, expressionType);
            return 'BOOLEAN';
        };
        IsAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                return inputFn(d, c) === expressionFn(d, c);
            };
        };
        IsAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return "(" + inputJS + "===" + expressionJS + ")";
        };
        IsAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return "(" + inputSQL + "=" + expressionSQL + ")";
        };
        IsAction.prototype._performOnLiteral = function (literalExpression) {
            var expression = this.expression;
            if (!expression.isOp('literal')) {
                return expression.is(literalExpression);
            }
            return null;
        };
        IsAction.prototype._performOnRef = function (refExpression) {
            if (this.expression.equals(refExpression)) {
                return Plywood.Expression.TRUE;
            }
            return null;
        };
        IsAction.prototype._performOnChain = function (chainExpression) {
            if (this.expression.equals(chainExpression)) {
                return Plywood.Expression.TRUE;
            }
            var actions = chainExpression.actions;
            var lastAction = actions[actions.length - 1];
            var literalValue = this.getLiteralValue();
            if (lastAction instanceof Plywood.TimeBucketAction && literalValue instanceof Plywood.TimeRange) {
                var duration = lastAction.duration;
                var timezone = lastAction.timezone;
                var start = literalValue.start;
                var end = literalValue.end;
                if (duration.isFloorable()) {
                    if (duration.floor(start, timezone).valueOf() === start.valueOf() &&
                        duration.move(start, timezone, 1).valueOf() === end.valueOf()) {
                        actions = actions.slice(0, -1);
                        actions.push(new Plywood.InAction({
                            expression: this.expression
                        }));
                        var chainExpressionValue = chainExpression.valueOf();
                        chainExpressionValue.actions = actions;
                        return new Plywood.ChainExpression(chainExpressionValue);
                    }
                    else {
                        return Plywood.Expression.FALSE;
                    }
                }
            }
            return null;
        };
        return IsAction;
    })(Plywood.Action);
    Plywood.IsAction = IsAction;
    Plywood.Action.register(IsAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var JoinAction = (function (_super) {
        __extends(JoinAction, _super);
        function JoinAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("join");
            if (!this.expression.canHaveType('DATASET'))
                throw new TypeError('expression must be a DATASET');
        }
        JoinAction.fromJS = function (parameters) {
            return new JoinAction(Plywood.Action.jsToValue(parameters));
        };
        JoinAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'DATASET';
        };
        JoinAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            var typeContextParent = typeContext.parent;
            var expressionFullType = this.expression._fillRefSubstitutions(typeContextParent, indexer, alterations);
            var inputDatasetType = typeContext.datasetType;
            var expressionDatasetType = expressionFullType.datasetType;
            var newDatasetType = Object.create(null);
            for (var k in inputDatasetType) {
                newDatasetType[k] = inputDatasetType[k];
            }
            for (var k in expressionDatasetType) {
                var ft = expressionDatasetType[k];
                if (hasOwnProperty(newDatasetType, k)) {
                    if (newDatasetType[k].type !== ft.type) {
                        throw new Error("incompatible types of joins on " + k + " between " + newDatasetType[k].type + " and " + ft.type);
                    }
                }
                else {
                    newDatasetType[k] = ft;
                }
            }
            return {
                parent: typeContextParent,
                type: 'DATASET',
                datasetType: newDatasetType,
                remote: Plywood.mergeRemotes([typeContext.remote, expressionFullType.remote])
            };
        };
        JoinAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                var inV = inputFn(d, c);
                return inV ? inV.join(expressionFn(d, c)) : inV;
            };
        };
        JoinAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            throw new Error('not possible');
        };
        return JoinAction;
    })(Plywood.Action);
    Plywood.JoinAction = JoinAction;
    Plywood.Action.register(JoinAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var LessThanAction = (function (_super) {
        __extends(LessThanAction, _super);
        function LessThanAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("lessThan");
            this._checkExpressionTypes('NUMBER', 'TIME');
        }
        LessThanAction.fromJS = function (parameters) {
            return new LessThanAction(Plywood.Action.jsToValue(parameters));
        };
        LessThanAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, this.expression.type);
            return 'BOOLEAN';
        };
        LessThanAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                return inputFn(d, c) < expressionFn(d, c);
            };
        };
        LessThanAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return "(" + inputJS + "<" + expressionJS + ")";
        };
        LessThanAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return "(" + inputSQL + "<" + expressionSQL + ")";
        };
        LessThanAction.prototype._specialSimplify = function (simpleExpression) {
            var expression = this.expression;
            if (expression instanceof Plywood.LiteralExpression) {
                return new Plywood.InAction({
                    expression: new Plywood.LiteralExpression({
                        value: Plywood.Range.fromJS({ start: null, end: expression.value, bounds: '()' })
                    })
                });
            }
            return null;
        };
        LessThanAction.prototype._performOnLiteral = function (literalExpression) {
            return (new Plywood.InAction({
                expression: new Plywood.LiteralExpression({
                    value: Plywood.Range.fromJS({ start: literalExpression.value, end: null, bounds: '()' })
                })
            })).performOnSimple(this.expression);
        };
        return LessThanAction;
    })(Plywood.Action);
    Plywood.LessThanAction = LessThanAction;
    Plywood.Action.register(LessThanAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var LessThanOrEqualAction = (function (_super) {
        __extends(LessThanOrEqualAction, _super);
        function LessThanOrEqualAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("lessThanOrEqual");
            this._checkExpressionTypes('NUMBER', 'TIME');
        }
        LessThanOrEqualAction.fromJS = function (parameters) {
            return new LessThanOrEqualAction(Plywood.Action.jsToValue(parameters));
        };
        LessThanOrEqualAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, this.expression.type);
            return 'BOOLEAN';
        };
        LessThanOrEqualAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                return inputFn(d, c) <= expressionFn(d, c);
            };
        };
        LessThanOrEqualAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return "(" + inputJS + "<=" + expressionJS + ")";
        };
        LessThanOrEqualAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return "(" + inputSQL + "<=" + expressionSQL + ")";
        };
        LessThanOrEqualAction.prototype._specialSimplify = function (simpleExpression) {
            var expression = this.expression;
            if (expression instanceof Plywood.LiteralExpression) {
                return new Plywood.InAction({
                    expression: new Plywood.LiteralExpression({
                        value: Plywood.Range.fromJS({ start: null, end: expression.value, bounds: '(]' })
                    })
                });
            }
            return null;
        };
        LessThanOrEqualAction.prototype._performOnLiteral = function (literalExpression) {
            return (new Plywood.InAction({
                expression: new Plywood.LiteralExpression({
                    value: Plywood.Range.fromJS({ start: literalExpression.value, end: null, bounds: '[)' })
                })
            })).performOnSimple(this.expression);
        };
        return LessThanOrEqualAction;
    })(Plywood.Action);
    Plywood.LessThanOrEqualAction = LessThanOrEqualAction;
    Plywood.Action.register(LessThanOrEqualAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var LookupAction = (function (_super) {
        __extends(LookupAction, _super);
        function LookupAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this.lookup = parameters.lookup;
            this._ensureAction("lookup");
        }
        LookupAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.lookup = parameters.lookup;
            return new LookupAction(value);
        };
        LookupAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'STRING');
            return 'STRING';
        };
        LookupAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.lookup = this.lookup;
            return value;
        };
        LookupAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.lookup = this.lookup;
            return js;
        };
        LookupAction.prototype._toStringParameters = function (expressionString) {
            return [expressionString, String(this.lookup)];
        };
        LookupAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.lookup === other.lookup;
        };
        LookupAction.prototype.fullyDefined = function () {
            return false;
        };
        LookupAction.prototype._getFnHelper = function (inputFn) {
            throw new Error('can not express as JS');
        };
        LookupAction.prototype._getJSHelper = function (inputJS) {
            throw new Error('can not express as JS');
        };
        LookupAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            throw new Error('can not express as SQL');
        };
        return LookupAction;
    })(Plywood.Action);
    Plywood.LookupAction = LookupAction;
    Plywood.Action.register(LookupAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var LimitAction = (function (_super) {
        __extends(LimitAction, _super);
        function LimitAction(parameters) {
            if (parameters === void 0) { parameters = {}; }
            _super.call(this, parameters, dummyObject);
            this.limit = parameters.limit;
            this._ensureAction("limit");
        }
        LimitAction.fromJS = function (parameters) {
            return new LimitAction({
                action: parameters.action,
                limit: parameters.limit
            });
        };
        LimitAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.limit = this.limit;
            return value;
        };
        LimitAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.limit = this.limit;
            return js;
        };
        LimitAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'DATASET';
        };
        LimitAction.prototype._toStringParameters = function (expressionString) {
            return [String(this.limit)];
        };
        LimitAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.limit === other.limit;
        };
        LimitAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            var limit = this.limit;
            return function (d, c) {
                var inV = inputFn(d, c);
                return inV ? inV.limit(limit) : null;
            };
        };
        LimitAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return "LIMIT " + this.limit;
        };
        LimitAction.prototype._foldWithPrevAction = function (prevAction) {
            if (prevAction instanceof LimitAction) {
                return new LimitAction({
                    limit: Math.min(prevAction.limit, this.limit)
                });
            }
            return null;
        };
        LimitAction.prototype._putBeforeAction = function (lastAction) {
            if (lastAction instanceof Plywood.ApplyAction) {
                return this;
            }
            return null;
        };
        return LimitAction;
    })(Plywood.Action);
    Plywood.LimitAction = LimitAction;
    Plywood.Action.register(LimitAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var REGEXP_SPECIAL = "\\^$.|?*+()[{";
    var MatchAction = (function (_super) {
        __extends(MatchAction, _super);
        function MatchAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this.regexp = parameters.regexp;
            this._ensureAction("match");
        }
        MatchAction.likeToRegExp = function (like, escapeChar) {
            if (escapeChar === void 0) { escapeChar = '\\'; }
            var regExp = ['^'];
            for (var i = 0; i < like.length; i++) {
                var char = like[i];
                if (char === escapeChar) {
                    var nextChar = like[i + 1];
                    if (!nextChar)
                        throw new Error("invalid LIKE string '" + like + "'");
                    char = nextChar;
                    i++;
                }
                else if (char === '%') {
                    regExp.push('.*');
                    continue;
                }
                else if (char === '_') {
                    regExp.push('.');
                    continue;
                }
                if (REGEXP_SPECIAL.indexOf(char) !== -1) {
                    regExp.push('\\');
                }
                regExp.push(char);
            }
            regExp.push('$');
            return regExp.join('');
        };
        MatchAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.regexp = parameters.regexp;
            return new MatchAction(value);
        };
        MatchAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.regexp = this.regexp;
            return value;
        };
        MatchAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.regexp = this.regexp;
            return js;
        };
        MatchAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'STRING');
            return 'BOOLEAN';
        };
        MatchAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            return {
                type: 'BOOLEAN',
                remote: typeContext.remote
            };
        };
        MatchAction.prototype._toStringParameters = function (expressionString) {
            return [this.regexp];
        };
        MatchAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.regexp === other.regexp;
        };
        MatchAction.prototype._getFnHelper = function (inputFn) {
            var re = new RegExp(this.regexp);
            return function (d, c) {
                return re.test(inputFn(d, c));
            };
        };
        MatchAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return "/" + this.regexp + "/.test(" + inputJS + ")";
        };
        MatchAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return inputSQL + " REGEXP '" + this.regexp + "'";
        };
        return MatchAction;
    })(Plywood.Action);
    Plywood.MatchAction = MatchAction;
    Plywood.Action.register(MatchAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var MaxAction = (function (_super) {
        __extends(MaxAction, _super);
        function MaxAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("max");
        }
        MaxAction.fromJS = function (parameters) {
            return new MaxAction(Plywood.Action.jsToValue(parameters));
        };
        MaxAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'NUMBER';
        };
        MaxAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
            return {
                type: 'NUMBER',
                remote: typeContext.remote
            };
        };
        MaxAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return 'MAX(' + expressionSQL + ')';
        };
        MaxAction.prototype.isAggregate = function () {
            return true;
        };
        MaxAction.prototype.isNester = function () {
            return true;
        };
        return MaxAction;
    })(Plywood.Action);
    Plywood.MaxAction = MaxAction;
    Plywood.Action.register(MaxAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var MinAction = (function (_super) {
        __extends(MinAction, _super);
        function MinAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("min");
        }
        MinAction.fromJS = function (parameters) {
            return new MinAction(Plywood.Action.jsToValue(parameters));
        };
        MinAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'NUMBER';
        };
        MinAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
            return {
                type: 'NUMBER',
                remote: typeContext.remote
            };
        };
        MinAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return 'MIN(' + expressionSQL + ')';
        };
        MinAction.prototype.isAggregate = function () {
            return true;
        };
        MinAction.prototype.isNester = function () {
            return true;
        };
        return MinAction;
    })(Plywood.Action);
    Plywood.MinAction = MinAction;
    Plywood.Action.register(MinAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var MultiplyAction = (function (_super) {
        __extends(MultiplyAction, _super);
        function MultiplyAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("multiply");
            this._checkExpressionType('NUMBER');
        }
        MultiplyAction.fromJS = function (parameters) {
            return new MultiplyAction(Plywood.Action.jsToValue(parameters));
        };
        MultiplyAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'NUMBER');
            return 'NUMBER';
        };
        MultiplyAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
            return {
                type: 'NUMBER',
                remote: typeContext.remote
            };
        };
        MultiplyAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                return (inputFn(d, c) || 0) * (expressionFn(d, c) || 0);
            };
        };
        MultiplyAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return '(' + inputJS + '*' + expressionJS + ')';
        };
        MultiplyAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return '(' + inputSQL + '*' + expressionSQL + ')';
        };
        MultiplyAction.prototype._removeAction = function () {
            return this.expression.equals(Plywood.Expression.ONE);
        };
        MultiplyAction.prototype._nukeExpression = function () {
            if (this.expression.equals(Plywood.Expression.ZERO))
                return Plywood.Expression.ZERO;
            return null;
        };
        MultiplyAction.prototype._distributeAction = function () {
            return this.expression.actionize(this.action);
        };
        MultiplyAction.prototype._performOnLiteral = function (literalExpression) {
            if (literalExpression.equals(Plywood.Expression.ONE)) {
                return this.expression;
            }
            else if (literalExpression.equals(Plywood.Expression.ZERO)) {
                return Plywood.Expression.ZERO;
            }
            return null;
        };
        return MultiplyAction;
    })(Plywood.Action);
    Plywood.MultiplyAction = MultiplyAction;
    Plywood.Action.register(MultiplyAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var NotAction = (function (_super) {
        __extends(NotAction, _super);
        function NotAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("not");
        }
        NotAction.fromJS = function (parameters) {
            return new NotAction(Plywood.Action.jsToValue(parameters));
        };
        NotAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'BOOLEAN');
            return 'BOOLEAN';
        };
        NotAction.prototype._getFnHelper = function (inputFn) {
            return function (d, c) {
                return !inputFn(d, c);
            };
        };
        NotAction.prototype._getJSHelper = function (inputJS) {
            return "!(" + inputJS + ")";
        };
        NotAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return 'NOT(' + inputSQL + ')';
        };
        NotAction.prototype._foldWithPrevAction = function (prevAction) {
            if (prevAction instanceof NotAction) {
                return new Plywood.AndAction({ expression: Plywood.Expression.TRUE });
            }
            return null;
        };
        return NotAction;
    })(Plywood.Action);
    Plywood.NotAction = NotAction;
    Plywood.Action.register(NotAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var NumberBucketAction = (function (_super) {
        __extends(NumberBucketAction, _super);
        function NumberBucketAction(parameters) {
            _super.call(this, parameters, dummyObject);
            var size = parameters.size;
            this.size = size;
            var offset = parameters.offset;
            this.offset = offset;
            var lowerLimit = parameters.lowerLimit;
            this.lowerLimit = lowerLimit;
            var upperLimit = parameters.upperLimit;
            this.upperLimit = upperLimit;
            this._ensureAction("numberBucket");
            if (lowerLimit !== null && upperLimit !== null && upperLimit - lowerLimit < size) {
                throw new Error('lowerLimit and upperLimit must be at least size apart');
            }
        }
        NumberBucketAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.size = parameters.size;
            value.offset = hasOwnProperty(parameters, 'offset') ? parameters.offset : 0;
            value.lowerLimit = hasOwnProperty(parameters, 'lowerLimit') ? parameters.lowerLimit : null;
            value.upperLimit = hasOwnProperty(parameters, 'upperLimit') ? parameters.upperLimit : null;
            return new NumberBucketAction(value);
        };
        NumberBucketAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.size = this.size;
            value.offset = this.offset;
            value.lowerLimit = this.lowerLimit;
            value.upperLimit = this.upperLimit;
            return value;
        };
        NumberBucketAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.size = this.size;
            if (this.offset)
                js.offset = this.offset;
            if (this.lowerLimit !== null)
                js.lowerLimit = this.lowerLimit;
            if (this.upperLimit !== null)
                js.upperLimit = this.upperLimit;
            return js;
        };
        NumberBucketAction.prototype.getOutputType = function (inputType) {
            this._checkInputTypes(inputType, 'NUMBER', 'NUMBER_RANGE');
            return 'NUMBER_RANGE';
        };
        NumberBucketAction.prototype._toStringParameters = function (expressionString) {
            var params = [String(this.size)];
            if (this.offset)
                params.push(String(this.offset));
            return params;
        };
        NumberBucketAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.size === other.size &&
                this.offset === other.offset &&
                this.lowerLimit === other.lowerLimit &&
                this.upperLimit === other.upperLimit;
        };
        NumberBucketAction.prototype._getFnHelper = function (inputFn) {
            var size = this.size;
            var offset = this.offset;
            var lowerLimit = this.lowerLimit;
            var upperLimit = this.upperLimit;
            return function (d, c) {
                var num = inputFn(d, c);
                if (num === null)
                    return null;
                return Plywood.NumberRange.numberBucket(num, size, offset);
            };
        };
        NumberBucketAction.prototype._getJSHelper = function (inputJS) {
            return Plywood.continuousFloorExpression(inputJS, "Math.floor", this.size, this.offset);
        };
        NumberBucketAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return Plywood.continuousFloorExpression(inputSQL, "FLOOR", this.size, this.offset);
        };
        return NumberBucketAction;
    })(Plywood.Action);
    Plywood.NumberBucketAction = NumberBucketAction;
    Plywood.Action.register(NumberBucketAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    function mergeOr(ex1, ex2) {
        if (!ex1.isOp('chain') ||
            !ex2.isOp('chain') ||
            !ex1.expression.isOp('ref') ||
            !ex2.expression.isOp('ref') ||
            !arraysEqual(ex1.getFreeReferences(), ex2.getFreeReferences()))
            return null;
        var ex1Actions = ex1.actions;
        var ex2Actions = ex2.actions;
        if (ex1Actions.length !== 1 || ex2Actions.length !== 1)
            return null;
        var firstActionExpression1 = ex1Actions[0].expression;
        var firstActionExpression2 = ex2Actions[0].expression;
        if (!firstActionExpression1.isOp('literal') || !firstActionExpression2.isOp('literal'))
            return null;
        var intersect = Plywood.Set.generalUnion(firstActionExpression1.getLiteralValue(), firstActionExpression2.getLiteralValue());
        if (intersect === null)
            return null;
        return Plywood.Expression.inOrIs(ex1.expression, intersect);
    }
    var OrAction = (function (_super) {
        __extends(OrAction, _super);
        function OrAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("or");
        }
        OrAction.fromJS = function (parameters) {
            return new OrAction(Plywood.Action.jsToValue(parameters));
        };
        OrAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'BOOLEAN');
            return 'BOOLEAN';
        };
        OrAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                return inputFn(d, c) || expressionFn(d, c);
            };
        };
        OrAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return '(' + inputJS + '||' + expressionJS + ')';
        };
        OrAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return '(' + inputSQL + ' OR ' + expressionSQL + ')';
        };
        OrAction.prototype._removeAction = function () {
            return this.expression.equals(Plywood.Expression.FALSE);
        };
        OrAction.prototype._nukeExpression = function () {
            if (this.expression.equals(Plywood.Expression.TRUE))
                return Plywood.Expression.TRUE;
            return null;
        };
        OrAction.prototype._distributeAction = function () {
            return this.expression.actionize(this.action);
        };
        OrAction.prototype._performOnLiteral = function (literalExpression) {
            if (literalExpression.equals(Plywood.Expression.FALSE)) {
                return this.expression;
            }
            if (literalExpression.equals(Plywood.Expression.TRUE)) {
                return Plywood.Expression.TRUE;
            }
            return null;
        };
        OrAction.prototype._performOnChain = function (chainExpression) {
            var expression = this.expression;
            var orExpressions = chainExpression.getExpressionPattern('or');
            if (orExpressions) {
                for (var i = 0; i < orExpressions.length; i++) {
                    var orExpression = orExpressions[i];
                    var mergedExpression = mergeOr(orExpression, expression);
                    if (mergedExpression) {
                        orExpressions[i] = mergedExpression;
                        return Plywood.Expression.or(orExpressions).simplify();
                    }
                }
            }
            else {
                return mergeOr(chainExpression, expression);
            }
        };
        return OrAction;
    })(Plywood.Action);
    Plywood.OrAction = OrAction;
    Plywood.Action.register(OrAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var QuantileAction = (function (_super) {
        __extends(QuantileAction, _super);
        function QuantileAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this.quantile = parameters.quantile;
            this._ensureAction("quantile");
        }
        QuantileAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.quantile = parameters.quantile;
            return new QuantileAction(value);
        };
        QuantileAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.quantile = this.quantile;
            return value;
        };
        QuantileAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.quantile = this.quantile;
            return js;
        };
        QuantileAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'NUMBER';
        };
        QuantileAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
            return {
                type: 'NUMBER',
                remote: typeContext.remote
            };
        };
        QuantileAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            var quantile = this.quantile;
            return function (d, c) {
                var inV = inputFn(d, c);
                return inV ? inV.quantile(expressionFn, quantile, Plywood.foldContext(d, c)) : null;
            };
        };
        QuantileAction.prototype._toStringParameters = function (expressionString) {
            return [expressionString, String(this.quantile)];
        };
        QuantileAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.quantile === other.quantile;
        };
        QuantileAction.prototype.isAggregate = function () {
            return true;
        };
        QuantileAction.prototype.isNester = function () {
            return true;
        };
        return QuantileAction;
    })(Plywood.Action);
    Plywood.QuantileAction = QuantileAction;
    Plywood.Action.register(QuantileAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var SortAction = (function (_super) {
        __extends(SortAction, _super);
        function SortAction(parameters) {
            if (parameters === void 0) { parameters = {}; }
            _super.call(this, parameters, dummyObject);
            var direction = parameters.direction;
            if (direction !== SortAction.DESCENDING && direction !== SortAction.ASCENDING) {
                throw new Error("direction must be '" + SortAction.DESCENDING + "' or '" + SortAction.ASCENDING + "'");
            }
            this.direction = direction;
            if (!this.expression.isOp('ref')) {
                throw new Error("must be a reference expression (for now): " + this.toString());
            }
            this._ensureAction("sort");
        }
        SortAction.fromJS = function (parameters) {
            return new SortAction({
                action: parameters.action,
                expression: Plywood.Expression.fromJS(parameters.expression),
                direction: parameters.direction
            });
        };
        SortAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.direction = this.direction;
            return value;
        };
        SortAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.direction = this.direction;
            return js;
        };
        SortAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'DATASET';
        };
        SortAction.prototype._toStringParameters = function (expressionString) {
            return [expressionString, this.direction];
        };
        SortAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.direction === other.direction;
        };
        SortAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            var direction = this.direction;
            return function (d, c) {
                var inV = inputFn(d, c);
                return inV ? inV.sort(expressionFn, direction) : null;
            };
        };
        SortAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            var dir = this.direction === SortAction.DESCENDING ? 'DESC' : 'ASC';
            return "ORDER BY " + expressionSQL + " " + dir;
        };
        SortAction.prototype.refName = function () {
            var expression = this.expression;
            return (expression instanceof Plywood.RefExpression) ? expression.name : null;
        };
        SortAction.prototype.isNester = function () {
            return true;
        };
        SortAction.prototype._foldWithPrevAction = function (prevAction) {
            if (prevAction instanceof SortAction && this.expression.equals(prevAction.expression)) {
                return this;
            }
            return null;
        };
        SortAction.prototype.toggleDirection = function () {
            return new SortAction({
                expression: this.expression,
                direction: this.direction === SortAction.ASCENDING ? SortAction.DESCENDING : SortAction.ASCENDING
            });
        };
        SortAction.DESCENDING = 'descending';
        SortAction.ASCENDING = 'ascending';
        return SortAction;
    })(Plywood.Action);
    Plywood.SortAction = SortAction;
    Plywood.Action.register(SortAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    function splitsFromJS(splitsJS) {
        var splits = Object.create(null);
        for (var name in splitsJS) {
            if (!hasOwnProperty(splitsJS, name))
                continue;
            splits[name] = Plywood.Expression.fromJS(splitsJS[name]);
        }
        return splits;
    }
    function splitsEqual(splitsA, splitsB) {
        var keysA = Object.keys(splitsA);
        var keysB = Object.keys(splitsB);
        if (keysA.length !== keysB.length)
            return false;
        for (var _i = 0; _i < keysA.length; _i++) {
            var k = keysA[_i];
            if (!splitsA[k].equals(splitsB[k]))
                return false;
        }
        return true;
    }
    var SplitAction = (function (_super) {
        __extends(SplitAction, _super);
        function SplitAction(parameters) {
            _super.call(this, parameters, dummyObject);
            var splits = parameters.splits;
            if (!splits)
                throw new Error('must have splits');
            this.splits = splits;
            this.keys = Object.keys(splits).sort();
            if (!this.keys.length)
                throw new Error('must have at least one split');
            this.dataName = parameters.dataName;
            this._ensureAction("split");
        }
        SplitAction.fromJS = function (parameters) {
            var value = {
                action: parameters.action
            };
            var splits;
            if (parameters.expression && parameters.name) {
                splits = (_a = {}, _a[parameters.name] = parameters.expression, _a);
            }
            else {
                splits = parameters.splits;
            }
            value.splits = splitsFromJS(splits);
            value.dataName = parameters.dataName;
            return new SplitAction(value);
            var _a;
        };
        SplitAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.splits = this.splits;
            value.dataName = this.dataName;
            return value;
        };
        SplitAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            if (this.isMultiSplit()) {
                js.splits = this.mapSplitExpressions(function (ex) { return ex.toJS(); });
            }
            else {
                var splits = this.splits;
                for (var name in splits) {
                    js.name = name;
                    js.expression = splits[name].toJS();
                }
            }
            js.dataName = this.dataName;
            return js;
        };
        SplitAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'DATASET';
        };
        SplitAction.prototype._toStringParameters = function (expressionString) {
            if (this.isMultiSplit()) {
                var splits = this.splits;
                var splitStrings = [];
                for (var name in splits) {
                    splitStrings.push(name + ": " + splits[name].toString());
                }
                return [splitStrings.join(', '), this.dataName];
            }
            else {
                return [this.firstSplitExpression().toString(), this.firstSplitName(), this.dataName];
            }
        };
        SplitAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                splitsEqual(this.splits, other.splits) &&
                this.dataName === other.dataName;
        };
        SplitAction.prototype.getFn = function (inputFn) {
            var dataName = this.dataName;
            var splitFns = this.mapSplitExpressions(function (ex) { return ex.getFn(); });
            return function (d, c) {
                var inV = inputFn(d, c);
                return inV ? inV.split(splitFns, dataName) : null;
            };
        };
        SplitAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            var newDatasetType = {};
            this.mapSplits(function (name, expression) {
                newDatasetType[name] = expression._fillRefSubstitutions(typeContext, indexer, alterations);
            });
            newDatasetType[this.dataName] = typeContext;
            return {
                parent: typeContext.parent,
                type: 'DATASET',
                datasetType: newDatasetType,
                remote: null
            };
        };
        SplitAction.prototype.getSQL = function (inputSQL, dialect) {
            var groupBys = this.mapSplits(function (name, expression) { return expression.getSQL(dialect); });
            return "GROUP BY " + groupBys.join(', ');
        };
        SplitAction.prototype.getSelectSQL = function (dialect) {
            return this.mapSplits(function (name, expression) { return (expression.getSQL(dialect) + " AS " + dialect.escapeLiteral(name)); });
        };
        SplitAction.prototype.getShortGroupBySQL = function () {
            return 'GROUP BY ' + Object.keys(this.splits).map(function (d, i) { return i + 1; }).join(', ');
        };
        SplitAction.prototype.expressionCount = function () {
            var count = 0;
            this.mapSplits(function (k, expression) {
                count += expression.expressionCount();
            });
            return count;
        };
        SplitAction.prototype.fullyDefined = function () {
            return false;
        };
        SplitAction.prototype.simplify = function () {
            if (this.simple)
                return this;
            var simpleSplits = this.mapSplitExpressions(function (ex) { return ex.simplify(); });
            var value = this.valueOf();
            value.splits = simpleSplits;
            value.simple = true;
            return new SplitAction(value);
        };
        SplitAction.prototype.getExpressions = function () {
            return this.mapSplits(function (name, ex) { return ex; });
        };
        SplitAction.prototype._substituteHelper = function (substitutionFn, thisArg, indexer, depth, nestDiff) {
            var nestDiffNext = nestDiff + 1;
            var hasChanged = false;
            var subSplits = this.mapSplitExpressions(function (ex) {
                var subExpression = ex._substituteHelper(substitutionFn, thisArg, indexer, depth, nestDiffNext);
                if (subExpression !== ex)
                    hasChanged = true;
                return subExpression;
            });
            if (!hasChanged)
                return this;
            var value = this.valueOf();
            value.splits = subSplits;
            return new SplitAction(value);
        };
        SplitAction.prototype.applyToExpression = function (transformation) {
            var hasChanged = false;
            var newSplits = this.mapSplitExpressions(function (ex) {
                var newExpression = transformation(ex);
                if (newExpression !== ex)
                    hasChanged = true;
                return newExpression;
            });
            if (!hasChanged)
                return this;
            var value = this.valueOf();
            value.splits = newSplits;
            return new SplitAction(value);
        };
        SplitAction.prototype.isNester = function () {
            return true;
        };
        SplitAction.prototype.numSplits = function () {
            return this.keys.length;
        };
        SplitAction.prototype.isMultiSplit = function () {
            return this.numSplits() > 1;
        };
        SplitAction.prototype.mapSplits = function (fn) {
            var _a = this, splits = _a.splits, keys = _a.keys;
            var res = [];
            for (var _i = 0; _i < keys.length; _i++) {
                var k = keys[_i];
                var v = fn(k, splits[k]);
                if (typeof v !== 'undefined')
                    res.push(v);
            }
            return res;
        };
        SplitAction.prototype.mapSplitExpressions = function (fn) {
            var _a = this, splits = _a.splits, keys = _a.keys;
            var ret = Object.create(null);
            for (var _i = 0; _i < keys.length; _i++) {
                var key = keys[_i];
                ret[key] = fn(splits[key], key);
            }
            return ret;
        };
        SplitAction.prototype.firstSplitName = function () {
            return this.keys[0];
        };
        SplitAction.prototype.firstSplitExpression = function () {
            return this.splits[this.firstSplitName()];
        };
        SplitAction.prototype.filterFromDatum = function (datum) {
            return Plywood.Expression.and(this.mapSplits(function (name, expression) {
                return expression.is(Plywood.r(datum[name]));
            })).simplify();
        };
        SplitAction.prototype.hasKey = function (key) {
            return hasOwnProperty(this.splits, key);
        };
        return SplitAction;
    })(Plywood.Action);
    Plywood.SplitAction = SplitAction;
    Plywood.Action.register(SplitAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var SubstrAction = (function (_super) {
        __extends(SubstrAction, _super);
        function SubstrAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this.position = parameters.position;
            this.length = parameters.length;
            this._ensureAction("substr");
        }
        SubstrAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.position = parameters.position;
            value.length = parameters.length;
            return new SubstrAction(value);
        };
        SubstrAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'STRING');
            return 'STRING';
        };
        SubstrAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.position = this.position;
            value.length = this.length;
            return value;
        };
        SubstrAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.position = this.position;
            js.length = this.length;
            return js;
        };
        SubstrAction.prototype._toStringParameters = function (expressionString) {
            return [expressionString, String(this.position), String(this.length)];
        };
        SubstrAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.position === other.position &&
                this.length === other.length;
        };
        SubstrAction.prototype._getFnHelper = function (inputFn) {
            var _a = this, position = _a.position, length = _a.length;
            return function (d, c) {
                var inV = inputFn(d, c);
                if (inV === null)
                    return null;
                return inV.substr(position, length);
            };
        };
        SubstrAction.prototype._getJSHelper = function (inputJS) {
            var _a = this, position = _a.position, length = _a.length;
            return "(''+" + inputJS + ").substr(" + position + "," + length + ")";
        };
        SubstrAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return "SUBSTR(" + inputSQL + "," + (this.position + 1) + "," + this.length + ")";
        };
        return SubstrAction;
    })(Plywood.Action);
    Plywood.SubstrAction = SubstrAction;
    Plywood.Action.register(SubstrAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var SubtractAction = (function (_super) {
        __extends(SubtractAction, _super);
        function SubtractAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("subtract");
            this._checkExpressionType('NUMBER');
        }
        SubtractAction.fromJS = function (parameters) {
            return new SubtractAction(Plywood.Action.jsToValue(parameters));
        };
        SubtractAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'NUMBER');
            return 'NUMBER';
        };
        SubtractAction.prototype._getFnHelper = function (inputFn, expressionFn) {
            return function (d, c) {
                return (inputFn(d, c) || 0) - (expressionFn(d, c) || 0);
            };
        };
        SubtractAction.prototype._getJSHelper = function (inputJS, expressionJS) {
            return '(' + inputJS + '-' + expressionJS + ')';
        };
        SubtractAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return '(' + inputSQL + '-' + expressionSQL + ')';
        };
        SubtractAction.prototype._removeAction = function () {
            return this.expression.equals(Plywood.Expression.ZERO);
        };
        return SubtractAction;
    })(Plywood.Action);
    Plywood.SubtractAction = SubtractAction;
    Plywood.Action.register(SubtractAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var SumAction = (function (_super) {
        __extends(SumAction, _super);
        function SumAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this._ensureAction("sum");
        }
        SumAction.fromJS = function (parameters) {
            return new SumAction(Plywood.Action.jsToValue(parameters));
        };
        SumAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'DATASET');
            return 'NUMBER';
        };
        SumAction.prototype._fillRefSubstitutions = function (typeContext, indexer, alterations) {
            this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
            return {
                type: 'NUMBER',
                remote: typeContext.remote
            };
        };
        SumAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return 'SUM(' + expressionSQL + ')';
        };
        SumAction.prototype.isAggregate = function () {
            return true;
        };
        SumAction.prototype.isNester = function () {
            return true;
        };
        SumAction.prototype.canDistribute = function () {
            var expression = this.expression;
            return expression instanceof Plywood.LiteralExpression ||
                Boolean(expression.getExpressionPattern('add') || expression.getExpressionPattern('subtract'));
        };
        SumAction.prototype.distribute = function (preEx) {
            var expression = this.expression;
            if (expression instanceof Plywood.LiteralExpression) {
                var value = expression.value;
                if (value === 0)
                    return Plywood.Expression.ZERO;
                return expression.multiply(preEx.count());
            }
            var pattern;
            if (pattern = expression.getExpressionPattern('add')) {
                return Plywood.Expression.add(pattern.map(function (ex) { return preEx.sum(ex).distribute(); }));
            }
            if (pattern = expression.getExpressionPattern('subtract')) {
                return Plywood.Expression.subtract(pattern.map(function (ex) { return preEx.sum(ex).distribute(); }));
            }
            return null;
        };
        return SumAction;
    })(Plywood.Action);
    Plywood.SumAction = SumAction;
    Plywood.Action.register(SumAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var TimeBucketAction = (function (_super) {
        __extends(TimeBucketAction, _super);
        function TimeBucketAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this.duration = parameters.duration;
            this.timezone = parameters.timezone;
            this._ensureAction("timeBucket");
            if (!Plywood.Duration.isDuration(this.duration)) {
                throw new Error("`duration` must be a Duration");
            }
            if (!Plywood.Timezone.isTimezone(this.timezone)) {
                throw new Error("`timezone` must be a Timezone");
            }
        }
        TimeBucketAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.duration = Plywood.Duration.fromJS(parameters.duration);
            value.timezone = Plywood.Timezone.fromJS(parameters.timezone);
            return new TimeBucketAction(value);
        };
        TimeBucketAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.duration = this.duration;
            value.timezone = this.timezone;
            return value;
        };
        TimeBucketAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.duration = this.duration.toJS();
            js.timezone = this.timezone.toJS();
            return js;
        };
        TimeBucketAction.prototype.getOutputType = function (inputType) {
            this._checkInputTypes(inputType, 'TIME', 'TIME_RANGE');
            return 'TIME_RANGE';
        };
        TimeBucketAction.prototype._toStringParameters = function (expressionString) {
            return [this.duration.toString(), this.timezone.toString()];
        };
        TimeBucketAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.duration.equals(other.duration) &&
                this.timezone.equals(other.timezone);
        };
        TimeBucketAction.prototype._getFnHelper = function (inputFn) {
            var duration = this.duration;
            var timezone = this.timezone;
            return function (d, c) {
                var inV = inputFn(d, c);
                return Plywood.TimeRange.timeBucket(inV, duration, timezone);
            };
        };
        TimeBucketAction.prototype._getJSHelper = function (inputJS) {
            throw new Error("implement me");
        };
        TimeBucketAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return dialect.timeBucketExpression(inputSQL, this.duration, this.timezone);
        };
        return TimeBucketAction;
    })(Plywood.Action);
    Plywood.TimeBucketAction = TimeBucketAction;
    Plywood.Action.register(TimeBucketAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var TimeOffsetAction = (function (_super) {
        __extends(TimeOffsetAction, _super);
        function TimeOffsetAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this.duration = parameters.duration;
            this.timezone = parameters.timezone;
            this._ensureAction("timeOffset");
            if (!Plywood.Duration.isDuration(this.duration)) {
                throw new Error("`duration` must be a Duration");
            }
        }
        TimeOffsetAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.duration = Plywood.Duration.fromJS(parameters.duration);
            value.timezone = Plywood.Timezone.fromJS(parameters.timezone);
            return new TimeOffsetAction(value);
        };
        TimeOffsetAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.duration = this.duration;
            value.timezone = this.timezone;
            return value;
        };
        TimeOffsetAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.duration = this.duration.toJS();
            js.timezone = this.timezone.toJS();
            return js;
        };
        TimeOffsetAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'TIME');
            return 'TIME';
        };
        TimeOffsetAction.prototype._toStringParameters = function (expressionString) {
            return [expressionString, this.duration.toString(), this.timezone.toString()];
        };
        TimeOffsetAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.duration.equals(other.duration) &&
                this.timezone.equals(other.timezone);
        };
        TimeOffsetAction.prototype._getFnHelper = function (inputFn) {
            var duration = this.duration;
            var timezone = this.timezone;
            return function (d, c) {
                var inV = inputFn(d, c);
                if (inV === null)
                    return null;
                return duration.move(inV, timezone, 1);
            };
        };
        TimeOffsetAction.prototype._getJSHelper = function (inputJS) {
            throw new Error("implement me");
        };
        TimeOffsetAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            return dialect.offsetTimeExpression(inputSQL, this.duration);
        };
        return TimeOffsetAction;
    })(Plywood.Action);
    Plywood.TimeOffsetAction = TimeOffsetAction;
    Plywood.Action.register(TimeOffsetAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var PART_TO_FUNCTION = {
        SECOND_OF_MINUTE: function (d) { return d.getSeconds(); },
        SECOND_OF_HOUR: function (d) { return d.getMinutes() * 60 + d.getSeconds(); },
        SECOND_OF_DAY: function (d) { return (d.getHours() * 60 + d.getMinutes()) * 60 + d.getSeconds(); },
        SECOND_OF_WEEK: function (d) { return ((d.getDay() * 24) + d.getHours() * 60 + d.getMinutes()) * 60 + d.getSeconds(); },
        SECOND_OF_MONTH: function (d) { return (((d.getDate() - 1) * 24) + d.getHours() * 60 + d.getMinutes()) * 60 + d.getSeconds(); },
        SECOND_OF_YEAR: null,
        MINUTE_OF_HOUR: function (d) { return d.getMinutes(); },
        MINUTE_OF_DAY: function (d) { return d.getHours() * 60 + d.getMinutes(); },
        MINUTE_OF_WEEK: function (d) { return (d.getDay() * 24) + d.getHours() * 60 + d.getMinutes(); },
        MINUTE_OF_MONTH: function (d) { return ((d.getDate() - 1) * 24) + d.getHours() * 60 + d.getMinutes(); },
        MINUTE_OF_YEAR: null,
        HOUR_OF_DAY: function (d) { return d.getHours(); },
        HOUR_OF_WEEK: function (d) { return d.getDay() * 24 + d.getHours(); },
        HOUR_OF_MONTH: function (d) { return (d.getDate() - 1) * 24 + d.getHours(); },
        HOUR_OF_YEAR: null,
        DAY_OF_WEEK: function (d) { return d.getDay(); },
        DAY_OF_MONTH: function (d) { return d.getDate() - 1; },
        DAY_OF_YEAR: null,
        WEEK_OF_MONTH: null,
        WEEK_OF_YEAR: null,
        MONTH_OF_YEAR: function (d) { return d.getMonth(); }
    };
    var TimePartAction = (function (_super) {
        __extends(TimePartAction, _super);
        function TimePartAction(parameters) {
            _super.call(this, parameters, dummyObject);
            this.part = parameters.part;
            this.timezone = parameters.timezone;
            this._ensureAction("timePart");
            if (typeof this.part !== 'string') {
                throw new Error("`part` must be a string");
            }
        }
        TimePartAction.fromJS = function (parameters) {
            var value = Plywood.Action.jsToValue(parameters);
            value.part = parameters.part;
            value.timezone = Plywood.Timezone.fromJS(parameters.timezone);
            return new TimePartAction(value);
        };
        TimePartAction.prototype.valueOf = function () {
            var value = _super.prototype.valueOf.call(this);
            value.part = this.part;
            value.timezone = this.timezone;
            return value;
        };
        TimePartAction.prototype.toJS = function () {
            var js = _super.prototype.toJS.call(this);
            js.part = this.part;
            js.timezone = this.timezone.toJS();
            return js;
        };
        TimePartAction.prototype.getOutputType = function (inputType) {
            this._checkInputType(inputType, 'TIME');
            return 'NUMBER';
        };
        TimePartAction.prototype._toStringParameters = function (expressionString) {
            return [expressionString, this.part.toString(), this.timezone.toString()];
        };
        TimePartAction.prototype.equals = function (other) {
            return _super.prototype.equals.call(this, other) &&
                this.part === other.part &&
                this.timezone.equals(other.timezone);
        };
        TimePartAction.prototype._getFnHelper = function (inputFn) {
            var _a = this, part = _a.part, timezone = _a.timezone;
            var parter = PART_TO_FUNCTION[part];
            if (!parter)
                throw new Error("unsupported part '" + part + "'");
            return function (d, c) {
                var inV = inputFn(d, c);
                if (!inV)
                    return null;
                inV = Plywood.WallTime.UTCToWallTime(inV, timezone.toString());
                return parter(inV);
            };
        };
        TimePartAction.prototype._getJSHelper = function (inputJS) {
            throw new Error("implement me");
        };
        TimePartAction.prototype._getSQLHelper = function (dialect, inputSQL, expressionSQL) {
            var _a = this, part = _a.part, timezone = _a.timezone;
            return dialect.timePartExpression(inputSQL, part, timezone);
        };
        TimePartAction.prototype.materializeWithinRange = function (extentRange, values) {
            var partUnits = this.part.toLowerCase().split('_of_');
            var unitSmall = partUnits[0];
            var unitBig = partUnits[1];
            var timezone = this.timezone;
            var smallTimeMover = Chronoshift[unitSmall];
            var bigTimeMover = Chronoshift[unitBig];
            var start = extentRange.start;
            var end = extentRange.end;
            var ranges = [];
            var iter = bigTimeMover.floor(start, timezone);
            while (iter <= end) {
                for (var _i = 0; _i < values.length; _i++) {
                    var value = values[_i];
                    var subIter = smallTimeMover.move(iter, timezone, value);
                    ranges.push(new Plywood.TimeRange({
                        start: subIter,
                        end: smallTimeMover.move(subIter, timezone, 1)
                    }));
                }
                iter = bigTimeMover.move(iter, timezone, 1);
            }
            return Plywood.Set.fromJS({
                setType: 'TIME_RANGE',
                elements: ranges
            });
        };
        return TimePartAction;
    })(Plywood.Action);
    Plywood.TimePartAction = TimePartAction;
    Plywood.Action.register(TimePartAction);
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    function basicExecutorFactory(parameters) {
        var datasets = parameters.datasets;
        return function (ex) {
            return ex.compute(datasets);
        };
    }
    Plywood.basicExecutorFactory = basicExecutorFactory;
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var helper;
    (function (helper) {
        var integerRegExp = /^\d+$/;
        function simpleLocator(parameters) {
            if (typeof parameters === "string")
                parameters = { resource: parameters };
            var resource = parameters.resource;
            var defaultPort = parameters.defaultPort;
            if (!resource)
                throw new Error("must have resource");
            var locations = resource.split(";").map(function (locationString) {
                var parts = locationString.split(":");
                if (parts.length > 2)
                    throw new Error("invalid resource part '" + locationString + "'");
                var location = {
                    hostname: parts[0]
                };
                if (parts.length === 2) {
                    if (!integerRegExp.test(parts[1])) {
                        throw new Error("invalid port in resource '" + parts[1] + "'");
                    }
                    location.port = Number(parts[1]);
                }
                else if (defaultPort) {
                    location.port = defaultPort;
                }
                return location;
            });
            return function () { return Q(locations[Math.floor(Math.random() * locations.length)]); };
        }
        helper.simpleLocator = simpleLocator;
    })(helper = Plywood.helper || (Plywood.helper = {}));
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var helper;
    (function (helper) {
        function verboseRequesterFactory(parameters) {
            var requester = parameters.requester;
            var printLine = parameters.printLine || (function (line) {
                console['log'](line);
            });
            var preQuery = parameters.preQuery || (function (query, queryNumber) {
                printLine("vvvvvvvvvvvvvvvvvvvvvvvvvv");
                printLine("Sending query " + queryNumber + ":");
                printLine(JSON.stringify(query, null, 2));
                printLine("^^^^^^^^^^^^^^^^^^^^^^^^^^");
            });
            var onSuccess = parameters.onSuccess || (function (data, time, query, queryNumber) {
                printLine("vvvvvvvvvvvvvvvvvvvvvvvvvv");
                printLine("Got result from query " + queryNumber + ": (in " + time + "ms)");
                printLine(JSON.stringify(data, null, 2));
                printLine("^^^^^^^^^^^^^^^^^^^^^^^^^^");
            });
            var onError = parameters.onError || (function (error, time, query, queryNumber) {
                printLine("vvvvvvvvvvvvvvvvvvvvvvvvvv");
                printLine("Got error in query " + queryNumber + ": " + error.message + " (in " + time + "ms)");
                printLine("^^^^^^^^^^^^^^^^^^^^^^^^^^");
            });
            var queryNumber = 0;
            return function (request) {
                queryNumber++;
                var myQueryNumber = queryNumber;
                preQuery(request.query, myQueryNumber);
                var startTime = Date.now();
                return requester(request)
                    .then(function (data) {
                    onSuccess(data, Date.now() - startTime, request.query, myQueryNumber);
                    return data;
                }, function (error) {
                    onError(error, Date.now() - startTime, request.query, myQueryNumber);
                    throw error;
                });
            };
        }
        helper.verboseRequesterFactory = verboseRequesterFactory;
    })(helper = Plywood.helper || (Plywood.helper = {}));
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var helper;
    (function (helper) {
        function retryRequester(parameters) {
            console.warn('retryRequester has been renamed to retryRequesterFactory and will soon be deprecated');
            return retryRequesterFactory(parameters);
        }
        helper.retryRequester = retryRequester;
        function retryRequesterFactory(parameters) {
            var requester = parameters.requester;
            var delay = parameters.delay || 500;
            var retry = parameters.retry || 3;
            var retryOnTimeout = Boolean(parameters.retryOnTimeout);
            if (typeof delay !== "number")
                throw new TypeError("delay should be a number");
            if (typeof retry !== "number")
                throw new TypeError("retry should be a number");
            return function (request) {
                var tries = 1;
                function handleError(err) {
                    if (tries > retry)
                        throw err;
                    tries++;
                    if (err.message === "timeout" && !retryOnTimeout)
                        throw err;
                    return Q.delay(delay).then(function () { return requester(request); }).catch(handleError);
                }
                return requester(request).catch(handleError);
            };
        }
        helper.retryRequesterFactory = retryRequesterFactory;
    })(helper = Plywood.helper || (Plywood.helper = {}));
})(Plywood || (Plywood = {}));
var Plywood;
(function (Plywood) {
    var helper;
    (function (helper) {
        function concurrentLimitRequesterFactory(parameters) {
            var requester = parameters.requester;
            var concurrentLimit = parameters.concurrentLimit || 5;
            if (typeof concurrentLimit !== "number")
                throw new TypeError("concurrentLimit should be a number");
            var requestQueue = [];
            var outstandingRequests = 0;
            function requestFinished() {
                outstandingRequests--;
                if (!(requestQueue.length && outstandingRequests < concurrentLimit))
                    return;
                var queueItem = requestQueue.shift();
                var deferred = queueItem.deferred;
                outstandingRequests++;
                requester(queueItem.request)
                    .then(deferred.resolve, deferred.reject)
                    .fin(requestFinished);
            }
            return function (request) {
                if (outstandingRequests < concurrentLimit) {
                    outstandingRequests++;
                    return requester(request).fin(requestFinished);
                }
                else {
                    var deferred = Q.defer();
                    requestQueue.push({
                        request: request,
                        deferred: deferred
                    });
                    return deferred.promise;
                }
            };
        }
        helper.concurrentLimitRequesterFactory = concurrentLimitRequesterFactory;
    })(helper = Plywood.helper || (Plywood.helper = {}));
})(Plywood || (Plywood = {}));
expressionParser = require("../parser/expression")(Plywood);
sqlParser = require("../parser/sql")(Plywood);
if (typeof module !== 'undefined' && typeof module.exports !== 'undefined') {
    module.exports = Plywood;
    module.exports.Chronoshift = Chronoshift;
}

},{"../parser/expression":7,"../parser/sql":8,"chronoshift":3,"immutable-class":5,"q":6}],2:[function(require,module,exports){
// shim for using process in browser

var process = module.exports = {};
var queue = [];
var draining = false;
var currentQueue;
var queueIndex = -1;

function cleanUpNextTick() {
    draining = false;
    if (currentQueue.length) {
        queue = currentQueue.concat(queue);
    } else {
        queueIndex = -1;
    }
    if (queue.length) {
        drainQueue();
    }
}

function drainQueue() {
    if (draining) {
        return;
    }
    var timeout = setTimeout(cleanUpNextTick);
    draining = true;

    var len = queue.length;
    while(len) {
        currentQueue = queue;
        queue = [];
        while (++queueIndex < len) {
            if (currentQueue) {
                currentQueue[queueIndex].run();
            }
        }
        queueIndex = -1;
        len = queue.length;
    }
    currentQueue = null;
    draining = false;
    clearTimeout(timeout);
}

process.nextTick = function (fun) {
    var args = new Array(arguments.length - 1);
    if (arguments.length > 1) {
        for (var i = 1; i < arguments.length; i++) {
            args[i - 1] = arguments[i];
        }
    }
    queue.push(new Item(fun, args));
    if (queue.length === 1 && !draining) {
        setTimeout(drainQueue, 0);
    }
};

// v8 likes predictible objects
function Item(fun, array) {
    this.fun = fun;
    this.array = array;
}
Item.prototype.run = function () {
    this.fun.apply(null, this.array);
};
process.title = 'browser';
process.browser = true;
process.env = {};
process.argv = [];
process.version = ''; // empty string to avoid regexp issues
process.versions = {};

function noop() {}

process.on = noop;
process.addListener = noop;
process.once = noop;
process.off = noop;
process.removeListener = noop;
process.removeAllListeners = noop;
process.emit = noop;

process.binding = function (name) {
    throw new Error('process.binding is not supported');
};

process.cwd = function () { return '/' };
process.chdir = function (dir) {
    throw new Error('process.chdir is not supported');
};
process.umask = function() { return 0; };

},{}],3:[function(require,module,exports){
/// <reference path="../typings/immutable-class.d.ts" />
"use strict";
var Chronoshift;
(function (Chronoshift) {
    Chronoshift.WallTime = require("../lib/walltime");
    var ImmutableClass = require("immutable-class");
    Chronoshift.isInstanceOf = ImmutableClass.isInstanceOf;
    function isDate(d) {
        return typeof d === 'object' &&
            d.constructor.name === 'Date';
    }
    Chronoshift.isDate = isDate;
})(Chronoshift || (Chronoshift = {}));
var Chronoshift;
(function (Chronoshift) {
    /**
     * Represents timezones
     */
    var check;
    var Timezone = (function () {
        /**
         * Constructs a timezone form the string representation by checking that it is defined
         */
        function Timezone(timezone) {
            if (typeof timezone !== 'string') {
                throw new TypeError("timezone description must be a string");
            }
            if (timezone !== 'Etc/UTC') {
                Chronoshift.WallTime.UTCToWallTime(new Date(0), timezone); // This will throw an error if timezone is not a real timezone
            }
            this.timezone = timezone;
        }
        Timezone.isTimezone = function (candidate) {
            return Chronoshift.isInstanceOf(candidate, Timezone);
        };
        Timezone.fromJS = function (spec) {
            return new Timezone(spec);
        };
        Timezone.prototype.valueOf = function () {
            return this.timezone;
        };
        Timezone.prototype.toJS = function () {
            return this.timezone;
        };
        Timezone.prototype.toJSON = function () {
            return this.timezone;
        };
        Timezone.prototype.toString = function () {
            return this.timezone;
        };
        Timezone.prototype.equals = function (other) {
            return Timezone.isTimezone(other) &&
                this.timezone === other.timezone;
        };
        Timezone.prototype.isUTC = function () {
            return this.timezone === 'Etc/UTC';
        };
        return Timezone;
    })();
    Chronoshift.Timezone = Timezone;
    check = Timezone;
    Timezone.UTC = new Timezone('Etc/UTC');
})(Chronoshift || (Chronoshift = {}));
var Chronoshift;
(function (Chronoshift) {
    function adjustDay(day) {
        return (day + 6) % 7;
    }
    function timeMoverFiller(tm) {
        var floor = tm.floor, move = tm.move;
        tm.ceil = function (dt, tz) {
            var floored = floor(dt, tz);
            if (floored.valueOf() === dt.valueOf())
                return dt; // Just like ceil(3) is 3 and not 4
            return move(floored, tz, 1);
        };
        return tm;
    }
    Chronoshift.second = timeMoverFiller({
        canonicalLength: 1000,
        siblings: 60,
        floor: function (dt, tz) {
            // Seconds do not actually need a timezone because all timezones align on seconds... for now...
            dt = new Date(dt.valueOf());
            dt.setUTCMilliseconds(0);
            return dt;
        },
        round: function (dt, roundTo, tz) {
            var cur = dt.getUTCSeconds();
            var adj = Math.floor(cur / roundTo) * roundTo;
            if (cur !== adj)
                dt.setUTCSeconds(adj);
            return dt;
        },
        move: function (dt, tz, step) {
            dt = new Date(dt.valueOf());
            dt.setUTCSeconds(dt.getUTCSeconds() + step);
            return dt;
        }
    });
    Chronoshift.minute = timeMoverFiller({
        canonicalLength: 60000,
        siblings: 60,
        floor: function (dt, tz) {
            // Minutes do not actually need a timezone because all timezones align on minutes... for now...
            dt = new Date(dt.valueOf());
            dt.setUTCSeconds(0, 0);
            return dt;
        },
        round: function (dt, roundTo, tz) {
            var cur = dt.getUTCMinutes();
            var adj = Math.floor(cur / roundTo) * roundTo;
            if (cur !== adj)
                dt.setUTCMinutes(adj);
            return dt;
        },
        move: function (dt, tz, step) {
            dt = new Date(dt.valueOf());
            dt.setUTCMinutes(dt.getUTCMinutes() + step);
            return dt;
        }
    });
    function hourMove(dt, tz, step) {
        if (tz.isUTC()) {
            dt = new Date(dt.valueOf());
            dt.setUTCHours(dt.getUTCHours() + step);
        }
        else {
            var wt = Chronoshift.WallTime.UTCToWallTime(dt, tz.toString());
            dt = Chronoshift.WallTime.WallTimeToUTC(tz.toString(), wt.getFullYear(), wt.getMonth(), wt.getDate(), wt.getHours() + step, wt.getMinutes(), wt.getSeconds(), wt.getMilliseconds());
        }
        return dt;
    }
    Chronoshift.hour = timeMoverFiller({
        canonicalLength: 3600000,
        siblings: 24,
        floor: function (dt, tz) {
            if (tz.isUTC()) {
                dt = new Date(dt.valueOf());
                dt.setUTCMinutes(0, 0, 0);
            }
            else {
                var wt = Chronoshift.WallTime.UTCToWallTime(dt, tz.toString());
                dt = Chronoshift.WallTime.WallTimeToUTC(tz.toString(), wt.getFullYear(), wt.getMonth(), wt.getDate(), wt.getHours(), 0, 0, 0);
            }
            return dt;
        },
        round: function (dt, roundTo, tz) {
            if (tz.isUTC()) {
                var cur = dt.getUTCHours();
                var adj = Math.floor(cur / roundTo) * roundTo;
                if (cur !== adj)
                    dt.setUTCHours(adj);
            }
            else {
                var cur = dt.getHours();
                var adj = Math.floor(cur / roundTo) * roundTo;
                if (cur !== adj)
                    return hourMove(dt, tz, adj - cur);
            }
            return dt;
        },
        move: hourMove
    });
    Chronoshift.day = timeMoverFiller({
        canonicalLength: 24 * 3600000,
        floor: function (dt, tz) {
            if (tz.isUTC()) {
                dt = new Date(dt.valueOf());
                dt.setUTCHours(0, 0, 0, 0);
            }
            else {
                var wt = Chronoshift.WallTime.UTCToWallTime(dt, tz.toString());
                dt = Chronoshift.WallTime.WallTimeToUTC(tz.toString(), wt.getFullYear(), wt.getMonth(), wt.getDate(), 0, 0, 0, 0);
            }
            return dt;
        },
        move: function (dt, tz, step) {
            if (tz.isUTC()) {
                dt = new Date(dt.valueOf());
                dt.setUTCDate(dt.getUTCDate() + step);
            }
            else {
                var wt = Chronoshift.WallTime.UTCToWallTime(dt, tz.toString());
                dt = Chronoshift.WallTime.WallTimeToUTC(tz.toString(), wt.getFullYear(), wt.getMonth(), wt.getDate() + step, wt.getHours(), wt.getMinutes(), wt.getSeconds(), wt.getMilliseconds());
            }
            return dt;
        }
    });
    Chronoshift.week = timeMoverFiller({
        canonicalLength: 7 * 24 * 3600000,
        floor: function (dt, tz) {
            if (tz.isUTC()) {
                dt = new Date(dt.valueOf());
                dt.setUTCHours(0, 0, 0, 0);
                dt.setUTCDate(dt.getUTCDate() - adjustDay(dt.getUTCDay()));
            }
            else {
                var wt = Chronoshift.WallTime.UTCToWallTime(dt, tz.toString());
                dt = Chronoshift.WallTime.WallTimeToUTC(tz.toString(), wt.getFullYear(), wt.getMonth(), wt.getDate() - adjustDay(wt.getDay()), 0, 0, 0, 0);
            }
            return dt;
        },
        move: function (dt, tz, step) {
            if (tz.isUTC()) {
                dt = new Date(dt.valueOf());
                dt.setUTCDate(dt.getUTCDate() + step * 7);
            }
            else {
                var wt = Chronoshift.WallTime.UTCToWallTime(dt, tz.toString());
                dt = Chronoshift.WallTime.WallTimeToUTC(tz.toString(), wt.getFullYear(), wt.getMonth(), wt.getDate() + step * 7, wt.getHours(), wt.getMinutes(), wt.getSeconds(), wt.getMilliseconds());
            }
            return dt;
        }
    });
    function monthMove(dt, tz, step) {
        if (tz.isUTC()) {
            dt = new Date(dt.valueOf());
            dt.setUTCMonth(dt.getUTCMonth() + step);
        }
        else {
            var wt = Chronoshift.WallTime.UTCToWallTime(dt, tz.toString());
            dt = Chronoshift.WallTime.WallTimeToUTC(tz.toString(), wt.getFullYear(), wt.getMonth() + step, wt.getDate(), wt.getHours(), wt.getMinutes(), wt.getSeconds(), wt.getMilliseconds());
        }
        return dt;
    }
    Chronoshift.month = timeMoverFiller({
        canonicalLength: 30 * 24 * 3600000,
        siblings: 12,
        floor: function (dt, tz) {
            if (tz.isUTC()) {
                dt = new Date(dt.valueOf());
                dt.setUTCHours(0, 0, 0, 0);
                dt.setUTCDate(1);
            }
            else {
                var wt = Chronoshift.WallTime.UTCToWallTime(dt, tz.toString());
                dt = Chronoshift.WallTime.WallTimeToUTC(tz.toString(), wt.getFullYear(), wt.getMonth(), 1, 0, 0, 0, 0);
            }
            return dt;
        },
        round: function (dt, roundTo, tz) {
            if (tz.isUTC()) {
                var cur = dt.getUTCMonth();
                var adj = Math.floor(cur / roundTo) * roundTo;
                if (cur !== adj)
                    dt.setUTCMonth(adj);
            }
            else {
                var cur = dt.getMonth();
                var adj = Math.floor(cur / roundTo) * roundTo;
                if (cur !== adj)
                    return monthMove(dt, tz, adj - cur);
            }
            return dt;
        },
        move: monthMove
    });
    function yearMove(dt, tz, step) {
        if (tz.isUTC()) {
            dt = new Date(dt.valueOf());
            dt.setUTCFullYear(dt.getUTCFullYear() + step);
        }
        else {
            var wt = Chronoshift.WallTime.UTCToWallTime(dt, tz.toString());
            dt = Chronoshift.WallTime.WallTimeToUTC(tz.toString(), wt.getFullYear() + step, wt.getMonth(), wt.getDate(), wt.getHours(), wt.getMinutes(), wt.getSeconds(), wt.getMilliseconds());
        }
        return dt;
    }
    Chronoshift.year = timeMoverFiller({
        canonicalLength: 365 * 24 * 3600000,
        siblings: 1000,
        floor: function (dt, tz) {
            if (tz.isUTC()) {
                dt = new Date(dt.valueOf());
                dt.setUTCHours(0, 0, 0, 0);
                dt.setUTCMonth(0, 1);
            }
            else {
                var wt = Chronoshift.WallTime.UTCToWallTime(dt, tz.toString());
                dt = Chronoshift.WallTime.WallTimeToUTC(tz.toString(), wt.getFullYear(), 0, 1, 0, 0, 0, 0);
            }
            return dt;
        },
        round: function (dt, roundTo, tz) {
            if (tz.isUTC()) {
                var cur = dt.getUTCFullYear();
                var adj = Math.floor(cur / roundTo) * roundTo;
                if (cur !== adj)
                    dt.setUTCFullYear(adj);
            }
            else {
                var cur = dt.getFullYear();
                var adj = Math.floor(cur / roundTo) * roundTo;
                if (cur !== adj)
                    return yearMove(dt, tz, adj - cur);
            }
            return dt;
        },
        move: yearMove
    });
    Chronoshift.movers = {
        second: Chronoshift.second,
        minute: Chronoshift.minute,
        hour: Chronoshift.hour,
        day: Chronoshift.day,
        week: Chronoshift.week,
        month: Chronoshift.month,
        year: Chronoshift.year
    };
})(Chronoshift || (Chronoshift = {}));
var Chronoshift;
(function (Chronoshift) {
    var spansWithWeek = ["year", "month", "week", "day", "hour", "minute", "second"];
    var spansWithoutWeek = ["year", "month", "day", "hour", "minute", "second"];
    var spanMultiplicity = {};
    var periodWeekRegExp = /^P(\d+)W$/;
    var periodRegExp = /^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$/;
    //                   P   (year ) (month   ) (day     )    T(hour    ) (minute  ) (second  )
    function getSpansFromString(durationStr) {
        var spans = {};
        var matches;
        if (matches = periodWeekRegExp.exec(durationStr)) {
            spans.week = Number(matches[1]);
            if (!spans.week)
                throw new Error("Duration can not be empty");
        }
        else if (matches = periodRegExp.exec(durationStr)) {
            matches = matches.map(Number);
            for (var i = 0; i < spansWithoutWeek.length; i++) {
                var span = spansWithoutWeek[i];
                var value = matches[i + 1];
                if (value)
                    spans[span] = value;
            }
        }
        else {
            throw new Error("Can not parse duration '" + durationStr + "'");
        }
        return spans;
    }
    function getSpansFromStartEnd(start, end, timezone) {
        start = Chronoshift.second.floor(start, timezone);
        end = Chronoshift.second.floor(end, timezone);
        if (end <= start)
            throw new Error("start must come before end");
        var spans = {};
        var iterator = start;
        for (var i = 0; i < spansWithoutWeek.length; i++) {
            var span = spansWithoutWeek[i];
            var spanCount = 0;
            // Shortcut
            var length = end.valueOf() - iterator.valueOf();
            var canonicalLength = Chronoshift.movers[span].canonicalLength;
            if (length < canonicalLength / 4)
                continue;
            var numberToFit = Math.min(0, Math.floor(length / canonicalLength) - 1);
            var iteratorMove;
            if (numberToFit > 0) {
                // try to skip by numberToFit
                iteratorMove = Chronoshift.movers[span].move(iterator, timezone, numberToFit);
                if (iteratorMove <= end) {
                    spanCount += numberToFit;
                    iterator = iteratorMove;
                }
            }
            while (true) {
                iteratorMove = Chronoshift.movers[span].move(iterator, timezone, 1);
                if (iteratorMove <= end) {
                    iterator = iteratorMove;
                    spanCount++;
                }
                else {
                    break;
                }
            }
            if (spanCount) {
                spans[span] = spanCount;
            }
        }
        return spans;
    }
    function removeZeros(spans) {
        var newSpans = {};
        for (var i = 0; i < spansWithWeek.length; i++) {
            var span = spansWithWeek[i];
            if (spans[span] > 0) {
                newSpans[span] = spans[span];
            }
        }
        return newSpans;
    }
    /**
     * Represents an ISO duration like P1DT3H
     */
    var check;
    var Duration = (function () {
        function Duration(spans, end, timezone) {
            if (spans && end && timezone) {
                spans = getSpansFromStartEnd(spans, end, timezone);
            }
            else if (typeof spans === 'object') {
                spans = removeZeros(spans);
            }
            else {
                throw new Error("new Duration called with bad argument");
            }
            var usedSpans = Object.keys(spans);
            if (!usedSpans.length)
                throw new Error("Duration can not be empty");
            if (usedSpans.length === 1) {
                this.singleSpan = usedSpans[0];
            }
            else if (spans.week) {
                throw new Error("Can not mix 'week' and other spans");
            }
            this.spans = spans;
        }
        Duration.fromJS = function (durationStr) {
            if (typeof durationStr !== 'string')
                throw new TypeError("Duration JS must be a string");
            return new Duration(getSpansFromString(durationStr));
        };
        Duration.fromCanonicalLength = function (length) {
            var spans = {};
            for (var i = 0; i < spansWithWeek.length; i++) {
                var span = spansWithWeek[i];
                var spanLength = Chronoshift.movers[span].canonicalLength;
                var count = Math.floor(length / spanLength);
                length -= spanLength * count;
                spans[span] = count;
            }
            return new Duration(spans);
        };
        Duration.isDuration = function (candidate) {
            return Chronoshift.isInstanceOf(candidate, Duration);
        };
        Duration.prototype.toString = function () {
            var strArr = ["P"];
            var spans = this.spans;
            if (spans.week) {
                strArr.push(String(spans.week), 'W');
            }
            else {
                var addedT = false;
                for (var i = 0; i < spansWithoutWeek.length; i++) {
                    var span = spansWithoutWeek[i];
                    var value = spans[span];
                    if (!value)
                        continue;
                    if (!addedT && i >= 3) {
                        strArr.push("T");
                        addedT = true;
                    }
                    strArr.push(String(value), span[0].toUpperCase());
                }
            }
            return strArr.join("");
        };
        Duration.prototype.add = function (duration) {
            return Duration.fromCanonicalLength(this.getCanonicalLength() + duration.getCanonicalLength());
        };
        Duration.prototype.subtract = function (duration) {
            if (this.getCanonicalLength() - duration.getCanonicalLength() < 0) {
                throw new Error("A duration can not be negative.");
            }
            return Duration.fromCanonicalLength(this.getCanonicalLength() - duration.getCanonicalLength());
        };
        Duration.prototype.valueOf = function () {
            return this.spans;
        };
        Duration.prototype.toJS = function () {
            return this.toString();
        };
        Duration.prototype.toJSON = function () {
            return this.toString();
        };
        Duration.prototype.equals = function (other) {
            return Boolean(other) &&
                this.toString() === other.toString();
        };
        Duration.prototype.isSimple = function () {
            var singleSpan = this.singleSpan;
            if (!singleSpan)
                return false;
            return this.spans[singleSpan] === 1;
        };
        Duration.prototype.isFloorable = function () {
            var singleSpan = this.singleSpan;
            if (!singleSpan)
                return false;
            var span = this.spans[singleSpan];
            if (span === 1)
                return true;
            var siblings = Chronoshift.movers[singleSpan].siblings;
            if (!siblings)
                return false;
            return siblings % span === 0;
        };
        /**
         * Floors the date according to this duration.
         * @param date The date to floor
         * @param timezone The timezone within which to floor
         */
        Duration.prototype.floor = function (date, timezone) {
            var singleSpan = this.singleSpan;
            if (!singleSpan)
                throw new Error("Can not floor on a complex duration");
            var span = this.spans[singleSpan];
            var mover = Chronoshift.movers[singleSpan];
            var dt = mover.floor(date, timezone);
            if (span !== 1) {
                if (!mover.siblings)
                    throw new Error("Can not floor on a " + singleSpan + " duration that is not 1");
                if (mover.siblings % span !== 0)
                    throw new Error("Can not floor on a " + singleSpan + " duration that is not a multiple of " + span);
                dt = mover.round(dt, span, timezone);
            }
            return dt;
        };
        /**
         * Moves the given date by 'step' times of the duration
         * Negative step value will move back in time.
         * @param date The date to move
         * @param timezone The timezone within which to make the move
         * @param step The number of times to step by the duration
         */
        Duration.prototype.move = function (date, timezone, step) {
            if (step === void 0) { step = 1; }
            var spans = this.spans;
            for (var _i = 0; _i < spansWithWeek.length; _i++) {
                var span = spansWithWeek[_i];
                var value = spans[span];
                if (value)
                    date = Chronoshift.movers[span].move(date, timezone, step * value);
            }
            return date;
        };
        Duration.prototype.getCanonicalLength = function () {
            var spans = this.spans;
            var length = 0;
            for (var _i = 0; _i < spansWithWeek.length; _i++) {
                var span = spansWithWeek[_i];
                var value = spans[span];
                if (value)
                    length += value * Chronoshift.movers[span].canonicalLength;
            }
            return length;
        };
        Duration.prototype.canonicalLength = function () {
            // This method is deprecated
            console.warn("The method 'canonicalLength()' is deprecated. Please use 'getCanonicalLength()' instead.");
            return this.getCanonicalLength();
        };
        Duration.prototype.getDescription = function () {
            var spans = this.spans;
            var description = [];
            for (var _i = 0; _i < spansWithWeek.length; _i++) {
                var span = spansWithWeek[_i];
                var value = spans[span];
                if (value) {
                    if (value === 1) {
                        description.push(span);
                    }
                    else {
                        description.push(String(value) + ' ' + span + 's');
                    }
                }
            }
            return description.join(', ');
        };
        return Duration;
    })();
    Chronoshift.Duration = Duration;
    check = Duration;
})(Chronoshift || (Chronoshift = {}));
if (typeof module !== 'undefined' && typeof module.exports !== 'undefined') {
    module.exports = Chronoshift;
}

},{"../lib/walltime":4,"immutable-class":5}],4:[function(require,module,exports){
/*
 *  WallTime 0.1.2
 *  Copyright (c) 2013 Sprout Social, Inc.
 *  Available under the MIT License (http://bit.ly/walltime-license)
 */

(function() {
  var Days, Milliseconds, Months, Time, helpers, _base;

  (_base = Array.prototype).indexOf || (_base.indexOf = function(item) {
    var i, x, _i, _len;
    for (i = _i = 0, _len = this.length; _i < _len; i = ++_i) {
      x = this[i];
      if (x === item) {
        return i;
      }
    }
    return -1;
  });

  Days = {
    DayShortNames: ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
    DayIndex: function(name) {
      return this.DayShortNames.indexOf(name);
    },
    DayNameFromIndex: function(dayIdx) {
      return this.DayShortNames[dayIdx];
    },
    AddToDate: function(dt, days) {
      return Time.MakeDateFromTimeStamp(dt.getTime() + (days * Milliseconds.inDay));
    }
  };

  Months = {
    MonthsShortNames: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
    CompareRuleMatch: new RegExp("([a-zA-Z]*)([\\<\\>]?=)([0-9]*)"),
    MonthIndex: function(shortName) {
      return this.MonthsShortNames.indexOf(shortName.slice(0, 3));
    },
    IsDayOfMonthRule: function(str) {
      return str.indexOf(">") > -1 || str.indexOf("<") > -1 || str.indexOf("=") > -1;
    },
    IsLastDayOfMonthRule: function(str) {
      return str.slice(0, 4) === "last";
    },
    DayOfMonthByRule: function(str, year, month) {
      var compareFunc, compares, dateIndex, dayIndex, dayName, ruleParse, testDate, testPart, _ref;
      ruleParse = this.CompareRuleMatch.exec(str);
      if (!ruleParse) {
        throw new Error("Unable to parse the 'on' rule for " + str);
      }
      _ref = ruleParse.slice(1, 4), dayName = _ref[0], testPart = _ref[1], dateIndex = _ref[2];
      dateIndex = parseInt(dateIndex, 10);
      if (dateIndex === NaN) {
        throw new Error("Unable to parse the dateIndex of the 'on' rule for " + str);
      }
      dayIndex = helpers.Days.DayIndex(dayName);
      compares = {
        ">=": function(a, b) {
          return a >= b;
        },
        "<=": function(a, b) {
          return a <= b;
        },
        ">": function(a, b) {
          return a > b;
        },
        "<": function(a, b) {
          return a < b;
        },
        "=": function(a, b) {
          return a === b;
        }
      };
      compareFunc = compares[testPart];
      if (!compareFunc) {
        throw new Error("Unable to parse the conditional for " + testPart);
      }
      testDate = helpers.Time.MakeDateFromParts(year, month);
      while (!(dayIndex === testDate.getUTCDay() && compareFunc(testDate.getUTCDate(), dateIndex))) {
        testDate = helpers.Days.AddToDate(testDate, 1);
      }
      return testDate.getUTCDate();
    },
    LastDayOfMonthRule: function(str, year, month) {
      var dayIndex, dayName, lastDay;
      dayName = str.slice(4);
      dayIndex = helpers.Days.DayIndex(dayName);
      if (month < 11) {
        lastDay = helpers.Time.MakeDateFromParts(year, month + 1);
      } else {
        lastDay = helpers.Time.MakeDateFromParts(year + 1, 0);
      }
      lastDay = helpers.Days.AddToDate(lastDay, -1);
      while (lastDay.getUTCDay() !== dayIndex) {
        lastDay = helpers.Days.AddToDate(lastDay, -1);
      }
      return lastDay.getUTCDate();
    }
  };

  Milliseconds = {
    inDay: 86400000,
    inHour: 3600000,
    inMinute: 60000,
    inSecond: 1000
  };

  Time = {
    Add: function(dt, hours, mins, secs) {
      var newTs;
      if (hours == null) {
        hours = 0;
      }
      if (mins == null) {
        mins = 0;
      }
      if (secs == null) {
        secs = 0;
      }
      newTs = dt.getTime() + (hours * Milliseconds.inHour) + (mins * Milliseconds.inMinute) + (secs * Milliseconds.inSecond);
      return this.MakeDateFromTimeStamp(newTs);
    },
    ParseGMTOffset: function(str) {
      var isNeg, match, matches, reg, result;
      reg = new RegExp("(-)?([0-9]*):([0-9]*):?([0-9]*)?");
      matches = reg.exec(str);
      result = matches ? (function() {
        var _i, _len, _ref, _results;
        _ref = matches.slice(2);
        _results = [];
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          match = _ref[_i];
          _results.push(parseInt(match, 10));
        }
        return _results;
      })() : [0, 0, 0];
      isNeg = matches && matches[1] === "-";
      result.splice(0, 0, isNeg);
      return result;
    },
    ParseTime: function(str) {
      var match, matches, qual, reg, timeParts;
      reg = new RegExp("(\\d*)\\:(\\d*)([wsugz]?)");
      matches = reg.exec(str);
      if (!matches) {
        return [0, 0, ''];
      }
      timeParts = (function() {
        var _i, _len, _ref, _results;
        _ref = matches.slice(1, 3);
        _results = [];
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          match = _ref[_i];
          _results.push(parseInt(match, 10));
        }
        return _results;
      })();
      qual = matches[3] ? matches[3] : '';
      timeParts.push(qual);
      return timeParts;
    },
    ApplyOffset: function(dt, offset, reverse) {
      var offset_ms;
      offset_ms = (Milliseconds.inHour * offset.hours) + (Milliseconds.inMinute * offset.mins) + (Milliseconds.inSecond * offset.secs);
      if (!offset.negative) {
        offset_ms = offset_ms * -1;
      }
      if (reverse) {
        offset_ms = offset_ms * -1;
      }
      return this.MakeDateFromTimeStamp(dt.getTime() + offset_ms);
    },
    ApplySave: function(dt, save, reverse) {
      if (reverse !== true) {
        reverse = false;
      }
      return this.ApplyOffset(dt, {
        negative: true,
        hours: save.hours,
        mins: save.mins,
        secs: 0
      }, reverse);
    },
    UTCToWallTime: function(dt, offset, save) {
      var endTime;
      endTime = this.UTCToStandardTime(dt, offset);
      return this.ApplySave(endTime, save);
    },
    UTCToStandardTime: function(dt, offset) {
      return this.ApplyOffset(dt, offset, true);
    },
    UTCToQualifiedTime: function(dt, qualifier, offset, getSave) {
      var endTime;
      endTime = dt;
      switch (qualifier) {
        case "w":
          endTime = this.UTCToWallTime(endTime, offset, getSave());
          break;
        case "s":
          endTime = this.UTCToStandardTime(endTime, offset);
          break;
      }
      return endTime;
    },
    QualifiedTimeToUTC: function(dt, qualifier, offset, getSave) {
      var endTime;
      endTime = dt;
      switch (qualifier) {
        case "w":
          endTime = this.WallTimeToUTC(offset, getSave(), endTime);
          break;
        case "s":
          endTime = this.StandardTimeToUTC(offset, endTime);
          break;
      }
      return endTime;
    },
    StandardTimeToUTC: function(offset, y, m, d, h, mi, s, ms) {
      var dt;
      if (m == null) {
        m = 0;
      }
      if (d == null) {
        d = 1;
      }
      if (h == null) {
        h = 0;
      }
      if (mi == null) {
        mi = 0;
      }
      if (s == null) {
        s = 0;
      }
      if (ms == null) {
        ms = 0;
      }
      dt = typeof y === "number" ? this.MakeDateFromParts(y, m, d, h, mi, s, ms) : y;
      return this.ApplyOffset(dt, offset);
    },
    WallTimeToUTC: function(offset, save, y, m, d, h, mi, s, ms) {
      var dt;
      if (m == null) {
        m = 0;
      }
      if (d == null) {
        d = 1;
      }
      if (h == null) {
        h = 0;
      }
      if (mi == null) {
        mi = 0;
      }
      if (s == null) {
        s = 0;
      }
      if (ms == null) {
        ms = 0;
      }
      dt = this.StandardTimeToUTC(offset, y, m, d, h, mi, s, ms);
      return this.ApplySave(dt, save, true);
    },
    MakeDateFromParts: function(y, m, d, h, mi, s, ms) {
      var dt;
      if (m == null) {
        m = 0;
      }
      if (d == null) {
        d = 1;
      }
      if (h == null) {
        h = 0;
      }
      if (mi == null) {
        mi = 0;
      }
      if (s == null) {
        s = 0;
      }
      if (ms == null) {
        ms = 0;
      }
      if (Date.UTC) {
        return new Date(Date.UTC(y, m, d, h, mi, s, ms));
      }
      dt = new Date;
      dt.setUTCFullYear(y);
      dt.setUTCMonth(m);
      dt.setUTCDate(d);
      dt.setUTCHours(h);
      dt.setUTCMinutes(mi);
      dt.setUTCSeconds(s);
      dt.setUTCMilliseconds(ms);
      return dt;
    },
    LocalDate: function(offset, save, y, m, d, h, mi, s, ms) {
      if (m == null) {
        m = 0;
      }
      if (d == null) {
        d = 1;
      }
      if (h == null) {
        h = 0;
      }
      if (mi == null) {
        mi = 0;
      }
      if (s == null) {
        s = 0;
      }
      if (ms == null) {
        ms = 0;
      }
      return this.WallTimeToUTC(offset, save, y, m, d, h, mi, s, ms);
    },
    MakeDateFromTimeStamp: function(ts) {
      return new Date(ts);
    },
    MaxDate: function() {
      return this.MakeDateFromTimeStamp(10000000 * 86400000);
    },
    MinDate: function() {
      return this.MakeDateFromTimeStamp(-10000000 * 86400000);
    }
  };

  helpers = {
    Days: Days,
    Months: Months,
    Milliseconds: Milliseconds,
    Time: Time,
    noSave: {
      hours: 0,
      mins: 0
    },
    noZone: {
      offset: {
        negative: false,
        hours: 0,
        mins: 0,
        secs: 0
      },
      name: "UTC"
    }
  };

  this.WallTime || (this.WallTime = {});
  this.WallTime.helpers = helpers;

}).call(this);

(function() {
  var init, req_helpers;

  init = function(helpers) {
    var TimeZoneTime;
    TimeZoneTime = (function() {
      function TimeZoneTime(utc, zone, save) {
        this.utc = utc;
        this.zone = zone;
        this.save = save;
        this.offset = this.zone.offset;
        this.wallTime = helpers.Time.UTCToWallTime(this.utc, this.offset, this.save);
      }

      TimeZoneTime.prototype.getFullYear = function() {
        return this.wallTime.getUTCFullYear();
      };

      TimeZoneTime.prototype.getMonth = function() {
        return this.wallTime.getUTCMonth();
      };

      TimeZoneTime.prototype.getDate = function() {
        return this.wallTime.getUTCDate();
      };

      TimeZoneTime.prototype.getDay = function() {
        return this.wallTime.getUTCDay();
      };

      TimeZoneTime.prototype.getHours = function() {
        return this.wallTime.getUTCHours();
      };

      TimeZoneTime.prototype.getMinutes = function() {
        return this.wallTime.getUTCMinutes();
      };

      TimeZoneTime.prototype.getSeconds = function() {
        return this.wallTime.getUTCSeconds();
      };

      TimeZoneTime.prototype.getMilliseconds = function() {
        return this.wallTime.getUTCMilliseconds();
      };

      TimeZoneTime.prototype.getUTCFullYear = function() {
        return this.utc.getUTCFullYear();
      };

      TimeZoneTime.prototype.getUTCMonth = function() {
        return this.utc.getUTCMonth();
      };

      TimeZoneTime.prototype.getUTCDate = function() {
        return this.utc.getUTCDate();
      };

      TimeZoneTime.prototype.getUTCDay = function() {
        return this.utc.getUTCDay();
      };

      TimeZoneTime.prototype.getUTCHours = function() {
        return this.utc.getUTCHours();
      };

      TimeZoneTime.prototype.getUTCMinutes = function() {
        return this.utc.getUTCMinutes();
      };

      TimeZoneTime.prototype.getUTCSeconds = function() {
        return this.utc.getUTCSeconds();
      };

      TimeZoneTime.prototype.getUTCMilliseconds = function() {
        return this.utc.getUTCMilliseconds();
      };

      TimeZoneTime.prototype.getTime = function() {
        return this.utc.getTime();
      };

      TimeZoneTime.prototype.getTimezoneOffset = function() {
        var base, dst;
        base = (this.offset.hours * 60) + this.offset.mins;
        dst = (this.save.hours * 60) + this.save.mins;
        if (!this.offset.negative) {
          base = -base;
        }
        return base - dst;
      };

      TimeZoneTime.prototype.toISOString = function() {
        return this.utc.toISOString();
      };

      TimeZoneTime.prototype.toUTCString = function() {
        return this.wallTime.toUTCString();
      };

      TimeZoneTime.prototype.toDateString = function() {
        var caps, utcStr;
        utcStr = this.wallTime.toUTCString();
        caps = utcStr.match("([a-zA-Z]*), ([0-9]+) ([a-zA-Z]*) ([0-9]+)");
        return [caps[1], caps[3], caps[2], caps[4]].join(" ");
      };

      TimeZoneTime.prototype.toFormattedTime = function(use24HourTime) {
        var hour, meridiem, min, origHour;
        if (use24HourTime == null) {
          use24HourTime = false;
        }
        hour = origHour = this.getHours();
        if (hour > 12 && !use24HourTime) {
          hour -= 12;
        }
        if (hour === 0) {
          hour = 12;
        }
        min = this.getMinutes();
        if (min < 10) {
          min = "0" + min;
        }
        meridiem = origHour > 11 ? ' PM' : ' AM';
        if (use24HourTime) {
          meridiem = '';
        }
        return "" + hour + ":" + min + meridiem;
      };

      TimeZoneTime.prototype.setTime = function(ms) {
        this.wallTime = helpers.Time.UTCToWallTime(new Date(ms), this.zone.offset, this.save);
        return this._updateUTC();
      };

      TimeZoneTime.prototype.setFullYear = function(y) {
        this.wallTime.setUTCFullYear(y);
        return this._updateUTC();
      };

      TimeZoneTime.prototype.setMonth = function(m) {
        this.wallTime.setUTCMonth(m);
        return this._updateUTC();
      };

      TimeZoneTime.prototype.setDate = function(utcDate) {
        this.wallTime.setUTCDate(utcDate);
        return this._updateUTC();
      };

      TimeZoneTime.prototype.setHours = function(hours) {
        this.wallTime.setUTCHours(hours);
        return this._updateUTC();
      };

      TimeZoneTime.prototype.setMinutes = function(m) {
        this.wallTime.setUTCMinutes(m);
        return this._updateUTC();
      };

      TimeZoneTime.prototype.setSeconds = function(s) {
        this.wallTime.setUTCSeconds(s);
        return this._updateUTC();
      };

      TimeZoneTime.prototype.setMilliseconds = function(ms) {
        this.wallTime.setUTCMilliseconds(ms);
        return this._updateUTC();
      };

      TimeZoneTime.prototype._updateUTC = function() {
        this.utc = helpers.Time.WallTimeToUTC(this.offset, this.save, this.getFullYear(), this.getMonth(), this.getDate(), this.getHours(), this.getMinutes(), this.getSeconds(), this.getMilliseconds());
        return this.utc.getTime();
      };

      return TimeZoneTime;

    })();
    return TimeZoneTime;
  };

  this.WallTime || (this.WallTime = {});
  this.WallTime.TimeZoneTime = init(this.WallTime.helpers);

}).call(this);

(function() {
  var init, req_TimeZoneTime, req_helpers,
    __hasProp = {}.hasOwnProperty;

  init = function(helpers, TimeZoneTime) {
    var CompareOnFieldHandler, LastOnFieldHandler, NumberOnFieldHandler, Rule, RuleSet, lib;
    NumberOnFieldHandler = (function() {
      function NumberOnFieldHandler() {}

      NumberOnFieldHandler.prototype.applies = function(str) {
        return !isNaN(parseInt(str, 10));
      };

      NumberOnFieldHandler.prototype.parseDate = function(str) {
        return parseInt(str, 10);
      };

      return NumberOnFieldHandler;

    })();
    LastOnFieldHandler = (function() {
      function LastOnFieldHandler() {}

      LastOnFieldHandler.prototype.applies = helpers.Months.IsLastDayOfMonthRule;

      LastOnFieldHandler.prototype.parseDate = function(str, year, month, qualifier, gmtOffset, daylightOffset) {
        return helpers.Months.LastDayOfMonthRule(str, year, month);
      };

      return LastOnFieldHandler;

    })();
    CompareOnFieldHandler = (function() {
      function CompareOnFieldHandler() {}

      CompareOnFieldHandler.prototype.applies = helpers.Months.IsDayOfMonthRule;

      CompareOnFieldHandler.prototype.parseDate = function(str, year, month) {
        return helpers.Months.DayOfMonthByRule(str, year, month);
      };

      return CompareOnFieldHandler;

    })();
    Rule = (function() {
      function Rule(name, _from, _to, type, _in, on, at, _save, letter) {
        var saveHour, saveMinute, toYear, _ref;
        this.name = name;
        this._from = _from;
        this._to = _to;
        this.type = type;
        this["in"] = _in;
        this.on = on;
        this.at = at;
        this._save = _save;
        this.letter = letter;
        this.from = parseInt(this._from, 10);
        this.isMax = false;
        toYear = this.from;
        switch (this._to) {
          case "max":
            toYear = (helpers.Time.MaxDate()).getUTCFullYear();
            this.isMax = true;
            break;
          case "only":
            toYear = this.from;
            break;
          default:
            toYear = parseInt(this._to, 10);
        }
        this.to = toYear;
        _ref = this._parseTime(this._save), saveHour = _ref[0], saveMinute = _ref[1];
        this.save = {
          hours: saveHour,
          mins: saveMinute
        };
      }

      Rule.prototype.forZone = function(offset) {
        this.offset = offset;
        this.fromUTC = helpers.Time.MakeDateFromParts(this.from, 0, 1, 0, 0, 0);
        this.fromUTC = helpers.Time.ApplyOffset(this.fromUTC, offset);
        this.toUTC = helpers.Time.MakeDateFromParts(this.to, 11, 31, 23, 59, 59, 999);
        return this.toUTC = helpers.Time.ApplyOffset(this.toUTC, offset);
      };

      Rule.prototype.setOnUTC = function(year, offset, getPrevSave) {
        var atQualifier, onParsed, toDay, toHour, toMinute, toMonth, _ref,
          _this = this;
        toMonth = helpers.Months.MonthIndex(this["in"]);
        onParsed = parseInt(this.on, 10);
        toDay = !isNaN(onParsed) ? onParsed : this._parseOnDay(this.on, year, toMonth);
        _ref = this._parseTime(this.at), toHour = _ref[0], toMinute = _ref[1], atQualifier = _ref[2];
        this.onUTC = helpers.Time.MakeDateFromParts(year, toMonth, toDay, toHour, toMinute);
        this.onUTC.setUTCMilliseconds(this.onUTC.getUTCMilliseconds() - 1);
        this.atQualifier = atQualifier !== '' ? atQualifier : "w";
        this.onUTC = helpers.Time.QualifiedTimeToUTC(this.onUTC, this.atQualifier, offset, function() {
          return getPrevSave(_this);
        });
        return this.onSort = "" + toMonth + "-" + toDay + "-" + (this.onUTC.getUTCHours()) + "-" + (this.onUTC.getUTCMinutes());
      };

      Rule.prototype.appliesToUTC = function(dt) {
        return (this.fromUTC <= dt && dt <= this.toUTC);
      };

      Rule.prototype._parseOnDay = function(onStr, year, month) {
        var handler, handlers, _i, _len;
        handlers = [new NumberOnFieldHandler, new LastOnFieldHandler, new CompareOnFieldHandler];
        for (_i = 0, _len = handlers.length; _i < _len; _i++) {
          handler = handlers[_i];
          if (!handler.applies(onStr)) {
            continue;
          }
          return handler.parseDate(onStr, year, month);
        }
        throw new Error("Unable to parse 'on' field for " + this.name + "|" + this._from + "|" + this._to + "|" + onStr);
      };

      Rule.prototype._parseTime = function(atStr) {
        return helpers.Time.ParseTime(atStr);
      };

      return Rule;

    })();
    RuleSet = (function() {
      function RuleSet(rules, timeZone) {
        var beginYears, commonUpdateYearEnds, endYears, max, min, rule, _i, _len, _ref,
          _this = this;
        this.rules = rules != null ? rules : [];
        this.timeZone = timeZone;
        min = null;
        max = null;
        endYears = {};
        beginYears = {};
        _ref = this.rules;
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          rule = _ref[_i];
          rule.forZone(this.timeZone.offset, function() {
            return helpers.noSave;
          });
          if (min === null || rule.from < min) {
            min = rule.from;
          }
          if (max === null || rule.to > max) {
            max = rule.to;
          }
          endYears[rule.to] = endYears[rule.to] || [];
          endYears[rule.to].push(rule);
          beginYears[rule.from] = beginYears[rule.from] || [];
          beginYears[rule.from].push(rule);
        }
        this.minYear = min;
        this.maxYear = max;
        commonUpdateYearEnds = function(end, years) {
          var lastRule, year, yearRules, _results;
          if (end == null) {
            end = "toUTC";
          }
          if (years == null) {
            years = endYears;
          }
          _results = [];
          for (year in years) {
            if (!__hasProp.call(years, year)) continue;
            rules = years[year];
            yearRules = _this.allThatAppliesTo(rules[0][end]);
            if (yearRules.length < 1) {
              continue;
            }
            rules = _this._sortRulesByOnTime(rules);
            lastRule = yearRules.slice(-1)[0];
            if (lastRule.save.hours === 0 && lastRule.save.mins === 0) {
              continue;
            }
            _results.push((function() {
              var _j, _len1, _results1;
              _results1 = [];
              for (_j = 0, _len1 = rules.length; _j < _len1; _j++) {
                rule = rules[_j];
                _results1.push(rule[end] = helpers.Time.ApplySave(rule[end], lastRule.save));
              }
              return _results1;
            })());
          }
          return _results;
        };
        commonUpdateYearEnds("toUTC", endYears);
        commonUpdateYearEnds("fromUTC", beginYears);
      }

      RuleSet.prototype.allThatAppliesTo = function(dt) {
        var rule, _i, _len, _ref, _results;
        _ref = this.rules;
        _results = [];
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          rule = _ref[_i];
          if (rule.appliesToUTC(dt)) {
            _results.push(rule);
          }
        }
        return _results;
      };

      RuleSet.prototype.getWallTimeForUTC = function(dt) {
        var appliedRules, getPrevRuleSave, lastSave, rule, rules, _i, _len;
        rules = this.allThatAppliesTo(dt);
        if (rules.length < 1) {
          return new TimeZoneTime(dt, this.timeZone, helpers.noSave);
        }
        rules = this._sortRulesByOnTime(rules);
        getPrevRuleSave = function(r) {
          var idx;
          idx = rules.indexOf(r);
          if (idx < 1) {
            if (rules.length < 1) {
              return helpers.noSave;
            }
            return rules.slice(-1)[0].save;
          }
          return rules[idx - 1].save;
        };
        for (_i = 0, _len = rules.length; _i < _len; _i++) {
          rule = rules[_i];
          rule.setOnUTC(dt.getUTCFullYear(), this.timeZone.offset, getPrevRuleSave);
        }
        appliedRules = (function() {
          var _j, _len1, _results;
          _results = [];
          for (_j = 0, _len1 = rules.length; _j < _len1; _j++) {
            rule = rules[_j];
            if (rule.onUTC.getTime() < dt.getTime()) {
              _results.push(rule);
            }
          }
          return _results;
        })();
        lastSave = rules.length < 1 ? helpers.noSave : rules.slice(-1)[0].save;
        if (appliedRules.length > 0) {
          lastSave = appliedRules.slice(-1)[0].save;
        }
        return new TimeZoneTime(dt, this.timeZone, lastSave);
      };

      RuleSet.prototype.getUTCForWallTime = function(dt) {
        var appliedRules, getPrevRuleSave, lastSave, rule, rules, utcStd, _i, _len;
        utcStd = helpers.Time.StandardTimeToUTC(this.timeZone.offset, dt);
        rules = (function() {
          var _i, _len, _ref, _results;
          _ref = this.rules;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            rule = _ref[_i];
            if (rule.appliesToUTC(utcStd)) {
              _results.push(rule);
            }
          }
          return _results;
        }).call(this);
        if (rules.length < 1) {
          return utcStd;
        }
        rules = this._sortRulesByOnTime(rules);
        getPrevRuleSave = function(r) {
          var idx;
          idx = rules.indexOf(r);
          if (idx < 1) {
            if (rules.length < 1) {
              return helpers.noSave;
            }
            return rules.slice(-1)[0].save;
          }
          return rules[idx - 1].save;
        };
        for (_i = 0, _len = rules.length; _i < _len; _i++) {
          rule = rules[_i];
          rule.setOnUTC(utcStd.getUTCFullYear(), this.timeZone.offset, getPrevRuleSave);
        }
        appliedRules = (function() {
          var _j, _len1, _results;
          _results = [];
          for (_j = 0, _len1 = rules.length; _j < _len1; _j++) {
            rule = rules[_j];
            if (rule.onUTC.getTime() < utcStd.getTime()) {
              _results.push(rule);
            }
          }
          return _results;
        })();
        lastSave = rules.length < 1 ? helpers.noSave : rules.slice(-1)[0].save;
        if (appliedRules.length > 0) {
          lastSave = appliedRules.slice(-1)[0].save;
        }
        return helpers.Time.WallTimeToUTC(this.timeZone.offset, lastSave, dt);
      };

      RuleSet.prototype.getYearEndDST = function(dt) {
        var appliedRules, getPrevRuleSave, lastSave, rule, rules, utcStd, year, _i, _len;
        year = typeof dt === number ? dt : dt.getUTCFullYear();
        utcStd = helpers.Time.StandardTimeToUTC(this.timeZone.offset, year, 11, 31, 23, 59, 59);
        rules = (function() {
          var _i, _len, _ref, _results;
          _ref = this.rules;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            rule = _ref[_i];
            if (rule.appliesToUTC(utcStd)) {
              _results.push(rule);
            }
          }
          return _results;
        }).call(this);
        if (rules.length < 1) {
          return helpers.noSave;
        }
        rules = this._sortRulesByOnTime(rules);
        getPrevRuleSave = function(r) {
          var idx;
          idx = rules.indexOf(r);
          if (idx < 1) {
            return helpers.noSave;
          }
          return rules[idx - 1].save;
        };
        for (_i = 0, _len = rules.length; _i < _len; _i++) {
          rule = rules[_i];
          rule.setOnUTC(utcStd.getUTCFullYear(), this.timeZone.offset, getPrevRuleSave);
        }
        appliedRules = (function() {
          var _j, _len1, _results;
          _results = [];
          for (_j = 0, _len1 = rules.length; _j < _len1; _j++) {
            rule = rules[_j];
            if (rule.onUTC.getTime() < utcStd.getTime()) {
              _results.push(rule);
            }
          }
          return _results;
        })();
        lastSave = helpers.noSave;
        if (appliedRules.length > 0) {
          lastSave = appliedRules.slice(-1)[0].save;
        }
        return lastSave;
      };

      RuleSet.prototype.isAmbiguous = function(dt) {
        var appliedRules, getPrevRuleSave, lastRule, makeAmbigRange, minsOff, prevSave, range, rule, rules, springForward, totalMinutes, utcStd, _i, _len;
        utcStd = helpers.Time.StandardTimeToUTC(this.timeZone.offset, dt);
        rules = (function() {
          var _i, _len, _ref, _results;
          _ref = this.rules;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            rule = _ref[_i];
            if (rule.appliesToUTC(utcStd)) {
              _results.push(rule);
            }
          }
          return _results;
        }).call(this);
        if (rules.length < 1) {
          return false;
        }
        rules = this._sortRulesByOnTime(rules);
        getPrevRuleSave = function(r) {
          var idx;
          idx = rules.indexOf(r);
          if (idx < 1) {
            return helpers.noSave;
          }
          return rules[idx - 1].save;
        };
        for (_i = 0, _len = rules.length; _i < _len; _i++) {
          rule = rules[_i];
          rule.setOnUTC(utcStd.getUTCFullYear(), this.timeZone.offset, getPrevRuleSave);
        }
        appliedRules = (function() {
          var _j, _len1, _results;
          _results = [];
          for (_j = 0, _len1 = rules.length; _j < _len1; _j++) {
            rule = rules[_j];
            if (rule.onUTC.getTime() <= utcStd.getTime() - 1) {
              _results.push(rule);
            }
          }
          return _results;
        })();
        if (appliedRules.length < 1) {
          return false;
        }
        lastRule = appliedRules.slice(-1)[0];
        prevSave = getPrevRuleSave(lastRule);
        totalMinutes = {
          prev: (prevSave.hours * 60) + prevSave.mins,
          last: (lastRule.save.hours * 60) + lastRule.save.mins
        };
        if (totalMinutes.prev === totalMinutes.last) {
          return false;
        }
        springForward = totalMinutes.prev < totalMinutes.last;
        makeAmbigRange = function(begin, minutesOff) {
          var ambigRange, tmp;
          ambigRange = {
            begin: helpers.Time.MakeDateFromTimeStamp(begin.getTime() + 1)
          };
          ambigRange.end = helpers.Time.Add(ambigRange.begin, 0, minutesOff);
          if (ambigRange.begin.getTime() > ambigRange.end.getTime()) {
            tmp = ambigRange.begin;
            ambigRange.begin = ambigRange.end;
            ambigRange.end = tmp;
          }
          return ambigRange;
        };
        minsOff = springForward ? totalMinutes.last : -totalMinutes.prev;
        range = makeAmbigRange(lastRule.onUTC, minsOff);
        utcStd = helpers.Time.WallTimeToUTC(this.timeZone.offset, prevSave, dt);
        return (range.begin <= utcStd && utcStd <= range.end);
      };

      RuleSet.prototype._sortRulesByOnTime = function(rules) {
        return rules.sort(function(a, b) {
          return (helpers.Months.MonthIndex(a["in"])) - (helpers.Months.MonthIndex(b["in"]));
        });
      };

      return RuleSet;

    })();
    lib = {
      Rule: Rule,
      RuleSet: RuleSet,
      OnFieldHandlers: {
        NumberHandler: NumberOnFieldHandler,
        LastHandler: LastOnFieldHandler,
        CompareHandler: CompareOnFieldHandler
      }
    };
    return lib;
  };

  this.WallTime || (this.WallTime = {});
  this.WallTime.rule = init(this.WallTime.helpers, this.WallTime.TimeZoneTime);

}).call(this);

(function() {
  var init, req_TimeZoneTime, req_helpers, req_rule;

  init = function(helpers, rule, TimeZoneTime) {
    var Zone, ZoneSet, lib;
    Zone = (function() {
      function Zone(name, _offset, _rule, format, _until, currZone) {
        var begin, isNegative, offsetHours, offsetMins, offsetSecs, _ref;
        this.name = name;
        this._offset = _offset;
        this._rule = _rule;
        this.format = format;
        this._until = _until;
        _ref = helpers.Time.ParseGMTOffset(this._offset), isNegative = _ref[0], offsetHours = _ref[1], offsetMins = _ref[2], offsetSecs = _ref[3];
        this.offset = {
          negative: isNegative,
          hours: offsetHours,
          mins: offsetMins,
          secs: isNaN(offsetSecs) ? 0 : offsetSecs
        };
        begin = currZone ? helpers.Time.MakeDateFromTimeStamp(currZone.range.end.getTime() + 1) : helpers.Time.MinDate();
        this.range = {
          begin: begin,
          end: this._parseUntilDate(this._until)
        };
      }

      Zone.prototype._parseUntilDate = function(til) {
        var day, endTime, h, mi, month, monthName, neg, s, standardTime, time, year, _ref, _ref1;
        _ref = til.split(" "), year = _ref[0], monthName = _ref[1], day = _ref[2], time = _ref[3];
        _ref1 = time ? helpers.Time.ParseGMTOffset(time) : [false, 0, 0, 0], neg = _ref1[0], h = _ref1[1], mi = _ref1[2], s = _ref1[3];
        s = isNaN(s) ? 0 : s;
        if (!year || year === "") {
          return helpers.Time.MaxDate();
        }
        year = parseInt(year, 10);
        month = monthName ? helpers.Months.MonthIndex(monthName) : 0;
        day || (day = "1");
        if (helpers.Months.IsDayOfMonthRule(day)) {
          day = helpers.Months.DayOfMonthByRule(day, year, month);
        } else if (helpers.Months.IsLastDayOfMonthRule(day)) {
          day = helpers.Months.LastDayOfMonthRule(day, year, month);
        } else {
          day = parseInt(day, 10);
        }
        standardTime = helpers.Time.StandardTimeToUTC(this.offset, year, month, day, h, mi, s);
        endTime = helpers.Time.MakeDateFromTimeStamp(standardTime.getTime() - 1);
        return endTime;
      };

      Zone.prototype.updateEndForRules = function(getRulesNamed) {
        var endSave, hours, mins, rules, _ref;
        if (this._rule === "-" || this._rule === "") {
          return;
        }
        if (this._rule.indexOf(":") >= 0) {
          _ref = helpers.Time.ParseTime(this._rule), hours = _ref[0], mins = _ref[1];
          this.range.end = helpers.Time.ApplySave(this.range.end, {
            hours: hours,
            mins: mins
          });
        }
        rules = new rule.RuleSet(getRulesNamed(this._rule), this);
        endSave = rules.getYearEndDST(this.range.end);
        return this.range.end = helpers.Time.ApplySave(this.range.end, endSave);
      };

      Zone.prototype.UTCToWallTime = function(dt, getRulesNamed) {
        var hours, mins, rules, _ref;
        if (this._rule === "-" || this._rule === "") {
          return new TimeZoneTime(dt, this, helpers.noSave);
        }
        if (this._rule.indexOf(":") >= 0) {
          _ref = helpers.Time.ParseTime(this._rule), hours = _ref[0], mins = _ref[1];
          return new TimeZoneTime(dt, this, {
            hours: hours,
            mins: mins
          });
        }
        rules = new rule.RuleSet(getRulesNamed(this._rule), this);
        return rules.getWallTimeForUTC(dt);
      };

      Zone.prototype.WallTimeToUTC = function(dt, getRulesNamed) {
        var hours, mins, rules, _ref;
        if (this._rule === "-" || this._rule === "") {
          return helpers.Time.StandardTimeToUTC(this.offset, dt);
        }
        if (this._rule.indexOf(":") >= 0) {
          _ref = helpers.Time.ParseTime(this._rule), hours = _ref[0], mins = _ref[1];
          return helpers.Time.WallTimeToUTC(this.offset, {
            hours: hours,
            mins: mins
          }, dt);
        }
        rules = new rule.RuleSet(getRulesNamed(this._rule), this);
        return rules.getUTCForWallTime(dt, this.offset);
      };

      Zone.prototype.IsAmbiguous = function(dt, getRulesNamed) {
        var ambigCheck, hours, makeAmbigZone, mins, rules, utcDt, _ref, _ref1, _ref2;
        if (this._rule === "-" || this._rule === "") {
          return false;
        }
        if (this._rule.indexOf(":") >= 0) {
          utcDt = helpers.Time.StandardTimeToUTC(this.offset, dt);
          _ref = helpers.Time.ParseTime(this._rule), hours = _ref[0], mins = _ref[1];
          makeAmbigZone = function(begin) {
            var ambigZone, tmp;
            ambigZone = {
              begin: this.range.begin,
              end: helpers.Time.ApplySave(this.range.begin, {
                hours: hours,
                mins: mins
              })
            };
            if (ambigZone.end.getTime() < ambigZone.begin.getTime()) {
              tmp = ambigZone.begin;
              ambigZone.begin = ambigZone.end;
              ambigZone.end = tmp;
            }
            return ambigZone;
          };
          ambigCheck = makeAmbigZone(this.range.begin);
          if ((ambigCheck.begin.getTime() <= (_ref1 = utcDt.getTime()) && _ref1 < ambigCheck.end.getTime())) {
            return true;
          }
          ambigCheck = makeAmbigZone(this.range.end);
          (ambigCheck.begin.getTime() <= (_ref2 = utcDt.getTime()) && _ref2 < ambigCheck.end.getTime());
        }
        rules = new rule.RuleSet(getRulesNamed(this._rule), this);
        return rules.isAmbiguous(dt, this.offset);
      };

      return Zone;

    })();
    ZoneSet = (function() {
      function ZoneSet(zones, getRulesNamed) {
        var zone, _i, _len, _ref;
        this.zones = zones != null ? zones : [];
        this.getRulesNamed = getRulesNamed;
        if (this.zones.length > 0) {
          this.name = this.zones[0].name;
        } else {
          this.name = "";
        }
        _ref = this.zones;
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          zone = _ref[_i];
          zone.updateEndForRules;
        }
      }

      ZoneSet.prototype.add = function(zone) {
        if (this.zones.length === 0 && this.name === "") {
          this.name = zone.name;
        }
        if (this.name !== zone.name) {
          throw new Error("Cannot add different named zones to a ZoneSet");
        }
        return this.zones.push(zone);
      };

      ZoneSet.prototype.findApplicable = function(dt, useOffset) {
        var findOffsetRange, found, range, ts, zone, _i, _len, _ref;
        if (useOffset == null) {
          useOffset = false;
        }
        ts = dt.getTime();
        findOffsetRange = function(zone) {
          return {
            begin: helpers.Time.UTCToStandardTime(zone.range.begin, zone.offset),
            end: helpers.Time.UTCToStandardTime(zone.range.end, zone.offset)
          };
        };
        found = null;
        _ref = this.zones;
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          zone = _ref[_i];
          range = !useOffset ? zone.range : findOffsetRange(zone);
          if ((range.begin.getTime() <= ts && ts <= range.end.getTime())) {
            found = zone;
            break;
          }
        }
        return found;
      };

      ZoneSet.prototype.getWallTimeForUTC = function(dt) {
        var applicable;
        applicable = this.findApplicable(dt);
        if (!applicable) {
          return new TimeZoneTime(dt, helpers.noZone, helpers.noSave);
        }
        return applicable.UTCToWallTime(dt, this.getRulesNamed);
      };

      ZoneSet.prototype.getUTCForWallTime = function(dt) {
        var applicable;
        applicable = this.findApplicable(dt, true);
        if (!applicable) {
          return dt;
        }
        return applicable.WallTimeToUTC(dt, this.getRulesNamed);
      };

      ZoneSet.prototype.isAmbiguous = function(dt) {
        var applicable;
        applicable = this.findApplicable(dt, true);
        if (!applicable) {
          return false;
        }
        return applicable.IsAmbiguous(dt, this.getRulesNamed);
      };

      return ZoneSet;

    })();
    return lib = {
      Zone: Zone,
      ZoneSet: ZoneSet
    };
  };

  this.WallTime || (this.WallTime = {});
  this.WallTime.zone = init(this.WallTime.helpers, this.WallTime.rule, this.WallTime.TimeZoneTime);

}).call(this);

(function() {
  var api, init, key, req_help, req_rule, req_zone, val, _ref, _ref1, _ref2,
    __hasProp = {}.hasOwnProperty;

  init = function(helpers, rule, zone) {
    var WallTime;
    WallTime = (function() {
      function WallTime() {}

      WallTime.prototype.init = function(rules, zones) {
        if (rules == null) {
          rules = {};
        }
        if (zones == null) {
          zones = {};
        }
        this.zones = {};
        this.rules = {};
        this.addRulesZones(rules, zones);
        this.zoneSet = null;
        this.timeZoneName = null;
        return this.doneInit = true;
      };

      WallTime.prototype.addRulesZones = function(rules, zones) {
        var currZone, newRules, newZone, newZones, r, ruleName, ruleVals, z, zoneName, zoneVals, _i, _len, _results;
        if (rules == null) {
          rules = {};
        }
        if (zones == null) {
          zones = {};
        }
        currZone = null;
        for (zoneName in zones) {
          if (!__hasProp.call(zones, zoneName)) continue;
          zoneVals = zones[zoneName];
          newZones = [];
          currZone = null;
          for (_i = 0, _len = zoneVals.length; _i < _len; _i++) {
            z = zoneVals[_i];
            newZone = new zone.Zone(z.name, z._offset, z._rule, z.format, z._until, currZone);
            newZones.push(newZone);
            currZone = newZone;
          }
          this.zones[zoneName] = newZones;
        }
        _results = [];
        for (ruleName in rules) {
          if (!__hasProp.call(rules, ruleName)) continue;
          ruleVals = rules[ruleName];
          newRules = (function() {
            var _j, _len1, _results1;
            _results1 = [];
            for (_j = 0, _len1 = ruleVals.length; _j < _len1; _j++) {
              r = ruleVals[_j];
              _results1.push(new rule.Rule(r.name, r._from, r._to, r.type, r["in"], r.on, r.at, r._save, r.letter));
            }
            return _results1;
          })();
          _results.push(this.rules[ruleName] = newRules);
        }
        return _results;
      };

      WallTime.prototype.setTimeZone = function(name) {
        var matches,
          _this = this;
        if (!this.doneInit) {
          throw new Error("Must call init with rules and zones before setting time zone");
        }
        if (!this.zones[name]) {
          throw new Error("Unable to find time zone named " + (name || '<blank>'));
        }
        matches = this.zones[name];
        this.zoneSet = new zone.ZoneSet(matches, function(ruleName) {
          return _this.rules[ruleName];
        });
        return this.timeZoneName = name;
      };

      WallTime.prototype.Date = function(y, m, d, h, mi, s, ms) {
        if (m == null) {
          m = 0;
        }
        if (d == null) {
          d = 1;
        }
        if (h == null) {
          h = 0;
        }
        if (mi == null) {
          mi = 0;
        }
        if (s == null) {
          s = 0;
        }
        if (ms == null) {
          ms = 0;
        }
        y || (y = new Date().getUTCFullYear());
        return helpers.Time.MakeDateFromParts(y, m, d, h, mi, s, ms);
      };

      WallTime.prototype.UTCToWallTime = function(dt, zoneName) {
        if (zoneName == null) {
          zoneName = this.timeZoneName;
        }
        if (typeof dt === "number") {
          dt = new Date(dt);
        }
        if (zoneName !== this.timeZoneName) {
          this.setTimeZone(zoneName);
        }
        if (!this.zoneSet) {
          throw new Error("Must set the time zone before converting times");
        }
        return this.zoneSet.getWallTimeForUTC(dt);
      };

      WallTime.prototype.WallTimeToUTC = function(zoneName, y, m, d, h, mi, s, ms) {
        var wallTime;
        if (zoneName == null) {
          zoneName = this.timeZoneName;
        }
        if (m == null) {
          m = 0;
        }
        if (d == null) {
          d = 1;
        }
        if (h == null) {
          h = 0;
        }
        if (mi == null) {
          mi = 0;
        }
        if (s == null) {
          s = 0;
        }
        if (ms == null) {
          ms = 0;
        }
        if (zoneName !== this.timeZoneName) {
          this.setTimeZone(zoneName);
        }
        wallTime = typeof y === "number" ? helpers.Time.MakeDateFromParts(y, m, d, h, mi, s, ms) : y;
        return this.zoneSet.getUTCForWallTime(wallTime);
      };

      WallTime.prototype.IsAmbiguous = function(zoneName, y, m, d, h, mi) {
        var wallTime;
        if (zoneName == null) {
          zoneName = this.timeZoneName;
        }
        if (mi == null) {
          mi = 0;
        }
        if (zoneName !== this.timeZoneName) {
          this.setTimeZone(zoneName);
        }
        wallTime = typeof y === "number" ? helpers.Time.MakeDateFromParts(y, m, d, h, mi) : y;
        return this.zoneSet.isAmbiguous(wallTime);
      };

      return WallTime;

    })();
    return new WallTime;
  };

  this.WallTime || (this.WallTime = {});
  api = init(this.WallTime.helpers, this.WallTime.rule, this.WallTime.zone);
  _ref = this.WallTime;
  for (key in _ref) {
    if (!__hasProp.call(_ref, key)) continue;
    val = _ref[key];
    api[key] = val;
  }
  this.WallTime = api;
  if (this.WallTime.autoinit && ((_ref1 = this.WallTime.data) != null ? _ref1.rules : void 0) && ((_ref2 = this.WallTime.data) != null ? _ref2.zones : void 0)) {
    this.WallTime.init(this.WallTime.data.rules, this.WallTime.data.zones);
  }

}).call(this);

module.exports = this.WallTime;


},{}],5:[function(require,module,exports){
"use strict";
/**
 * Checks to see if thing is an instance of the given constructor.
 * Works just like the native instanceof method but handles the case when
 * objects are coming from different frames or from different modules.
 * @param thing - the thing to test
 * @param constructor - the constructor class to check against
 * @returns {boolean}
 */
function isInstanceOf(thing, constructor) {
    if (typeof constructor !== 'function')
        throw new TypeError("constructor must be a function");
    if (thing instanceof constructor)
        return true;
    if (thing == null)
        return false;
    var constructorName = constructor.name;
    if (!constructorName)
        return false;
    var thingProto = thing.__proto__;
    while (thingProto && thingProto.constructor) {
        if (thingProto.constructor.name === constructorName)
            return true;
        thingProto = thingProto.__proto__;
    }
    return false;
}
exports.isInstanceOf = isInstanceOf;
/**
 * Check to see if things are an array of instances of the given constructor
 * Uses isInstanceOf internally
 * @param things - the array of things to test
 * @param constructor - the constructor class to check against
 * @returns {boolean}
 */
function isArrayOf(things, constructor) {
    if (!Array.isArray(things))
        return false;
    for (var i = 0, length = things.length; i < length; i++) {
        if (!isInstanceOf(things[i], constructor))
            return false;
    }
    return true;
}
exports.isArrayOf = isArrayOf;
/**
 * Does a quick 'duck typing' test to see if the given parameter is an immutable class
 * @param thing - the thing to test
 * @returns {boolean}
 */
function isImmutableClass(thing) {
    if (!thing || typeof thing !== 'object')
        return false;
    var ClassFn = thing.constructor;
    return typeof ClassFn.fromJS === 'function' &&
        typeof thing.toJS === 'function' &&
        typeof thing.equals === 'function'; // Has Class#equals
}
exports.isImmutableClass = isImmutableClass;
/**
 * Checks is two arrays have equal immutable classes
 * @param arrayA - array to compare
 * @param arrayB - array to compare
 * @returns {boolean}
 */
function arraysEqual(arrayA, arrayB) {
    var length = arrayA.length;
    if (length !== arrayB.length)
        return false;
    for (var i = 0; i < length; i++) {
        var vA = arrayA[i];
        if (!(vA && typeof vA.equals === 'function' && vA.equals(arrayB[i])))
            return false;
    }
    return true;
}
exports.arraysEqual = arraysEqual;

},{}],6:[function(require,module,exports){
(function (process){
// vim:ts=4:sts=4:sw=4:
/*!
 *
 * Copyright 2009-2012 Kris Kowal under the terms of the MIT
 * license found at http://github.com/kriskowal/q/raw/master/LICENSE
 *
 * With parts by Tyler Close
 * Copyright 2007-2009 Tyler Close under the terms of the MIT X license found
 * at http://www.opensource.org/licenses/mit-license.html
 * Forked at ref_send.js version: 2009-05-11
 *
 * With parts by Mark Miller
 * Copyright (C) 2011 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

(function (definition) {
    "use strict";

    // This file will function properly as a <script> tag, or a module
    // using CommonJS and NodeJS or RequireJS module formats.  In
    // Common/Node/RequireJS, the module exports the Q API and when
    // executed as a simple <script>, it creates a Q global instead.

    // Montage Require
    if (typeof bootstrap === "function") {
        bootstrap("promise", definition);

    // CommonJS
    } else if (typeof exports === "object" && typeof module === "object") {
        module.exports = definition();

    // RequireJS
    } else if (typeof define === "function" && define.amd) {
        define(definition);

    // SES (Secure EcmaScript)
    } else if (typeof ses !== "undefined") {
        if (!ses.ok()) {
            return;
        } else {
            ses.makeQ = definition;
        }

    // <script>
    } else if (typeof window !== "undefined" || typeof self !== "undefined") {
        // Prefer window over self for add-on scripts. Use self for
        // non-windowed contexts.
        var global = typeof window !== "undefined" ? window : self;

        // Get the `window` object, save the previous Q global
        // and initialize Q as a global.
        var previousQ = global.Q;
        global.Q = definition();

        // Add a noConflict function so Q can be removed from the
        // global namespace.
        global.Q.noConflict = function () {
            global.Q = previousQ;
            return this;
        };

    } else {
        throw new Error("This environment was not anticipated by Q. Please file a bug.");
    }

})(function () {
"use strict";

var hasStacks = false;
try {
    throw new Error();
} catch (e) {
    hasStacks = !!e.stack;
}

// All code after this point will be filtered from stack traces reported
// by Q.
var qStartingLine = captureLine();
var qFileName;

// shims

// used for fallback in "allResolved"
var noop = function () {};

// Use the fastest possible means to execute a task in a future turn
// of the event loop.
var nextTick =(function () {
    // linked list of tasks (single, with head node)
    var head = {task: void 0, next: null};
    var tail = head;
    var flushing = false;
    var requestTick = void 0;
    var isNodeJS = false;
    // queue for late tasks, used by unhandled rejection tracking
    var laterQueue = [];

    function flush() {
        /* jshint loopfunc: true */
        var task, domain;

        while (head.next) {
            head = head.next;
            task = head.task;
            head.task = void 0;
            domain = head.domain;

            if (domain) {
                head.domain = void 0;
                domain.enter();
            }
            runSingle(task, domain);

        }
        while (laterQueue.length) {
            task = laterQueue.pop();
            runSingle(task);
        }
        flushing = false;
    }
    // runs a single function in the async queue
    function runSingle(task, domain) {
        try {
            task();

        } catch (e) {
            if (isNodeJS) {
                // In node, uncaught exceptions are considered fatal errors.
                // Re-throw them synchronously to interrupt flushing!

                // Ensure continuation if the uncaught exception is suppressed
                // listening "uncaughtException" events (as domains does).
                // Continue in next event to avoid tick recursion.
                if (domain) {
                    domain.exit();
                }
                setTimeout(flush, 0);
                if (domain) {
                    domain.enter();
                }

                throw e;

            } else {
                // In browsers, uncaught exceptions are not fatal.
                // Re-throw them asynchronously to avoid slow-downs.
                setTimeout(function () {
                    throw e;
                }, 0);
            }
        }

        if (domain) {
            domain.exit();
        }
    }

    nextTick = function (task) {
        tail = tail.next = {
            task: task,
            domain: isNodeJS && process.domain,
            next: null
        };

        if (!flushing) {
            flushing = true;
            requestTick();
        }
    };

    if (typeof process === "object" &&
        process.toString() === "[object process]" && process.nextTick) {
        // Ensure Q is in a real Node environment, with a `process.nextTick`.
        // To see through fake Node environments:
        // * Mocha test runner - exposes a `process` global without a `nextTick`
        // * Browserify - exposes a `process.nexTick` function that uses
        //   `setTimeout`. In this case `setImmediate` is preferred because
        //    it is faster. Browserify's `process.toString()` yields
        //   "[object Object]", while in a real Node environment
        //   `process.nextTick()` yields "[object process]".
        isNodeJS = true;

        requestTick = function () {
            process.nextTick(flush);
        };

    } else if (typeof setImmediate === "function") {
        // In IE10, Node.js 0.9+, or https://github.com/NobleJS/setImmediate
        if (typeof window !== "undefined") {
            requestTick = setImmediate.bind(window, flush);
        } else {
            requestTick = function () {
                setImmediate(flush);
            };
        }

    } else if (typeof MessageChannel !== "undefined") {
        // modern browsers
        // http://www.nonblocking.io/2011/06/windownexttick.html
        var channel = new MessageChannel();
        // At least Safari Version 6.0.5 (8536.30.1) intermittently cannot create
        // working message ports the first time a page loads.
        channel.port1.onmessage = function () {
            requestTick = requestPortTick;
            channel.port1.onmessage = flush;
            flush();
        };
        var requestPortTick = function () {
            // Opera requires us to provide a message payload, regardless of
            // whether we use it.
            channel.port2.postMessage(0);
        };
        requestTick = function () {
            setTimeout(flush, 0);
            requestPortTick();
        };

    } else {
        // old browsers
        requestTick = function () {
            setTimeout(flush, 0);
        };
    }
    // runs a task after all other tasks have been run
    // this is useful for unhandled rejection tracking that needs to happen
    // after all `then`d tasks have been run.
    nextTick.runAfter = function (task) {
        laterQueue.push(task);
        if (!flushing) {
            flushing = true;
            requestTick();
        }
    };
    return nextTick;
})();

// Attempt to make generics safe in the face of downstream
// modifications.
// There is no situation where this is necessary.
// If you need a security guarantee, these primordials need to be
// deeply frozen anyway, and if you don’t need a security guarantee,
// this is just plain paranoid.
// However, this **might** have the nice side-effect of reducing the size of
// the minified code by reducing x.call() to merely x()
// See Mark Miller’s explanation of what this does.
// http://wiki.ecmascript.org/doku.php?id=conventions:safe_meta_programming
var call = Function.call;
function uncurryThis(f) {
    return function () {
        return call.apply(f, arguments);
    };
}
// This is equivalent, but slower:
// uncurryThis = Function_bind.bind(Function_bind.call);
// http://jsperf.com/uncurrythis

var array_slice = uncurryThis(Array.prototype.slice);

var array_reduce = uncurryThis(
    Array.prototype.reduce || function (callback, basis) {
        var index = 0,
            length = this.length;
        // concerning the initial value, if one is not provided
        if (arguments.length === 1) {
            // seek to the first value in the array, accounting
            // for the possibility that is is a sparse array
            do {
                if (index in this) {
                    basis = this[index++];
                    break;
                }
                if (++index >= length) {
                    throw new TypeError();
                }
            } while (1);
        }
        // reduce
        for (; index < length; index++) {
            // account for the possibility that the array is sparse
            if (index in this) {
                basis = callback(basis, this[index], index);
            }
        }
        return basis;
    }
);

var array_indexOf = uncurryThis(
    Array.prototype.indexOf || function (value) {
        // not a very good shim, but good enough for our one use of it
        for (var i = 0; i < this.length; i++) {
            if (this[i] === value) {
                return i;
            }
        }
        return -1;
    }
);

var array_map = uncurryThis(
    Array.prototype.map || function (callback, thisp) {
        var self = this;
        var collect = [];
        array_reduce(self, function (undefined, value, index) {
            collect.push(callback.call(thisp, value, index, self));
        }, void 0);
        return collect;
    }
);

var object_create = Object.create || function (prototype) {
    function Type() { }
    Type.prototype = prototype;
    return new Type();
};

var object_hasOwnProperty = uncurryThis(Object.prototype.hasOwnProperty);

var object_keys = Object.keys || function (object) {
    var keys = [];
    for (var key in object) {
        if (object_hasOwnProperty(object, key)) {
            keys.push(key);
        }
    }
    return keys;
};

var object_toString = uncurryThis(Object.prototype.toString);

function isObject(value) {
    return value === Object(value);
}

// generator related shims

// FIXME: Remove this function once ES6 generators are in SpiderMonkey.
function isStopIteration(exception) {
    return (
        object_toString(exception) === "[object StopIteration]" ||
        exception instanceof QReturnValue
    );
}

// FIXME: Remove this helper and Q.return once ES6 generators are in
// SpiderMonkey.
var QReturnValue;
if (typeof ReturnValue !== "undefined") {
    QReturnValue = ReturnValue;
} else {
    QReturnValue = function (value) {
        this.value = value;
    };
}

// long stack traces

var STACK_JUMP_SEPARATOR = "From previous event:";

function makeStackTraceLong(error, promise) {
    // If possible, transform the error stack trace by removing Node and Q
    // cruft, then concatenating with the stack trace of `promise`. See #57.
    if (hasStacks &&
        promise.stack &&
        typeof error === "object" &&
        error !== null &&
        error.stack &&
        error.stack.indexOf(STACK_JUMP_SEPARATOR) === -1
    ) {
        var stacks = [];
        for (var p = promise; !!p; p = p.source) {
            if (p.stack) {
                stacks.unshift(p.stack);
            }
        }
        stacks.unshift(error.stack);

        var concatedStacks = stacks.join("\n" + STACK_JUMP_SEPARATOR + "\n");
        error.stack = filterStackString(concatedStacks);
    }
}

function filterStackString(stackString) {
    var lines = stackString.split("\n");
    var desiredLines = [];
    for (var i = 0; i < lines.length; ++i) {
        var line = lines[i];

        if (!isInternalFrame(line) && !isNodeFrame(line) && line) {
            desiredLines.push(line);
        }
    }
    return desiredLines.join("\n");
}

function isNodeFrame(stackLine) {
    return stackLine.indexOf("(module.js:") !== -1 ||
           stackLine.indexOf("(node.js:") !== -1;
}

function getFileNameAndLineNumber(stackLine) {
    // Named functions: "at functionName (filename:lineNumber:columnNumber)"
    // In IE10 function name can have spaces ("Anonymous function") O_o
    var attempt1 = /at .+ \((.+):(\d+):(?:\d+)\)$/.exec(stackLine);
    if (attempt1) {
        return [attempt1[1], Number(attempt1[2])];
    }

    // Anonymous functions: "at filename:lineNumber:columnNumber"
    var attempt2 = /at ([^ ]+):(\d+):(?:\d+)$/.exec(stackLine);
    if (attempt2) {
        return [attempt2[1], Number(attempt2[2])];
    }

    // Firefox style: "function@filename:lineNumber or @filename:lineNumber"
    var attempt3 = /.*@(.+):(\d+)$/.exec(stackLine);
    if (attempt3) {
        return [attempt3[1], Number(attempt3[2])];
    }
}

function isInternalFrame(stackLine) {
    var fileNameAndLineNumber = getFileNameAndLineNumber(stackLine);

    if (!fileNameAndLineNumber) {
        return false;
    }

    var fileName = fileNameAndLineNumber[0];
    var lineNumber = fileNameAndLineNumber[1];

    return fileName === qFileName &&
        lineNumber >= qStartingLine &&
        lineNumber <= qEndingLine;
}

// discover own file name and line number range for filtering stack
// traces
function captureLine() {
    if (!hasStacks) {
        return;
    }

    try {
        throw new Error();
    } catch (e) {
        var lines = e.stack.split("\n");
        var firstLine = lines[0].indexOf("@") > 0 ? lines[1] : lines[2];
        var fileNameAndLineNumber = getFileNameAndLineNumber(firstLine);
        if (!fileNameAndLineNumber) {
            return;
        }

        qFileName = fileNameAndLineNumber[0];
        return fileNameAndLineNumber[1];
    }
}

function deprecate(callback, name, alternative) {
    return function () {
        if (typeof console !== "undefined" &&
            typeof console.warn === "function") {
            console.warn(name + " is deprecated, use " + alternative +
                         " instead.", new Error("").stack);
        }
        return callback.apply(callback, arguments);
    };
}

// end of shims
// beginning of real work

/**
 * Constructs a promise for an immediate reference, passes promises through, or
 * coerces promises from different systems.
 * @param value immediate reference or promise
 */
function Q(value) {
    // If the object is already a Promise, return it directly.  This enables
    // the resolve function to both be used to created references from objects,
    // but to tolerably coerce non-promises to promises.
    if (value instanceof Promise) {
        return value;
    }

    // assimilate thenables
    if (isPromiseAlike(value)) {
        return coerce(value);
    } else {
        return fulfill(value);
    }
}
Q.resolve = Q;

/**
 * Performs a task in a future turn of the event loop.
 * @param {Function} task
 */
Q.nextTick = nextTick;

/**
 * Controls whether or not long stack traces will be on
 */
Q.longStackSupport = false;

// enable long stacks if Q_DEBUG is set
if (typeof process === "object" && process && process.env && process.env.Q_DEBUG) {
    Q.longStackSupport = true;
}

/**
 * Constructs a {promise, resolve, reject} object.
 *
 * `resolve` is a callback to invoke with a more resolved value for the
 * promise. To fulfill the promise, invoke `resolve` with any value that is
 * not a thenable. To reject the promise, invoke `resolve` with a rejected
 * thenable, or invoke `reject` with the reason directly. To resolve the
 * promise to another thenable, thus putting it in the same state, invoke
 * `resolve` with that other thenable.
 */
Q.defer = defer;
function defer() {
    // if "messages" is an "Array", that indicates that the promise has not yet
    // been resolved.  If it is "undefined", it has been resolved.  Each
    // element of the messages array is itself an array of complete arguments to
    // forward to the resolved promise.  We coerce the resolution value to a
    // promise using the `resolve` function because it handles both fully
    // non-thenable values and other thenables gracefully.
    var messages = [], progressListeners = [], resolvedPromise;

    var deferred = object_create(defer.prototype);
    var promise = object_create(Promise.prototype);

    promise.promiseDispatch = function (resolve, op, operands) {
        var args = array_slice(arguments);
        if (messages) {
            messages.push(args);
            if (op === "when" && operands[1]) { // progress operand
                progressListeners.push(operands[1]);
            }
        } else {
            Q.nextTick(function () {
                resolvedPromise.promiseDispatch.apply(resolvedPromise, args);
            });
        }
    };

    // XXX deprecated
    promise.valueOf = function () {
        if (messages) {
            return promise;
        }
        var nearerValue = nearer(resolvedPromise);
        if (isPromise(nearerValue)) {
            resolvedPromise = nearerValue; // shorten chain
        }
        return nearerValue;
    };

    promise.inspect = function () {
        if (!resolvedPromise) {
            return { state: "pending" };
        }
        return resolvedPromise.inspect();
    };

    if (Q.longStackSupport && hasStacks) {
        try {
            throw new Error();
        } catch (e) {
            // NOTE: don't try to use `Error.captureStackTrace` or transfer the
            // accessor around; that causes memory leaks as per GH-111. Just
            // reify the stack trace as a string ASAP.
            //
            // At the same time, cut off the first line; it's always just
            // "[object Promise]\n", as per the `toString`.
            promise.stack = e.stack.substring(e.stack.indexOf("\n") + 1);
        }
    }

    // NOTE: we do the checks for `resolvedPromise` in each method, instead of
    // consolidating them into `become`, since otherwise we'd create new
    // promises with the lines `become(whatever(value))`. See e.g. GH-252.

    function become(newPromise) {
        resolvedPromise = newPromise;
        promise.source = newPromise;

        array_reduce(messages, function (undefined, message) {
            Q.nextTick(function () {
                newPromise.promiseDispatch.apply(newPromise, message);
            });
        }, void 0);

        messages = void 0;
        progressListeners = void 0;
    }

    deferred.promise = promise;
    deferred.resolve = function (value) {
        if (resolvedPromise) {
            return;
        }

        become(Q(value));
    };

    deferred.fulfill = function (value) {
        if (resolvedPromise) {
            return;
        }

        become(fulfill(value));
    };
    deferred.reject = function (reason) {
        if (resolvedPromise) {
            return;
        }

        become(reject(reason));
    };
    deferred.notify = function (progress) {
        if (resolvedPromise) {
            return;
        }

        array_reduce(progressListeners, function (undefined, progressListener) {
            Q.nextTick(function () {
                progressListener(progress);
            });
        }, void 0);
    };

    return deferred;
}

/**
 * Creates a Node-style callback that will resolve or reject the deferred
 * promise.
 * @returns a nodeback
 */
defer.prototype.makeNodeResolver = function () {
    var self = this;
    return function (error, value) {
        if (error) {
            self.reject(error);
        } else if (arguments.length > 2) {
            self.resolve(array_slice(arguments, 1));
        } else {
            self.resolve(value);
        }
    };
};

/**
 * @param resolver {Function} a function that returns nothing and accepts
 * the resolve, reject, and notify functions for a deferred.
 * @returns a promise that may be resolved with the given resolve and reject
 * functions, or rejected by a thrown exception in resolver
 */
Q.Promise = promise; // ES6
Q.promise = promise;
function promise(resolver) {
    if (typeof resolver !== "function") {
        throw new TypeError("resolver must be a function.");
    }
    var deferred = defer();
    try {
        resolver(deferred.resolve, deferred.reject, deferred.notify);
    } catch (reason) {
        deferred.reject(reason);
    }
    return deferred.promise;
}

promise.race = race; // ES6
promise.all = all; // ES6
promise.reject = reject; // ES6
promise.resolve = Q; // ES6

// XXX experimental.  This method is a way to denote that a local value is
// serializable and should be immediately dispatched to a remote upon request,
// instead of passing a reference.
Q.passByCopy = function (object) {
    //freeze(object);
    //passByCopies.set(object, true);
    return object;
};

Promise.prototype.passByCopy = function () {
    //freeze(object);
    //passByCopies.set(object, true);
    return this;
};

/**
 * If two promises eventually fulfill to the same value, promises that value,
 * but otherwise rejects.
 * @param x {Any*}
 * @param y {Any*}
 * @returns {Any*} a promise for x and y if they are the same, but a rejection
 * otherwise.
 *
 */
Q.join = function (x, y) {
    return Q(x).join(y);
};

Promise.prototype.join = function (that) {
    return Q([this, that]).spread(function (x, y) {
        if (x === y) {
            // TODO: "===" should be Object.is or equiv
            return x;
        } else {
            throw new Error("Can't join: not the same: " + x + " " + y);
        }
    });
};

/**
 * Returns a promise for the first of an array of promises to become settled.
 * @param answers {Array[Any*]} promises to race
 * @returns {Any*} the first promise to be settled
 */
Q.race = race;
function race(answerPs) {
    return promise(function (resolve, reject) {
        // Switch to this once we can assume at least ES5
        // answerPs.forEach(function (answerP) {
        //     Q(answerP).then(resolve, reject);
        // });
        // Use this in the meantime
        for (var i = 0, len = answerPs.length; i < len; i++) {
            Q(answerPs[i]).then(resolve, reject);
        }
    });
}

Promise.prototype.race = function () {
    return this.then(Q.race);
};

/**
 * Constructs a Promise with a promise descriptor object and optional fallback
 * function.  The descriptor contains methods like when(rejected), get(name),
 * set(name, value), post(name, args), and delete(name), which all
 * return either a value, a promise for a value, or a rejection.  The fallback
 * accepts the operation name, a resolver, and any further arguments that would
 * have been forwarded to the appropriate method above had a method been
 * provided with the proper name.  The API makes no guarantees about the nature
 * of the returned object, apart from that it is usable whereever promises are
 * bought and sold.
 */
Q.makePromise = Promise;
function Promise(descriptor, fallback, inspect) {
    if (fallback === void 0) {
        fallback = function (op) {
            return reject(new Error(
                "Promise does not support operation: " + op
            ));
        };
    }
    if (inspect === void 0) {
        inspect = function () {
            return {state: "unknown"};
        };
    }

    var promise = object_create(Promise.prototype);

    promise.promiseDispatch = function (resolve, op, args) {
        var result;
        try {
            if (descriptor[op]) {
                result = descriptor[op].apply(promise, args);
            } else {
                result = fallback.call(promise, op, args);
            }
        } catch (exception) {
            result = reject(exception);
        }
        if (resolve) {
            resolve(result);
        }
    };

    promise.inspect = inspect;

    // XXX deprecated `valueOf` and `exception` support
    if (inspect) {
        var inspected = inspect();
        if (inspected.state === "rejected") {
            promise.exception = inspected.reason;
        }

        promise.valueOf = function () {
            var inspected = inspect();
            if (inspected.state === "pending" ||
                inspected.state === "rejected") {
                return promise;
            }
            return inspected.value;
        };
    }

    return promise;
}

Promise.prototype.toString = function () {
    return "[object Promise]";
};

Promise.prototype.then = function (fulfilled, rejected, progressed) {
    var self = this;
    var deferred = defer();
    var done = false;   // ensure the untrusted promise makes at most a
                        // single call to one of the callbacks

    function _fulfilled(value) {
        try {
            return typeof fulfilled === "function" ? fulfilled(value) : value;
        } catch (exception) {
            return reject(exception);
        }
    }

    function _rejected(exception) {
        if (typeof rejected === "function") {
            makeStackTraceLong(exception, self);
            try {
                return rejected(exception);
            } catch (newException) {
                return reject(newException);
            }
        }
        return reject(exception);
    }

    function _progressed(value) {
        return typeof progressed === "function" ? progressed(value) : value;
    }

    Q.nextTick(function () {
        self.promiseDispatch(function (value) {
            if (done) {
                return;
            }
            done = true;

            deferred.resolve(_fulfilled(value));
        }, "when", [function (exception) {
            if (done) {
                return;
            }
            done = true;

            deferred.resolve(_rejected(exception));
        }]);
    });

    // Progress propagator need to be attached in the current tick.
    self.promiseDispatch(void 0, "when", [void 0, function (value) {
        var newValue;
        var threw = false;
        try {
            newValue = _progressed(value);
        } catch (e) {
            threw = true;
            if (Q.onerror) {
                Q.onerror(e);
            } else {
                throw e;
            }
        }

        if (!threw) {
            deferred.notify(newValue);
        }
    }]);

    return deferred.promise;
};

Q.tap = function (promise, callback) {
    return Q(promise).tap(callback);
};

/**
 * Works almost like "finally", but not called for rejections.
 * Original resolution value is passed through callback unaffected.
 * Callback may return a promise that will be awaited for.
 * @param {Function} callback
 * @returns {Q.Promise}
 * @example
 * doSomething()
 *   .then(...)
 *   .tap(console.log)
 *   .then(...);
 */
Promise.prototype.tap = function (callback) {
    callback = Q(callback);

    return this.then(function (value) {
        return callback.fcall(value).thenResolve(value);
    });
};

/**
 * Registers an observer on a promise.
 *
 * Guarantees:
 *
 * 1. that fulfilled and rejected will be called only once.
 * 2. that either the fulfilled callback or the rejected callback will be
 *    called, but not both.
 * 3. that fulfilled and rejected will not be called in this turn.
 *
 * @param value      promise or immediate reference to observe
 * @param fulfilled  function to be called with the fulfilled value
 * @param rejected   function to be called with the rejection exception
 * @param progressed function to be called on any progress notifications
 * @return promise for the return value from the invoked callback
 */
Q.when = when;
function when(value, fulfilled, rejected, progressed) {
    return Q(value).then(fulfilled, rejected, progressed);
}

Promise.prototype.thenResolve = function (value) {
    return this.then(function () { return value; });
};

Q.thenResolve = function (promise, value) {
    return Q(promise).thenResolve(value);
};

Promise.prototype.thenReject = function (reason) {
    return this.then(function () { throw reason; });
};

Q.thenReject = function (promise, reason) {
    return Q(promise).thenReject(reason);
};

/**
 * If an object is not a promise, it is as "near" as possible.
 * If a promise is rejected, it is as "near" as possible too.
 * If it’s a fulfilled promise, the fulfillment value is nearer.
 * If it’s a deferred promise and the deferred has been resolved, the
 * resolution is "nearer".
 * @param object
 * @returns most resolved (nearest) form of the object
 */

// XXX should we re-do this?
Q.nearer = nearer;
function nearer(value) {
    if (isPromise(value)) {
        var inspected = value.inspect();
        if (inspected.state === "fulfilled") {
            return inspected.value;
        }
    }
    return value;
}

/**
 * @returns whether the given object is a promise.
 * Otherwise it is a fulfilled value.
 */
Q.isPromise = isPromise;
function isPromise(object) {
    return object instanceof Promise;
}

Q.isPromiseAlike = isPromiseAlike;
function isPromiseAlike(object) {
    return isObject(object) && typeof object.then === "function";
}

/**
 * @returns whether the given object is a pending promise, meaning not
 * fulfilled or rejected.
 */
Q.isPending = isPending;
function isPending(object) {
    return isPromise(object) && object.inspect().state === "pending";
}

Promise.prototype.isPending = function () {
    return this.inspect().state === "pending";
};

/**
 * @returns whether the given object is a value or fulfilled
 * promise.
 */
Q.isFulfilled = isFulfilled;
function isFulfilled(object) {
    return !isPromise(object) || object.inspect().state === "fulfilled";
}

Promise.prototype.isFulfilled = function () {
    return this.inspect().state === "fulfilled";
};

/**
 * @returns whether the given object is a rejected promise.
 */
Q.isRejected = isRejected;
function isRejected(object) {
    return isPromise(object) && object.inspect().state === "rejected";
}

Promise.prototype.isRejected = function () {
    return this.inspect().state === "rejected";
};

//// BEGIN UNHANDLED REJECTION TRACKING

// This promise library consumes exceptions thrown in handlers so they can be
// handled by a subsequent promise.  The exceptions get added to this array when
// they are created, and removed when they are handled.  Note that in ES6 or
// shimmed environments, this would naturally be a `Set`.
var unhandledReasons = [];
var unhandledRejections = [];
var reportedUnhandledRejections = [];
var trackUnhandledRejections = true;

function resetUnhandledRejections() {
    unhandledReasons.length = 0;
    unhandledRejections.length = 0;

    if (!trackUnhandledRejections) {
        trackUnhandledRejections = true;
    }
}

function trackRejection(promise, reason) {
    if (!trackUnhandledRejections) {
        return;
    }
    if (typeof process === "object" && typeof process.emit === "function") {
        Q.nextTick.runAfter(function () {
            if (array_indexOf(unhandledRejections, promise) !== -1) {
                process.emit("unhandledRejection", reason, promise);
                reportedUnhandledRejections.push(promise);
            }
        });
    }

    unhandledRejections.push(promise);
    if (reason && typeof reason.stack !== "undefined") {
        unhandledReasons.push(reason.stack);
    } else {
        unhandledReasons.push("(no stack) " + reason);
    }
}

function untrackRejection(promise) {
    if (!trackUnhandledRejections) {
        return;
    }

    var at = array_indexOf(unhandledRejections, promise);
    if (at !== -1) {
        if (typeof process === "object" && typeof process.emit === "function") {
            Q.nextTick.runAfter(function () {
                var atReport = array_indexOf(reportedUnhandledRejections, promise);
                if (atReport !== -1) {
                    process.emit("rejectionHandled", unhandledReasons[at], promise);
                    reportedUnhandledRejections.splice(atReport, 1);
                }
            });
        }
        unhandledRejections.splice(at, 1);
        unhandledReasons.splice(at, 1);
    }
}

Q.resetUnhandledRejections = resetUnhandledRejections;

Q.getUnhandledReasons = function () {
    // Make a copy so that consumers can't interfere with our internal state.
    return unhandledReasons.slice();
};

Q.stopUnhandledRejectionTracking = function () {
    resetUnhandledRejections();
    trackUnhandledRejections = false;
};

resetUnhandledRejections();

//// END UNHANDLED REJECTION TRACKING

/**
 * Constructs a rejected promise.
 * @param reason value describing the failure
 */
Q.reject = reject;
function reject(reason) {
    var rejection = Promise({
        "when": function (rejected) {
            // note that the error has been handled
            if (rejected) {
                untrackRejection(this);
            }
            return rejected ? rejected(reason) : this;
        }
    }, function fallback() {
        return this;
    }, function inspect() {
        return { state: "rejected", reason: reason };
    });

    // Note that the reason has not been handled.
    trackRejection(rejection, reason);

    return rejection;
}

/**
 * Constructs a fulfilled promise for an immediate reference.
 * @param value immediate reference
 */
Q.fulfill = fulfill;
function fulfill(value) {
    return Promise({
        "when": function () {
            return value;
        },
        "get": function (name) {
            return value[name];
        },
        "set": function (name, rhs) {
            value[name] = rhs;
        },
        "delete": function (name) {
            delete value[name];
        },
        "post": function (name, args) {
            // Mark Miller proposes that post with no name should apply a
            // promised function.
            if (name === null || name === void 0) {
                return value.apply(void 0, args);
            } else {
                return value[name].apply(value, args);
            }
        },
        "apply": function (thisp, args) {
            return value.apply(thisp, args);
        },
        "keys": function () {
            return object_keys(value);
        }
    }, void 0, function inspect() {
        return { state: "fulfilled", value: value };
    });
}

/**
 * Converts thenables to Q promises.
 * @param promise thenable promise
 * @returns a Q promise
 */
function coerce(promise) {
    var deferred = defer();
    Q.nextTick(function () {
        try {
            promise.then(deferred.resolve, deferred.reject, deferred.notify);
        } catch (exception) {
            deferred.reject(exception);
        }
    });
    return deferred.promise;
}

/**
 * Annotates an object such that it will never be
 * transferred away from this process over any promise
 * communication channel.
 * @param object
 * @returns promise a wrapping of that object that
 * additionally responds to the "isDef" message
 * without a rejection.
 */
Q.master = master;
function master(object) {
    return Promise({
        "isDef": function () {}
    }, function fallback(op, args) {
        return dispatch(object, op, args);
    }, function () {
        return Q(object).inspect();
    });
}

/**
 * Spreads the values of a promised array of arguments into the
 * fulfillment callback.
 * @param fulfilled callback that receives variadic arguments from the
 * promised array
 * @param rejected callback that receives the exception if the promise
 * is rejected.
 * @returns a promise for the return value or thrown exception of
 * either callback.
 */
Q.spread = spread;
function spread(value, fulfilled, rejected) {
    return Q(value).spread(fulfilled, rejected);
}

Promise.prototype.spread = function (fulfilled, rejected) {
    return this.all().then(function (array) {
        return fulfilled.apply(void 0, array);
    }, rejected);
};

/**
 * The async function is a decorator for generator functions, turning
 * them into asynchronous generators.  Although generators are only part
 * of the newest ECMAScript 6 drafts, this code does not cause syntax
 * errors in older engines.  This code should continue to work and will
 * in fact improve over time as the language improves.
 *
 * ES6 generators are currently part of V8 version 3.19 with the
 * --harmony-generators runtime flag enabled.  SpiderMonkey has had them
 * for longer, but under an older Python-inspired form.  This function
 * works on both kinds of generators.
 *
 * Decorates a generator function such that:
 *  - it may yield promises
 *  - execution will continue when that promise is fulfilled
 *  - the value of the yield expression will be the fulfilled value
 *  - it returns a promise for the return value (when the generator
 *    stops iterating)
 *  - the decorated function returns a promise for the return value
 *    of the generator or the first rejected promise among those
 *    yielded.
 *  - if an error is thrown in the generator, it propagates through
 *    every following yield until it is caught, or until it escapes
 *    the generator function altogether, and is translated into a
 *    rejection for the promise returned by the decorated generator.
 */
Q.async = async;
function async(makeGenerator) {
    return function () {
        // when verb is "send", arg is a value
        // when verb is "throw", arg is an exception
        function continuer(verb, arg) {
            var result;

            // Until V8 3.19 / Chromium 29 is released, SpiderMonkey is the only
            // engine that has a deployed base of browsers that support generators.
            // However, SM's generators use the Python-inspired semantics of
            // outdated ES6 drafts.  We would like to support ES6, but we'd also
            // like to make it possible to use generators in deployed browsers, so
            // we also support Python-style generators.  At some point we can remove
            // this block.

            if (typeof StopIteration === "undefined") {
                // ES6 Generators
                try {
                    result = generator[verb](arg);
                } catch (exception) {
                    return reject(exception);
                }
                if (result.done) {
                    return Q(result.value);
                } else {
                    return when(result.value, callback, errback);
                }
            } else {
                // SpiderMonkey Generators
                // FIXME: Remove this case when SM does ES6 generators.
                try {
                    result = generator[verb](arg);
                } catch (exception) {
                    if (isStopIteration(exception)) {
                        return Q(exception.value);
                    } else {
                        return reject(exception);
                    }
                }
                return when(result, callback, errback);
            }
        }
        var generator = makeGenerator.apply(this, arguments);
        var callback = continuer.bind(continuer, "next");
        var errback = continuer.bind(continuer, "throw");
        return callback();
    };
}

/**
 * The spawn function is a small wrapper around async that immediately
 * calls the generator and also ends the promise chain, so that any
 * unhandled errors are thrown instead of forwarded to the error
 * handler. This is useful because it's extremely common to run
 * generators at the top-level to work with libraries.
 */
Q.spawn = spawn;
function spawn(makeGenerator) {
    Q.done(Q.async(makeGenerator)());
}

// FIXME: Remove this interface once ES6 generators are in SpiderMonkey.
/**
 * Throws a ReturnValue exception to stop an asynchronous generator.
 *
 * This interface is a stop-gap measure to support generator return
 * values in older Firefox/SpiderMonkey.  In browsers that support ES6
 * generators like Chromium 29, just use "return" in your generator
 * functions.
 *
 * @param value the return value for the surrounding generator
 * @throws ReturnValue exception with the value.
 * @example
 * // ES6 style
 * Q.async(function* () {
 *      var foo = yield getFooPromise();
 *      var bar = yield getBarPromise();
 *      return foo + bar;
 * })
 * // Older SpiderMonkey style
 * Q.async(function () {
 *      var foo = yield getFooPromise();
 *      var bar = yield getBarPromise();
 *      Q.return(foo + bar);
 * })
 */
Q["return"] = _return;
function _return(value) {
    throw new QReturnValue(value);
}

/**
 * The promised function decorator ensures that any promise arguments
 * are settled and passed as values (`this` is also settled and passed
 * as a value).  It will also ensure that the result of a function is
 * always a promise.
 *
 * @example
 * var add = Q.promised(function (a, b) {
 *     return a + b;
 * });
 * add(Q(a), Q(B));
 *
 * @param {function} callback The function to decorate
 * @returns {function} a function that has been decorated.
 */
Q.promised = promised;
function promised(callback) {
    return function () {
        return spread([this, all(arguments)], function (self, args) {
            return callback.apply(self, args);
        });
    };
}

/**
 * sends a message to a value in a future turn
 * @param object* the recipient
 * @param op the name of the message operation, e.g., "when",
 * @param args further arguments to be forwarded to the operation
 * @returns result {Promise} a promise for the result of the operation
 */
Q.dispatch = dispatch;
function dispatch(object, op, args) {
    return Q(object).dispatch(op, args);
}

Promise.prototype.dispatch = function (op, args) {
    var self = this;
    var deferred = defer();
    Q.nextTick(function () {
        self.promiseDispatch(deferred.resolve, op, args);
    });
    return deferred.promise;
};

/**
 * Gets the value of a property in a future turn.
 * @param object    promise or immediate reference for target object
 * @param name      name of property to get
 * @return promise for the property value
 */
Q.get = function (object, key) {
    return Q(object).dispatch("get", [key]);
};

Promise.prototype.get = function (key) {
    return this.dispatch("get", [key]);
};

/**
 * Sets the value of a property in a future turn.
 * @param object    promise or immediate reference for object object
 * @param name      name of property to set
 * @param value     new value of property
 * @return promise for the return value
 */
Q.set = function (object, key, value) {
    return Q(object).dispatch("set", [key, value]);
};

Promise.prototype.set = function (key, value) {
    return this.dispatch("set", [key, value]);
};

/**
 * Deletes a property in a future turn.
 * @param object    promise or immediate reference for target object
 * @param name      name of property to delete
 * @return promise for the return value
 */
Q.del = // XXX legacy
Q["delete"] = function (object, key) {
    return Q(object).dispatch("delete", [key]);
};

Promise.prototype.del = // XXX legacy
Promise.prototype["delete"] = function (key) {
    return this.dispatch("delete", [key]);
};

/**
 * Invokes a method in a future turn.
 * @param object    promise or immediate reference for target object
 * @param name      name of method to invoke
 * @param value     a value to post, typically an array of
 *                  invocation arguments for promises that
 *                  are ultimately backed with `resolve` values,
 *                  as opposed to those backed with URLs
 *                  wherein the posted value can be any
 *                  JSON serializable object.
 * @return promise for the return value
 */
// bound locally because it is used by other methods
Q.mapply = // XXX As proposed by "Redsandro"
Q.post = function (object, name, args) {
    return Q(object).dispatch("post", [name, args]);
};

Promise.prototype.mapply = // XXX As proposed by "Redsandro"
Promise.prototype.post = function (name, args) {
    return this.dispatch("post", [name, args]);
};

/**
 * Invokes a method in a future turn.
 * @param object    promise or immediate reference for target object
 * @param name      name of method to invoke
 * @param ...args   array of invocation arguments
 * @return promise for the return value
 */
Q.send = // XXX Mark Miller's proposed parlance
Q.mcall = // XXX As proposed by "Redsandro"
Q.invoke = function (object, name /*...args*/) {
    return Q(object).dispatch("post", [name, array_slice(arguments, 2)]);
};

Promise.prototype.send = // XXX Mark Miller's proposed parlance
Promise.prototype.mcall = // XXX As proposed by "Redsandro"
Promise.prototype.invoke = function (name /*...args*/) {
    return this.dispatch("post", [name, array_slice(arguments, 1)]);
};

/**
 * Applies the promised function in a future turn.
 * @param object    promise or immediate reference for target function
 * @param args      array of application arguments
 */
Q.fapply = function (object, args) {
    return Q(object).dispatch("apply", [void 0, args]);
};

Promise.prototype.fapply = function (args) {
    return this.dispatch("apply", [void 0, args]);
};

/**
 * Calls the promised function in a future turn.
 * @param object    promise or immediate reference for target function
 * @param ...args   array of application arguments
 */
Q["try"] =
Q.fcall = function (object /* ...args*/) {
    return Q(object).dispatch("apply", [void 0, array_slice(arguments, 1)]);
};

Promise.prototype.fcall = function (/*...args*/) {
    return this.dispatch("apply", [void 0, array_slice(arguments)]);
};

/**
 * Binds the promised function, transforming return values into a fulfilled
 * promise and thrown errors into a rejected one.
 * @param object    promise or immediate reference for target function
 * @param ...args   array of application arguments
 */
Q.fbind = function (object /*...args*/) {
    var promise = Q(object);
    var args = array_slice(arguments, 1);
    return function fbound() {
        return promise.dispatch("apply", [
            this,
            args.concat(array_slice(arguments))
        ]);
    };
};
Promise.prototype.fbind = function (/*...args*/) {
    var promise = this;
    var args = array_slice(arguments);
    return function fbound() {
        return promise.dispatch("apply", [
            this,
            args.concat(array_slice(arguments))
        ]);
    };
};

/**
 * Requests the names of the owned properties of a promised
 * object in a future turn.
 * @param object    promise or immediate reference for target object
 * @return promise for the keys of the eventually settled object
 */
Q.keys = function (object) {
    return Q(object).dispatch("keys", []);
};

Promise.prototype.keys = function () {
    return this.dispatch("keys", []);
};

/**
 * Turns an array of promises into a promise for an array.  If any of
 * the promises gets rejected, the whole array is rejected immediately.
 * @param {Array*} an array (or promise for an array) of values (or
 * promises for values)
 * @returns a promise for an array of the corresponding values
 */
// By Mark Miller
// http://wiki.ecmascript.org/doku.php?id=strawman:concurrency&rev=1308776521#allfulfilled
Q.all = all;
function all(promises) {
    return when(promises, function (promises) {
        var pendingCount = 0;
        var deferred = defer();
        array_reduce(promises, function (undefined, promise, index) {
            var snapshot;
            if (
                isPromise(promise) &&
                (snapshot = promise.inspect()).state === "fulfilled"
            ) {
                promises[index] = snapshot.value;
            } else {
                ++pendingCount;
                when(
                    promise,
                    function (value) {
                        promises[index] = value;
                        if (--pendingCount === 0) {
                            deferred.resolve(promises);
                        }
                    },
                    deferred.reject,
                    function (progress) {
                        deferred.notify({ index: index, value: progress });
                    }
                );
            }
        }, void 0);
        if (pendingCount === 0) {
            deferred.resolve(promises);
        }
        return deferred.promise;
    });
}

Promise.prototype.all = function () {
    return all(this);
};

/**
 * Returns the first resolved promise of an array. Prior rejected promises are
 * ignored.  Rejects only if all promises are rejected.
 * @param {Array*} an array containing values or promises for values
 * @returns a promise fulfilled with the value of the first resolved promise,
 * or a rejected promise if all promises are rejected.
 */
Q.any = any;

function any(promises) {
    if (promises.length === 0) {
        return Q.resolve();
    }

    var deferred = Q.defer();
    var pendingCount = 0;
    array_reduce(promises, function (prev, current, index) {
        var promise = promises[index];

        pendingCount++;

        when(promise, onFulfilled, onRejected, onProgress);
        function onFulfilled(result) {
            deferred.resolve(result);
        }
        function onRejected() {
            pendingCount--;
            if (pendingCount === 0) {
                deferred.reject(new Error(
                    "Can't get fulfillment value from any promise, all " +
                    "promises were rejected."
                ));
            }
        }
        function onProgress(progress) {
            deferred.notify({
                index: index,
                value: progress
            });
        }
    }, undefined);

    return deferred.promise;
}

Promise.prototype.any = function () {
    return any(this);
};

/**
 * Waits for all promises to be settled, either fulfilled or
 * rejected.  This is distinct from `all` since that would stop
 * waiting at the first rejection.  The promise returned by
 * `allResolved` will never be rejected.
 * @param promises a promise for an array (or an array) of promises
 * (or values)
 * @return a promise for an array of promises
 */
Q.allResolved = deprecate(allResolved, "allResolved", "allSettled");
function allResolved(promises) {
    return when(promises, function (promises) {
        promises = array_map(promises, Q);
        return when(all(array_map(promises, function (promise) {
            return when(promise, noop, noop);
        })), function () {
            return promises;
        });
    });
}

Promise.prototype.allResolved = function () {
    return allResolved(this);
};

/**
 * @see Promise#allSettled
 */
Q.allSettled = allSettled;
function allSettled(promises) {
    return Q(promises).allSettled();
}

/**
 * Turns an array of promises into a promise for an array of their states (as
 * returned by `inspect`) when they have all settled.
 * @param {Array[Any*]} values an array (or promise for an array) of values (or
 * promises for values)
 * @returns {Array[State]} an array of states for the respective values.
 */
Promise.prototype.allSettled = function () {
    return this.then(function (promises) {
        return all(array_map(promises, function (promise) {
            promise = Q(promise);
            function regardless() {
                return promise.inspect();
            }
            return promise.then(regardless, regardless);
        }));
    });
};

/**
 * Captures the failure of a promise, giving an oportunity to recover
 * with a callback.  If the given promise is fulfilled, the returned
 * promise is fulfilled.
 * @param {Any*} promise for something
 * @param {Function} callback to fulfill the returned promise if the
 * given promise is rejected
 * @returns a promise for the return value of the callback
 */
Q.fail = // XXX legacy
Q["catch"] = function (object, rejected) {
    return Q(object).then(void 0, rejected);
};

Promise.prototype.fail = // XXX legacy
Promise.prototype["catch"] = function (rejected) {
    return this.then(void 0, rejected);
};

/**
 * Attaches a listener that can respond to progress notifications from a
 * promise's originating deferred. This listener receives the exact arguments
 * passed to ``deferred.notify``.
 * @param {Any*} promise for something
 * @param {Function} callback to receive any progress notifications
 * @returns the given promise, unchanged
 */
Q.progress = progress;
function progress(object, progressed) {
    return Q(object).then(void 0, void 0, progressed);
}

Promise.prototype.progress = function (progressed) {
    return this.then(void 0, void 0, progressed);
};

/**
 * Provides an opportunity to observe the settling of a promise,
 * regardless of whether the promise is fulfilled or rejected.  Forwards
 * the resolution to the returned promise when the callback is done.
 * The callback can return a promise to defer completion.
 * @param {Any*} promise
 * @param {Function} callback to observe the resolution of the given
 * promise, takes no arguments.
 * @returns a promise for the resolution of the given promise when
 * ``fin`` is done.
 */
Q.fin = // XXX legacy
Q["finally"] = function (object, callback) {
    return Q(object)["finally"](callback);
};

Promise.prototype.fin = // XXX legacy
Promise.prototype["finally"] = function (callback) {
    callback = Q(callback);
    return this.then(function (value) {
        return callback.fcall().then(function () {
            return value;
        });
    }, function (reason) {
        // TODO attempt to recycle the rejection with "this".
        return callback.fcall().then(function () {
            throw reason;
        });
    });
};

/**
 * Terminates a chain of promises, forcing rejections to be
 * thrown as exceptions.
 * @param {Any*} promise at the end of a chain of promises
 * @returns nothing
 */
Q.done = function (object, fulfilled, rejected, progress) {
    return Q(object).done(fulfilled, rejected, progress);
};

Promise.prototype.done = function (fulfilled, rejected, progress) {
    var onUnhandledError = function (error) {
        // forward to a future turn so that ``when``
        // does not catch it and turn it into a rejection.
        Q.nextTick(function () {
            makeStackTraceLong(error, promise);
            if (Q.onerror) {
                Q.onerror(error);
            } else {
                throw error;
            }
        });
    };

    // Avoid unnecessary `nextTick`ing via an unnecessary `when`.
    var promise = fulfilled || rejected || progress ?
        this.then(fulfilled, rejected, progress) :
        this;

    if (typeof process === "object" && process && process.domain) {
        onUnhandledError = process.domain.bind(onUnhandledError);
    }

    promise.then(void 0, onUnhandledError);
};

/**
 * Causes a promise to be rejected if it does not get fulfilled before
 * some milliseconds time out.
 * @param {Any*} promise
 * @param {Number} milliseconds timeout
 * @param {Any*} custom error message or Error object (optional)
 * @returns a promise for the resolution of the given promise if it is
 * fulfilled before the timeout, otherwise rejected.
 */
Q.timeout = function (object, ms, error) {
    return Q(object).timeout(ms, error);
};

Promise.prototype.timeout = function (ms, error) {
    var deferred = defer();
    var timeoutId = setTimeout(function () {
        if (!error || "string" === typeof error) {
            error = new Error(error || "Timed out after " + ms + " ms");
            error.code = "ETIMEDOUT";
        }
        deferred.reject(error);
    }, ms);

    this.then(function (value) {
        clearTimeout(timeoutId);
        deferred.resolve(value);
    }, function (exception) {
        clearTimeout(timeoutId);
        deferred.reject(exception);
    }, deferred.notify);

    return deferred.promise;
};

/**
 * Returns a promise for the given value (or promised value), some
 * milliseconds after it resolved. Passes rejections immediately.
 * @param {Any*} promise
 * @param {Number} milliseconds
 * @returns a promise for the resolution of the given promise after milliseconds
 * time has elapsed since the resolution of the given promise.
 * If the given promise rejects, that is passed immediately.
 */
Q.delay = function (object, timeout) {
    if (timeout === void 0) {
        timeout = object;
        object = void 0;
    }
    return Q(object).delay(timeout);
};

Promise.prototype.delay = function (timeout) {
    return this.then(function (value) {
        var deferred = defer();
        setTimeout(function () {
            deferred.resolve(value);
        }, timeout);
        return deferred.promise;
    });
};

/**
 * Passes a continuation to a Node function, which is called with the given
 * arguments provided as an array, and returns a promise.
 *
 *      Q.nfapply(FS.readFile, [__filename])
 *      .then(function (content) {
 *      })
 *
 */
Q.nfapply = function (callback, args) {
    return Q(callback).nfapply(args);
};

Promise.prototype.nfapply = function (args) {
    var deferred = defer();
    var nodeArgs = array_slice(args);
    nodeArgs.push(deferred.makeNodeResolver());
    this.fapply(nodeArgs).fail(deferred.reject);
    return deferred.promise;
};

/**
 * Passes a continuation to a Node function, which is called with the given
 * arguments provided individually, and returns a promise.
 * @example
 * Q.nfcall(FS.readFile, __filename)
 * .then(function (content) {
 * })
 *
 */
Q.nfcall = function (callback /*...args*/) {
    var args = array_slice(arguments, 1);
    return Q(callback).nfapply(args);
};

Promise.prototype.nfcall = function (/*...args*/) {
    var nodeArgs = array_slice(arguments);
    var deferred = defer();
    nodeArgs.push(deferred.makeNodeResolver());
    this.fapply(nodeArgs).fail(deferred.reject);
    return deferred.promise;
};

/**
 * Wraps a NodeJS continuation passing function and returns an equivalent
 * version that returns a promise.
 * @example
 * Q.nfbind(FS.readFile, __filename)("utf-8")
 * .then(console.log)
 * .done()
 */
Q.nfbind =
Q.denodeify = function (callback /*...args*/) {
    var baseArgs = array_slice(arguments, 1);
    return function () {
        var nodeArgs = baseArgs.concat(array_slice(arguments));
        var deferred = defer();
        nodeArgs.push(deferred.makeNodeResolver());
        Q(callback).fapply(nodeArgs).fail(deferred.reject);
        return deferred.promise;
    };
};

Promise.prototype.nfbind =
Promise.prototype.denodeify = function (/*...args*/) {
    var args = array_slice(arguments);
    args.unshift(this);
    return Q.denodeify.apply(void 0, args);
};

Q.nbind = function (callback, thisp /*...args*/) {
    var baseArgs = array_slice(arguments, 2);
    return function () {
        var nodeArgs = baseArgs.concat(array_slice(arguments));
        var deferred = defer();
        nodeArgs.push(deferred.makeNodeResolver());
        function bound() {
            return callback.apply(thisp, arguments);
        }
        Q(bound).fapply(nodeArgs).fail(deferred.reject);
        return deferred.promise;
    };
};

Promise.prototype.nbind = function (/*thisp, ...args*/) {
    var args = array_slice(arguments, 0);
    args.unshift(this);
    return Q.nbind.apply(void 0, args);
};

/**
 * Calls a method of a Node-style object that accepts a Node-style
 * callback with a given array of arguments, plus a provided callback.
 * @param object an object that has the named method
 * @param {String} name name of the method of object
 * @param {Array} args arguments to pass to the method; the callback
 * will be provided by Q and appended to these arguments.
 * @returns a promise for the value or error
 */
Q.nmapply = // XXX As proposed by "Redsandro"
Q.npost = function (object, name, args) {
    return Q(object).npost(name, args);
};

Promise.prototype.nmapply = // XXX As proposed by "Redsandro"
Promise.prototype.npost = function (name, args) {
    var nodeArgs = array_slice(args || []);
    var deferred = defer();
    nodeArgs.push(deferred.makeNodeResolver());
    this.dispatch("post", [name, nodeArgs]).fail(deferred.reject);
    return deferred.promise;
};

/**
 * Calls a method of a Node-style object that accepts a Node-style
 * callback, forwarding the given variadic arguments, plus a provided
 * callback argument.
 * @param object an object that has the named method
 * @param {String} name name of the method of object
 * @param ...args arguments to pass to the method; the callback will
 * be provided by Q and appended to these arguments.
 * @returns a promise for the value or error
 */
Q.nsend = // XXX Based on Mark Miller's proposed "send"
Q.nmcall = // XXX Based on "Redsandro's" proposal
Q.ninvoke = function (object, name /*...args*/) {
    var nodeArgs = array_slice(arguments, 2);
    var deferred = defer();
    nodeArgs.push(deferred.makeNodeResolver());
    Q(object).dispatch("post", [name, nodeArgs]).fail(deferred.reject);
    return deferred.promise;
};

Promise.prototype.nsend = // XXX Based on Mark Miller's proposed "send"
Promise.prototype.nmcall = // XXX Based on "Redsandro's" proposal
Promise.prototype.ninvoke = function (name /*...args*/) {
    var nodeArgs = array_slice(arguments, 1);
    var deferred = defer();
    nodeArgs.push(deferred.makeNodeResolver());
    this.dispatch("post", [name, nodeArgs]).fail(deferred.reject);
    return deferred.promise;
};

/**
 * If a function would like to support both Node continuation-passing-style and
 * promise-returning-style, it can end its internal promise chain with
 * `nodeify(nodeback)`, forwarding the optional nodeback argument.  If the user
 * elects to use a nodeback, the result will be sent there.  If they do not
 * pass a nodeback, they will receive the result promise.
 * @param object a result (or a promise for a result)
 * @param {Function} nodeback a Node.js-style callback
 * @returns either the promise or nothing
 */
Q.nodeify = nodeify;
function nodeify(object, nodeback) {
    return Q(object).nodeify(nodeback);
}

Promise.prototype.nodeify = function (nodeback) {
    if (nodeback) {
        this.then(function (value) {
            Q.nextTick(function () {
                nodeback(null, value);
            });
        }, function (error) {
            Q.nextTick(function () {
                nodeback(error);
            });
        });
    } else {
        return this;
    }
};

Q.noConflict = function() {
    throw new Error("Q.noConflict only works when Q is used as a global");
};

// All code before this point will be filtered from stack traces.
var qEndingLine = captureLine();

return Q;

});

}).call(this,require('_process'))
},{"_process":2}],7:[function(require,module,exports){
module.exports = function(plywood) {
  "use strict";

  /*
   * Generated by PEG.js 0.9.0.
   *
   * http://pegjs.org/
   */

  function peg$subclass(child, parent) {
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor();
  }

  function peg$SyntaxError(message, expected, found, location) {
    this.message  = message;
    this.expected = expected;
    this.found    = found;
    this.location = location;
    this.name     = "SyntaxError";

    if (typeof Error.captureStackTrace === "function") {
      Error.captureStackTrace(this, peg$SyntaxError);
    }
  }

  peg$subclass(peg$SyntaxError, Error);

  function peg$parse(input) {
    var options = arguments.length > 1 ? arguments[1] : {},
        parser  = this,

        peg$FAILED = {},

        peg$startRuleFunctions = { start: peg$parsestart },
        peg$startRuleFunction  = peg$parsestart,

        peg$c0 = function(ex) { return ex; },
        peg$c1 = function(head, tail) { return naryExpressionFactory('or', head, tail); },
        peg$c2 = function(head, tail) { return naryExpressionFactory('and', head, tail); },
        peg$c3 = function(ex) { return ex.not(); },
        peg$c4 = function(lhs, rest) {
              if (!rest) return lhs;
              return lhs[rest[1]](rest[3]);
            },
        peg$c5 = "==",
        peg$c6 = { type: "literal", value: "==", description: "\"==\"" },
        peg$c7 = function() { return 'is'; },
        peg$c8 = "!=",
        peg$c9 = { type: "literal", value: "!=", description: "\"!=\"" },
        peg$c10 = function() { return 'isnt'; },
        peg$c11 = "in",
        peg$c12 = { type: "literal", value: "in", description: "\"in\"" },
        peg$c13 = function() { return 'in'; },
        peg$c14 = "<=",
        peg$c15 = { type: "literal", value: "<=", description: "\"<=\"" },
        peg$c16 = function() { return 'lessThanOrEqual'; },
        peg$c17 = ">=",
        peg$c18 = { type: "literal", value: ">=", description: "\">=\"" },
        peg$c19 = function() { return 'greaterThanOrEqual'; },
        peg$c20 = "<",
        peg$c21 = { type: "literal", value: "<", description: "\"<\"" },
        peg$c22 = function() { return 'lessThan'; },
        peg$c23 = ">",
        peg$c24 = { type: "literal", value: ">", description: "\">\"" },
        peg$c25 = function() { return 'greaterThan'; },
        peg$c26 = function(head, tail) { return naryExpressionFactory('concat', head, tail); },
        peg$c27 = function(head, tail) { return naryExpressionWithAltFactory('add', head, tail, '-', 'subtract'); },
        peg$c28 = /^[+\-]/,
        peg$c29 = { type: "class", value: "[+-]", description: "[+-]" },
        peg$c30 = /^[+]/,
        peg$c31 = { type: "class", value: "[+]", description: "[+]" },
        peg$c32 = function(op) { return op; },
        peg$c33 = function(head, tail) { return naryExpressionWithAltFactory('multiply', head, tail, '/', 'divide'); },
        peg$c34 = /^[*\/]/,
        peg$c35 = { type: "class", value: "[*/]", description: "[*/]" },
        peg$c36 = function(op, ex) {
              var negEx = ex.negate(); // Always negate (even with +) just to make sure it is possible
              return op === '-' ? negEx : ex;
            },
        peg$c37 = ".",
        peg$c38 = { type: "literal", value: ".", description: "\".\"" },
        peg$c39 = "(",
        peg$c40 = { type: "literal", value: "(", description: "\"(\"" },
        peg$c41 = ")",
        peg$c42 = { type: "literal", value: ")", description: "\")\"" },
        peg$c43 = function(lhs, tail) {
              if (!tail.length) return lhs;
              var operand = lhs;
              for (var i = 0, n = tail.length; i < n; i++) {
                var part = tail[i];
                var op = part[3];
                if (!possibleCalls[op]) error('no such call: ' + op);
                var params = part[6] || [];
                operand = operand[op].apply(operand, params);
              }
              return operand;
            },
        peg$c44 = ",",
        peg$c45 = { type: "literal", value: ",", description: "\",\"" },
        peg$c46 = function(head, tail) { return makeListMap3(head, tail); },
        peg$c47 = "ply(",
        peg$c48 = { type: "literal", value: "ply(", description: "\"ply(\"" },
        peg$c49 = function() { return ply(); },
        peg$c50 = "$",
        peg$c51 = { type: "literal", value: "$", description: "\"$\"" },
        peg$c52 = "^",
        peg$c53 = { type: "literal", value: "^", description: "\"^\"" },
        peg$c54 = ":",
        peg$c55 = { type: "literal", value: ":", description: "\":\"" },
        peg$c56 = function(name) { return RefExpression.parse(name); },
        peg$c57 = "{",
        peg$c58 = { type: "literal", value: "{", description: "\"{\"" },
        peg$c59 = /^[^}]/,
        peg$c60 = { type: "class", value: "[^}]", description: "[^}]" },
        peg$c61 = "}",
        peg$c62 = { type: "literal", value: "}", description: "\"}\"" },
        peg$c63 = function(value) { return r(value); },
        peg$c64 = { type: "other", description: "StringSet" },
        peg$c65 = "[",
        peg$c66 = { type: "literal", value: "[", description: "\"[\"" },
        peg$c67 = "]",
        peg$c68 = { type: "literal", value: "]", description: "\"]\"" },
        peg$c69 = function(head, tail) { return Set.fromJS(makeListMap3(head, tail)); },
        peg$c70 = { type: "other", description: "NumberSet" },
        peg$c71 = { type: "other", description: "String" },
        peg$c72 = "'",
        peg$c73 = { type: "literal", value: "'", description: "\"'\"" },
        peg$c74 = function(chars) { return chars; },
        peg$c75 = function(chars) { error("Unmatched single quote"); },
        peg$c76 = "\"",
        peg$c77 = { type: "literal", value: "\"", description: "\"\\\"\"" },
        peg$c78 = function(chars) { error("Unmatched double quote"); },
        peg$c79 = "null",
        peg$c80 = { type: "literal", value: "null", description: "\"null\"" },
        peg$c81 = function() { return null; },
        peg$c82 = "false",
        peg$c83 = { type: "literal", value: "false", description: "\"false\"" },
        peg$c84 = function() { return false; },
        peg$c85 = "true",
        peg$c86 = { type: "literal", value: "true", description: "\"true\"" },
        peg$c87 = function() { return true; },
        peg$c88 = "not",
        peg$c89 = { type: "literal", value: "not", description: "\"not\"" },
        peg$c90 = "and",
        peg$c91 = { type: "literal", value: "and", description: "\"and\"" },
        peg$c92 = "or",
        peg$c93 = { type: "literal", value: "or", description: "\"or\"" },
        peg$c94 = "++",
        peg$c95 = { type: "literal", value: "++", description: "\"++\"" },
        peg$c96 = /^[A-Za-z_]/,
        peg$c97 = { type: "class", value: "[A-Za-z_]", description: "[A-Za-z_]" },
        peg$c98 = { type: "other", description: "Number" },
        peg$c99 = function(n) { return parseFloat(n); },
        peg$c100 = "-",
        peg$c101 = { type: "literal", value: "-", description: "\"-\"" },
        peg$c102 = /^[1-9]/,
        peg$c103 = { type: "class", value: "[1-9]", description: "[1-9]" },
        peg$c104 = "e",
        peg$c105 = { type: "literal", value: "e", description: "\"e\"" },
        peg$c106 = /^[0-9]/,
        peg$c107 = { type: "class", value: "[0-9]", description: "[0-9]" },
        peg$c108 = "ply",
        peg$c109 = { type: "literal", value: "ply", description: "\"ply\"" },
        peg$c110 = { type: "other", description: "CallFn" },
        peg$c111 = /^[a-zA-Z]/,
        peg$c112 = { type: "class", value: "[a-zA-Z]", description: "[a-zA-Z]" },
        peg$c113 = { type: "other", description: "Name" },
        peg$c114 = /^[a-zA-Z_]/,
        peg$c115 = { type: "class", value: "[a-zA-Z_]", description: "[a-zA-Z_]" },
        peg$c116 = /^[a-z0-9A-Z_]/,
        peg$c117 = { type: "class", value: "[a-z0-9A-Z_]", description: "[a-z0-9A-Z_]" },
        peg$c118 = { type: "other", description: "TypeName" },
        peg$c119 = /^[A-Z_\/]/,
        peg$c120 = { type: "class", value: "[A-Z_/]", description: "[A-Z_/]" },
        peg$c121 = { type: "other", description: "NotSQuote" },
        peg$c122 = /^[^']/,
        peg$c123 = { type: "class", value: "[^']", description: "[^']" },
        peg$c124 = { type: "other", description: "NotDQuote" },
        peg$c125 = /^[^"]/,
        peg$c126 = { type: "class", value: "[^\"]", description: "[^\"]" },
        peg$c127 = { type: "other", description: "Whitespace" },
        peg$c128 = /^[ \t\r\n]/,
        peg$c129 = { type: "class", value: "[ \\t\\r\\n]", description: "[ \\t\\r\\n]" },

        peg$currPos          = 0,
        peg$savedPos         = 0,
        peg$posDetailsCache  = [{ line: 1, column: 1, seenCR: false }],
        peg$maxFailPos       = 0,
        peg$maxFailExpected  = [],
        peg$silentFails      = 0,

        peg$result;

    if ("startRule" in options) {
      if (!(options.startRule in peg$startRuleFunctions)) {
        throw new Error("Can't start parsing from rule \"" + options.startRule + "\".");
      }

      peg$startRuleFunction = peg$startRuleFunctions[options.startRule];
    }

    function text() {
      return input.substring(peg$savedPos, peg$currPos);
    }

    function location() {
      return peg$computeLocation(peg$savedPos, peg$currPos);
    }

    function expected(description) {
      throw peg$buildException(
        null,
        [{ type: "other", description: description }],
        input.substring(peg$savedPos, peg$currPos),
        peg$computeLocation(peg$savedPos, peg$currPos)
      );
    }

    function error(message) {
      throw peg$buildException(
        message,
        null,
        input.substring(peg$savedPos, peg$currPos),
        peg$computeLocation(peg$savedPos, peg$currPos)
      );
    }

    function peg$computePosDetails(pos) {
      var details = peg$posDetailsCache[pos],
          p, ch;

      if (details) {
        return details;
      } else {
        p = pos - 1;
        while (!peg$posDetailsCache[p]) {
          p--;
        }

        details = peg$posDetailsCache[p];
        details = {
          line:   details.line,
          column: details.column,
          seenCR: details.seenCR
        };

        while (p < pos) {
          ch = input.charAt(p);
          if (ch === "\n") {
            if (!details.seenCR) { details.line++; }
            details.column = 1;
            details.seenCR = false;
          } else if (ch === "\r" || ch === "\u2028" || ch === "\u2029") {
            details.line++;
            details.column = 1;
            details.seenCR = true;
          } else {
            details.column++;
            details.seenCR = false;
          }

          p++;
        }

        peg$posDetailsCache[pos] = details;
        return details;
      }
    }

    function peg$computeLocation(startPos, endPos) {
      var startPosDetails = peg$computePosDetails(startPos),
          endPosDetails   = peg$computePosDetails(endPos);

      return {
        start: {
          offset: startPos,
          line:   startPosDetails.line,
          column: startPosDetails.column
        },
        end: {
          offset: endPos,
          line:   endPosDetails.line,
          column: endPosDetails.column
        }
      };
    }

    function peg$fail(expected) {
      if (peg$currPos < peg$maxFailPos) { return; }

      if (peg$currPos > peg$maxFailPos) {
        peg$maxFailPos = peg$currPos;
        peg$maxFailExpected = [];
      }

      peg$maxFailExpected.push(expected);
    }

    function peg$buildException(message, expected, found, location) {
      function cleanupExpected(expected) {
        var i = 1;

        expected.sort(function(a, b) {
          if (a.description < b.description) {
            return -1;
          } else if (a.description > b.description) {
            return 1;
          } else {
            return 0;
          }
        });

        while (i < expected.length) {
          if (expected[i - 1] === expected[i]) {
            expected.splice(i, 1);
          } else {
            i++;
          }
        }
      }

      function buildMessage(expected, found) {
        function stringEscape(s) {
          function hex(ch) { return ch.charCodeAt(0).toString(16).toUpperCase(); }

          return s
            .replace(/\\/g,   '\\\\')
            .replace(/"/g,    '\\"')
            .replace(/\x08/g, '\\b')
            .replace(/\t/g,   '\\t')
            .replace(/\n/g,   '\\n')
            .replace(/\f/g,   '\\f')
            .replace(/\r/g,   '\\r')
            .replace(/[\x00-\x07\x0B\x0E\x0F]/g, function(ch) { return '\\x0' + hex(ch); })
            .replace(/[\x10-\x1F\x80-\xFF]/g,    function(ch) { return '\\x'  + hex(ch); })
            .replace(/[\u0100-\u0FFF]/g,         function(ch) { return '\\u0' + hex(ch); })
            .replace(/[\u1000-\uFFFF]/g,         function(ch) { return '\\u'  + hex(ch); });
        }

        var expectedDescs = new Array(expected.length),
            expectedDesc, foundDesc, i;

        for (i = 0; i < expected.length; i++) {
          expectedDescs[i] = expected[i].description;
        }

        expectedDesc = expected.length > 1
          ? expectedDescs.slice(0, -1).join(", ")
              + " or "
              + expectedDescs[expected.length - 1]
          : expectedDescs[0];

        foundDesc = found ? "\"" + stringEscape(found) + "\"" : "end of input";

        return "Expected " + expectedDesc + " but " + foundDesc + " found.";
      }

      if (expected !== null) {
        cleanupExpected(expected);
      }

      return new peg$SyntaxError(
        message !== null ? message : buildMessage(expected, found),
        expected,
        found,
        location
      );
    }

    function peg$parsestart() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseOrExpression();
        if (s2 !== peg$FAILED) {
          s3 = peg$parse_();
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c0(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseOrExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7;

      s0 = peg$currPos;
      s1 = peg$parseAndExpression();
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseOrToken();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseAndExpression();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$currPos;
          s4 = peg$parse_();
          if (s4 !== peg$FAILED) {
            s5 = peg$parseOrToken();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 !== peg$FAILED) {
                s7 = peg$parseAndExpression();
                if (s7 !== peg$FAILED) {
                  s4 = [s4, s5, s6, s7];
                  s3 = s4;
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c1(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseAndExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7;

      s0 = peg$currPos;
      s1 = peg$parseNotExpression();
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseAndToken();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseNotExpression();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$currPos;
          s4 = peg$parse_();
          if (s4 !== peg$FAILED) {
            s5 = peg$parseAndToken();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 !== peg$FAILED) {
                s7 = peg$parseNotExpression();
                if (s7 !== peg$FAILED) {
                  s4 = [s4, s5, s6, s7];
                  s3 = s4;
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c2(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseNotExpression() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      s1 = peg$parseNotToken();
      if (s1 !== peg$FAILED) {
        s2 = peg$parse_();
        if (s2 !== peg$FAILED) {
          s3 = peg$parseComparisonExpression();
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c3(s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$parseComparisonExpression();
      }

      return s0;
    }

    function peg$parseComparisonExpression() {
      var s0, s1, s2, s3, s4, s5, s6;

      s0 = peg$currPos;
      s1 = peg$parseConcatenationExpression();
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        s3 = peg$parse_();
        if (s3 !== peg$FAILED) {
          s4 = peg$parseComparisonOp();
          if (s4 !== peg$FAILED) {
            s5 = peg$parse_();
            if (s5 !== peg$FAILED) {
              s6 = peg$parseConcatenationExpression();
              if (s6 !== peg$FAILED) {
                s3 = [s3, s4, s5, s6];
                s2 = s3;
              } else {
                peg$currPos = s2;
                s2 = peg$FAILED;
              }
            } else {
              peg$currPos = s2;
              s2 = peg$FAILED;
            }
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c4(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseComparisonOp() {
      var s0, s1;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 2) === peg$c5) {
        s1 = peg$c5;
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c6); }
      }
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c7();
      }
      s0 = s1;
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        if (input.substr(peg$currPos, 2) === peg$c8) {
          s1 = peg$c8;
          peg$currPos += 2;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c9); }
        }
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c10();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          if (input.substr(peg$currPos, 2) === peg$c11) {
            s1 = peg$c11;
            peg$currPos += 2;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c12); }
          }
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c13();
          }
          s0 = s1;
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            if (input.substr(peg$currPos, 2) === peg$c14) {
              s1 = peg$c14;
              peg$currPos += 2;
            } else {
              s1 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c15); }
            }
            if (s1 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c16();
            }
            s0 = s1;
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              if (input.substr(peg$currPos, 2) === peg$c17) {
                s1 = peg$c17;
                peg$currPos += 2;
              } else {
                s1 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c18); }
              }
              if (s1 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c19();
              }
              s0 = s1;
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                if (input.charCodeAt(peg$currPos) === 60) {
                  s1 = peg$c20;
                  peg$currPos++;
                } else {
                  s1 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c21); }
                }
                if (s1 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c22();
                }
                s0 = s1;
                if (s0 === peg$FAILED) {
                  s0 = peg$currPos;
                  if (input.charCodeAt(peg$currPos) === 62) {
                    s1 = peg$c23;
                    peg$currPos++;
                  } else {
                    s1 = peg$FAILED;
                    if (peg$silentFails === 0) { peg$fail(peg$c24); }
                  }
                  if (s1 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c25();
                  }
                  s0 = s1;
                }
              }
            }
          }
        }
      }

      return s0;
    }

    function peg$parseConcatenationExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7;

      s0 = peg$currPos;
      s1 = peg$parseAdditiveExpression();
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseConcatToken();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseAdditiveExpression();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$currPos;
          s4 = peg$parse_();
          if (s4 !== peg$FAILED) {
            s5 = peg$parseConcatToken();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 !== peg$FAILED) {
                s7 = peg$parseAdditiveExpression();
                if (s7 !== peg$FAILED) {
                  s4 = [s4, s5, s6, s7];
                  s3 = s4;
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c26(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseAdditiveExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7;

      s0 = peg$currPos;
      s1 = peg$parseMultiplicativeExpression();
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseAdditiveOp();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseMultiplicativeExpression();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$currPos;
          s4 = peg$parse_();
          if (s4 !== peg$FAILED) {
            s5 = peg$parseAdditiveOp();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 !== peg$FAILED) {
                s7 = peg$parseMultiplicativeExpression();
                if (s7 !== peg$FAILED) {
                  s4 = [s4, s5, s6, s7];
                  s3 = s4;
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c27(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseAdditiveOp() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (peg$c28.test(input.charAt(peg$currPos))) {
        s1 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c29); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        if (peg$c30.test(input.charAt(peg$currPos))) {
          s3 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c31); }
        }
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c32(s1);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseMultiplicativeExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7;

      s0 = peg$currPos;
      s1 = peg$parseUnaryExpression();
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseMultiplicativeOp();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseUnaryExpression();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$currPos;
          s4 = peg$parse_();
          if (s4 !== peg$FAILED) {
            s5 = peg$parseMultiplicativeOp();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 !== peg$FAILED) {
                s7 = peg$parseUnaryExpression();
                if (s7 !== peg$FAILED) {
                  s4 = [s4, s5, s6, s7];
                  s3 = s4;
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c33(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseMultiplicativeOp() {
      var s0;

      if (peg$c34.test(input.charAt(peg$currPos))) {
        s0 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c35); }
      }

      return s0;
    }

    function peg$parseUnaryExpression() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      s1 = peg$parseAdditiveOp();
      if (s1 !== peg$FAILED) {
        s2 = peg$parse_();
        if (s2 !== peg$FAILED) {
          s3 = peg$parseCallChainExpression();
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c36(s1, s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$parseCallChainExpression();
      }

      return s0;
    }

    function peg$parseCallChainExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12;

      s0 = peg$currPos;
      s1 = peg$parseBasicExpression();
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 46) {
            s5 = peg$c37;
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c38); }
          }
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseCallFn();
              if (s7 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 40) {
                  s8 = peg$c39;
                  peg$currPos++;
                } else {
                  s8 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c40); }
                }
                if (s8 !== peg$FAILED) {
                  s9 = peg$parse_();
                  if (s9 !== peg$FAILED) {
                    s10 = peg$parseParams();
                    if (s10 === peg$FAILED) {
                      s10 = null;
                    }
                    if (s10 !== peg$FAILED) {
                      s11 = peg$parse_();
                      if (s11 !== peg$FAILED) {
                        if (input.charCodeAt(peg$currPos) === 41) {
                          s12 = peg$c41;
                          peg$currPos++;
                        } else {
                          s12 = peg$FAILED;
                          if (peg$silentFails === 0) { peg$fail(peg$c42); }
                        }
                        if (s12 !== peg$FAILED) {
                          s4 = [s4, s5, s6, s7, s8, s9, s10, s11, s12];
                          s3 = s4;
                        } else {
                          peg$currPos = s3;
                          s3 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s3;
                        s3 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s3;
                      s3 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s3;
                    s3 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$currPos;
          s4 = peg$parse_();
          if (s4 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 46) {
              s5 = peg$c37;
              peg$currPos++;
            } else {
              s5 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c38); }
            }
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 !== peg$FAILED) {
                s7 = peg$parseCallFn();
                if (s7 !== peg$FAILED) {
                  if (input.charCodeAt(peg$currPos) === 40) {
                    s8 = peg$c39;
                    peg$currPos++;
                  } else {
                    s8 = peg$FAILED;
                    if (peg$silentFails === 0) { peg$fail(peg$c40); }
                  }
                  if (s8 !== peg$FAILED) {
                    s9 = peg$parse_();
                    if (s9 !== peg$FAILED) {
                      s10 = peg$parseParams();
                      if (s10 === peg$FAILED) {
                        s10 = null;
                      }
                      if (s10 !== peg$FAILED) {
                        s11 = peg$parse_();
                        if (s11 !== peg$FAILED) {
                          if (input.charCodeAt(peg$currPos) === 41) {
                            s12 = peg$c41;
                            peg$currPos++;
                          } else {
                            s12 = peg$FAILED;
                            if (peg$silentFails === 0) { peg$fail(peg$c42); }
                          }
                          if (s12 !== peg$FAILED) {
                            s4 = [s4, s5, s6, s7, s8, s9, s10, s11, s12];
                            s3 = s4;
                          } else {
                            peg$currPos = s3;
                            s3 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s3;
                          s3 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s3;
                        s3 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s3;
                      s3 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s3;
                    s3 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c43(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseParams() {
      var s0, s1, s2, s3, s4, s5, s6, s7;

      s0 = peg$currPos;
      s1 = peg$parseOrExpression();
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 44) {
            s5 = peg$c44;
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c45); }
          }
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseOrExpression();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$currPos;
          s4 = peg$parse_();
          if (s4 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 44) {
              s5 = peg$c44;
              peg$currPos++;
            } else {
              s5 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c45); }
            }
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 !== peg$FAILED) {
                s7 = peg$parseOrExpression();
                if (s7 !== peg$FAILED) {
                  s4 = [s4, s5, s6, s7];
                  s3 = s4;
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c46(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseBasicExpression() {
      var s0, s1, s2, s3, s4, s5;

      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 40) {
        s1 = peg$c39;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c40); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parse_();
        if (s2 !== peg$FAILED) {
          s3 = peg$parseOrExpression();
          if (s3 !== peg$FAILED) {
            s4 = peg$parse_();
            if (s4 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 41) {
                s5 = peg$c41;
                peg$currPos++;
              } else {
                s5 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c42); }
              }
              if (s5 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c0(s3);
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        if (input.substr(peg$currPos, 4) === peg$c47) {
          s1 = peg$c47;
          peg$currPos += 4;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c48); }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$parse_();
          if (s2 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 41) {
              s3 = peg$c41;
              peg$currPos++;
            } else {
              s3 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c42); }
            }
            if (s3 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c49();
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$parseRefExpression();
          if (s0 === peg$FAILED) {
            s0 = peg$parseLiteralExpression();
          }
        }
      }

      return s0;
    }

    function peg$parseRefExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10;

      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 36) {
        s1 = peg$c50;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c51); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        s3 = peg$currPos;
        s4 = [];
        if (input.charCodeAt(peg$currPos) === 94) {
          s5 = peg$c52;
          peg$currPos++;
        } else {
          s5 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c53); }
        }
        while (s5 !== peg$FAILED) {
          s4.push(s5);
          if (input.charCodeAt(peg$currPos) === 94) {
            s5 = peg$c52;
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c53); }
          }
        }
        if (s4 !== peg$FAILED) {
          s5 = peg$parseName();
          if (s5 !== peg$FAILED) {
            s6 = peg$currPos;
            if (input.charCodeAt(peg$currPos) === 58) {
              s7 = peg$c54;
              peg$currPos++;
            } else {
              s7 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c55); }
            }
            if (s7 !== peg$FAILED) {
              s8 = peg$parseTypeName();
              if (s8 !== peg$FAILED) {
                s7 = [s7, s8];
                s6 = s7;
              } else {
                peg$currPos = s6;
                s6 = peg$FAILED;
              }
            } else {
              peg$currPos = s6;
              s6 = peg$FAILED;
            }
            if (s6 === peg$FAILED) {
              s6 = null;
            }
            if (s6 !== peg$FAILED) {
              s4 = [s4, s5, s6];
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        if (s3 !== peg$FAILED) {
          s2 = input.substring(s2, peg$currPos);
        } else {
          s2 = s3;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c56(s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 36) {
          s1 = peg$c50;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c51); }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          s3 = peg$currPos;
          s4 = [];
          if (input.charCodeAt(peg$currPos) === 94) {
            s5 = peg$c52;
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c53); }
          }
          while (s5 !== peg$FAILED) {
            s4.push(s5);
            if (input.charCodeAt(peg$currPos) === 94) {
              s5 = peg$c52;
              peg$currPos++;
            } else {
              s5 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c53); }
            }
          }
          if (s4 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 123) {
              s5 = peg$c57;
              peg$currPos++;
            } else {
              s5 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c58); }
            }
            if (s5 !== peg$FAILED) {
              s6 = [];
              if (peg$c59.test(input.charAt(peg$currPos))) {
                s7 = input.charAt(peg$currPos);
                peg$currPos++;
              } else {
                s7 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c60); }
              }
              if (s7 !== peg$FAILED) {
                while (s7 !== peg$FAILED) {
                  s6.push(s7);
                  if (peg$c59.test(input.charAt(peg$currPos))) {
                    s7 = input.charAt(peg$currPos);
                    peg$currPos++;
                  } else {
                    s7 = peg$FAILED;
                    if (peg$silentFails === 0) { peg$fail(peg$c60); }
                  }
                }
              } else {
                s6 = peg$FAILED;
              }
              if (s6 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 125) {
                  s7 = peg$c61;
                  peg$currPos++;
                } else {
                  s7 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c62); }
                }
                if (s7 !== peg$FAILED) {
                  s8 = peg$currPos;
                  if (input.charCodeAt(peg$currPos) === 58) {
                    s9 = peg$c54;
                    peg$currPos++;
                  } else {
                    s9 = peg$FAILED;
                    if (peg$silentFails === 0) { peg$fail(peg$c55); }
                  }
                  if (s9 !== peg$FAILED) {
                    s10 = peg$parseTypeName();
                    if (s10 !== peg$FAILED) {
                      s9 = [s9, s10];
                      s8 = s9;
                    } else {
                      peg$currPos = s8;
                      s8 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s8;
                    s8 = peg$FAILED;
                  }
                  if (s8 === peg$FAILED) {
                    s8 = null;
                  }
                  if (s8 !== peg$FAILED) {
                    s4 = [s4, s5, s6, s7, s8];
                    s3 = s4;
                  } else {
                    peg$currPos = s3;
                    s3 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
          if (s3 !== peg$FAILED) {
            s2 = input.substring(s2, peg$currPos);
          } else {
            s2 = s3;
          }
          if (s2 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c56(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      }

      return s0;
    }

    function peg$parseLiteralExpression() {
      var s0, s1;

      s0 = peg$currPos;
      s1 = peg$parseNullToken();
      if (s1 === peg$FAILED) {
        s1 = peg$parseFalseToken();
        if (s1 === peg$FAILED) {
          s1 = peg$parseTrueToken();
        }
      }
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c63(s1);
      }
      s0 = s1;
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parseNumber();
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c63(s1);
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseName();
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c63(s1);
          }
          s0 = s1;
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            s1 = peg$parseString();
            if (s1 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c63(s1);
            }
            s0 = s1;
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              s1 = peg$parseStringSet();
              if (s1 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c63(s1);
              }
              s0 = s1;
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                s1 = peg$parseNumberSet();
                if (s1 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c63(s1);
                }
                s0 = s1;
              }
            }
          }
        }
      }

      return s0;
    }

    function peg$parseStringSet() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8;

      peg$silentFails++;
      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 91) {
        s1 = peg$c65;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c66); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parseStringOrNull();
        if (s2 !== peg$FAILED) {
          s3 = [];
          s4 = peg$currPos;
          s5 = peg$parse_();
          if (s5 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 44) {
              s6 = peg$c44;
              peg$currPos++;
            } else {
              s6 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c45); }
            }
            if (s6 !== peg$FAILED) {
              s7 = peg$parse_();
              if (s7 !== peg$FAILED) {
                s8 = peg$parseStringOrNull();
                if (s8 !== peg$FAILED) {
                  s5 = [s5, s6, s7, s8];
                  s4 = s5;
                } else {
                  peg$currPos = s4;
                  s4 = peg$FAILED;
                }
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          } else {
            peg$currPos = s4;
            s4 = peg$FAILED;
          }
          while (s4 !== peg$FAILED) {
            s3.push(s4);
            s4 = peg$currPos;
            s5 = peg$parse_();
            if (s5 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 44) {
                s6 = peg$c44;
                peg$currPos++;
              } else {
                s6 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c45); }
              }
              if (s6 !== peg$FAILED) {
                s7 = peg$parse_();
                if (s7 !== peg$FAILED) {
                  s8 = peg$parseStringOrNull();
                  if (s8 !== peg$FAILED) {
                    s5 = [s5, s6, s7, s8];
                    s4 = s5;
                  } else {
                    peg$currPos = s4;
                    s4 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s4;
                  s4 = peg$FAILED;
                }
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          }
          if (s3 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 93) {
              s4 = peg$c67;
              peg$currPos++;
            } else {
              s4 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c68); }
            }
            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c69(s2, s3);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c64); }
      }

      return s0;
    }

    function peg$parseStringOrNull() {
      var s0;

      s0 = peg$parseString();
      if (s0 === peg$FAILED) {
        s0 = peg$parseNullToken();
      }

      return s0;
    }

    function peg$parseNumberSet() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8;

      peg$silentFails++;
      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 91) {
        s1 = peg$c65;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c66); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parseNumberOrNull();
        if (s2 !== peg$FAILED) {
          s3 = [];
          s4 = peg$currPos;
          s5 = peg$parse_();
          if (s5 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 44) {
              s6 = peg$c44;
              peg$currPos++;
            } else {
              s6 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c45); }
            }
            if (s6 !== peg$FAILED) {
              s7 = peg$parse_();
              if (s7 !== peg$FAILED) {
                s8 = peg$parseNumberOrNull();
                if (s8 !== peg$FAILED) {
                  s5 = [s5, s6, s7, s8];
                  s4 = s5;
                } else {
                  peg$currPos = s4;
                  s4 = peg$FAILED;
                }
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          } else {
            peg$currPos = s4;
            s4 = peg$FAILED;
          }
          while (s4 !== peg$FAILED) {
            s3.push(s4);
            s4 = peg$currPos;
            s5 = peg$parse_();
            if (s5 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 44) {
                s6 = peg$c44;
                peg$currPos++;
              } else {
                s6 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c45); }
              }
              if (s6 !== peg$FAILED) {
                s7 = peg$parse_();
                if (s7 !== peg$FAILED) {
                  s8 = peg$parseNumberOrNull();
                  if (s8 !== peg$FAILED) {
                    s5 = [s5, s6, s7, s8];
                    s4 = s5;
                  } else {
                    peg$currPos = s4;
                    s4 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s4;
                  s4 = peg$FAILED;
                }
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          }
          if (s3 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 93) {
              s4 = peg$c67;
              peg$currPos++;
            } else {
              s4 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c68); }
            }
            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c69(s2, s3);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c70); }
      }

      return s0;
    }

    function peg$parseNumberOrNull() {
      var s0;

      s0 = peg$parseNumber();
      if (s0 === peg$FAILED) {
        s0 = peg$parseNullToken();
      }

      return s0;
    }

    function peg$parseString() {
      var s0, s1, s2, s3;

      peg$silentFails++;
      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 39) {
        s1 = peg$c72;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c73); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parseNotSQuote();
        if (s2 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 39) {
            s3 = peg$c72;
            peg$currPos++;
          } else {
            s3 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c73); }
          }
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c74(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 39) {
          s1 = peg$c72;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c73); }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$parseNotSQuote();
          if (s2 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c75(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          if (input.charCodeAt(peg$currPos) === 34) {
            s1 = peg$c76;
            peg$currPos++;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c77); }
          }
          if (s1 !== peg$FAILED) {
            s2 = peg$parseNotDQuote();
            if (s2 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 34) {
                s3 = peg$c76;
                peg$currPos++;
              } else {
                s3 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c77); }
              }
              if (s3 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c74(s2);
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            if (input.charCodeAt(peg$currPos) === 34) {
              s1 = peg$c76;
              peg$currPos++;
            } else {
              s1 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c77); }
            }
            if (s1 !== peg$FAILED) {
              s2 = peg$parseNotDQuote();
              if (s2 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c78(s2);
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          }
        }
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c71); }
      }

      return s0;
    }

    function peg$parseNullToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4) === peg$c79) {
        s1 = peg$c79;
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c80); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c81();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseFalseToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 5) === peg$c82) {
        s1 = peg$c82;
        peg$currPos += 5;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c83); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c84();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseTrueToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4) === peg$c85) {
        s1 = peg$c85;
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c86); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c87();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseNotToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 3) === peg$c88) {
        s1 = peg$c88;
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c89); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseAndToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 3) === peg$c90) {
        s1 = peg$c90;
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c91); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseOrToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 2) === peg$c92) {
        s1 = peg$c92;
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c93); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseConcatToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 2) === peg$c94) {
        s1 = peg$c94;
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c95); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseIdentifierPart() {
      var s0;

      if (peg$c96.test(input.charAt(peg$currPos))) {
        s0 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c97); }
      }

      return s0;
    }

    function peg$parseNumber() {
      var s0, s1, s2, s3, s4, s5;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = peg$currPos;
      s2 = peg$currPos;
      s3 = peg$parseInt();
      if (s3 !== peg$FAILED) {
        s4 = peg$parseFraction();
        if (s4 === peg$FAILED) {
          s4 = null;
        }
        if (s4 !== peg$FAILED) {
          s5 = peg$parseExp();
          if (s5 === peg$FAILED) {
            s5 = null;
          }
          if (s5 !== peg$FAILED) {
            s3 = [s3, s4, s5];
            s2 = s3;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
      } else {
        peg$currPos = s2;
        s2 = peg$FAILED;
      }
      if (s2 !== peg$FAILED) {
        s1 = input.substring(s1, peg$currPos);
      } else {
        s1 = s2;
      }
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c99(s1);
      }
      s0 = s1;
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c98); }
      }

      return s0;
    }

    function peg$parseInt() {
      var s0, s1, s2, s3, s4;

      s0 = peg$currPos;
      s1 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 45) {
        s2 = peg$c100;
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c101); }
      }
      if (s2 === peg$FAILED) {
        s2 = null;
      }
      if (s2 !== peg$FAILED) {
        if (peg$c102.test(input.charAt(peg$currPos))) {
          s3 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c103); }
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parseDigits();
          if (s4 !== peg$FAILED) {
            s2 = [s2, s3, s4];
            s1 = s2;
          } else {
            peg$currPos = s1;
            s1 = peg$FAILED;
          }
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 45) {
          s2 = peg$c100;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c101); }
        }
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          s3 = peg$parseDigit();
          if (s3 !== peg$FAILED) {
            s2 = [s2, s3];
            s1 = s2;
          } else {
            peg$currPos = s1;
            s1 = peg$FAILED;
          }
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
        if (s1 !== peg$FAILED) {
          s0 = input.substring(s0, peg$currPos);
        } else {
          s0 = s1;
        }
      }

      return s0;
    }

    function peg$parseFraction() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      s1 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 46) {
        s2 = peg$c37;
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c38); }
      }
      if (s2 !== peg$FAILED) {
        s3 = peg$parseDigits();
        if (s3 !== peg$FAILED) {
          s2 = [s2, s3];
          s1 = s2;
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }

      return s0;
    }

    function peg$parseExp() {
      var s0, s1, s2, s3, s4;

      s0 = peg$currPos;
      s1 = peg$currPos;
      if (input.substr(peg$currPos, 1).toLowerCase() === peg$c104) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c105); }
      }
      if (s2 !== peg$FAILED) {
        if (peg$c28.test(input.charAt(peg$currPos))) {
          s3 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c29); }
        }
        if (s3 === peg$FAILED) {
          s3 = null;
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parseDigits();
          if (s4 !== peg$FAILED) {
            s2 = [s2, s3, s4];
            s1 = s2;
          } else {
            peg$currPos = s1;
            s1 = peg$FAILED;
          }
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }

      return s0;
    }

    function peg$parseDigits() {
      var s0, s1, s2;

      s0 = peg$currPos;
      s1 = [];
      s2 = peg$parseDigit();
      if (s2 !== peg$FAILED) {
        while (s2 !== peg$FAILED) {
          s1.push(s2);
          s2 = peg$parseDigit();
        }
      } else {
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }

      return s0;
    }

    function peg$parseDigit() {
      var s0;

      if (peg$c106.test(input.charAt(peg$currPos))) {
        s0 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c107); }
      }

      return s0;
    }

    function peg$parseReservedWord() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 3) === peg$c108) {
        s1 = peg$c108;
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c109); }
      }
      if (s1 === peg$FAILED) {
        if (input.substr(peg$currPos, 5) === peg$c82) {
          s1 = peg$c82;
          peg$currPos += 5;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c83); }
        }
        if (s1 === peg$FAILED) {
          if (input.substr(peg$currPos, 4) === peg$c85) {
            s1 = peg$c85;
            peg$currPos += 4;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c86); }
          }
        }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        if (peg$c96.test(input.charAt(peg$currPos))) {
          s3 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c97); }
        }
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseCallFn() {
      var s0, s1, s2;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = [];
      if (peg$c111.test(input.charAt(peg$currPos))) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c112); }
      }
      if (s2 !== peg$FAILED) {
        while (s2 !== peg$FAILED) {
          s1.push(s2);
          if (peg$c111.test(input.charAt(peg$currPos))) {
            s2 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s2 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c112); }
          }
        }
      } else {
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c110); }
      }

      return s0;
    }

    function peg$parseName() {
      var s0, s1, s2, s3, s4, s5;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = peg$currPos;
      s2 = peg$currPos;
      peg$silentFails++;
      s3 = peg$parseReservedWord();
      peg$silentFails--;
      if (s3 === peg$FAILED) {
        s2 = void 0;
      } else {
        peg$currPos = s2;
        s2 = peg$FAILED;
      }
      if (s2 !== peg$FAILED) {
        if (peg$c114.test(input.charAt(peg$currPos))) {
          s3 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c115); }
        }
        if (s3 !== peg$FAILED) {
          s4 = [];
          if (peg$c116.test(input.charAt(peg$currPos))) {
            s5 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c117); }
          }
          while (s5 !== peg$FAILED) {
            s4.push(s5);
            if (peg$c116.test(input.charAt(peg$currPos))) {
              s5 = input.charAt(peg$currPos);
              peg$currPos++;
            } else {
              s5 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c117); }
            }
          }
          if (s4 !== peg$FAILED) {
            s2 = [s2, s3, s4];
            s1 = s2;
          } else {
            peg$currPos = s1;
            s1 = peg$FAILED;
          }
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c113); }
      }

      return s0;
    }

    function peg$parseTypeName() {
      var s0, s1, s2;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = [];
      if (peg$c119.test(input.charAt(peg$currPos))) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c120); }
      }
      if (s2 !== peg$FAILED) {
        while (s2 !== peg$FAILED) {
          s1.push(s2);
          if (peg$c119.test(input.charAt(peg$currPos))) {
            s2 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s2 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c120); }
          }
        }
      } else {
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c118); }
      }

      return s0;
    }

    function peg$parseNotSQuote() {
      var s0, s1, s2;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = [];
      if (peg$c122.test(input.charAt(peg$currPos))) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c123); }
      }
      while (s2 !== peg$FAILED) {
        s1.push(s2);
        if (peg$c122.test(input.charAt(peg$currPos))) {
          s2 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c123); }
        }
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c121); }
      }

      return s0;
    }

    function peg$parseNotDQuote() {
      var s0, s1, s2;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = [];
      if (peg$c125.test(input.charAt(peg$currPos))) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c126); }
      }
      while (s2 !== peg$FAILED) {
        s1.push(s2);
        if (peg$c125.test(input.charAt(peg$currPos))) {
          s2 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c126); }
        }
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c124); }
      }

      return s0;
    }

    function peg$parse_() {
      var s0, s1, s2;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = [];
      if (peg$c128.test(input.charAt(peg$currPos))) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c129); }
      }
      while (s2 !== peg$FAILED) {
        s1.push(s2);
        if (peg$c128.test(input.charAt(peg$currPos))) {
          s2 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c129); }
        }
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c127); }
      }

      return s0;
    }

    // starts with function(plywood)
    var ply = plywood.ply;
    var $ = plywood.$;
    var r = plywood.r;
    var Expression = plywood.Expression;
    var LiteralExpression = plywood.LiteralExpression;
    var RefExpression = plywood.RefExpression;
    var Set = plywood.Set;

    var possibleCalls = {
      'add': 1,
      'apply': 1,
      'average': 1,
      'contains': 1,
      'count': 1,
      'countDistinct': 1,
      'custom': 1,
      'divide': 1,
      'extract': 1,
      'filter': 1,
      'greaterThan': 1,
      'greaterThanOrEqual': 1,
      'in': 1,
      'is': 1,
      'lessThan': 1,
      'lessThanOrEqual': 1,
      'limit': 1,
      'lookup': 1,
      'match': 1,
      'max': 1,
      'min': 1,
      'multiply': 1,
      'not': 1,
      'numberBucket': 1,
      'quantile': 1,
      'sort': 1,
      'split': 1,
      'substr': 1,
      'subtract': 1,
      'sum': 1,
      'timeBucket': 1,
      'timePart': 1
    };

    function makeListMap3(head, tail) {
      return [head].concat(tail.map(function(t) { return t[3] }));
    }

    function naryExpressionFactory(op, head, tail) {
      if (!tail.length) return head;
      return head[op].apply(head, tail.map(function(t) { return t[3]; }));
    }

    function naryExpressionWithAltFactory(op, head, tail, altToken, altOp) {
      if (!tail.length) return head;
      for (var i = 0; i < tail.length; i++) {
        var t = tail[i];
        head = head[t[1] === altToken ? altOp : op].call(head, t[3]);
      }
      return head;
    }



    peg$result = peg$startRuleFunction();

    if (peg$result !== peg$FAILED && peg$currPos === input.length) {
      return peg$result;
    } else {
      if (peg$result !== peg$FAILED && peg$currPos < input.length) {
        peg$fail({ type: "end", description: "end of input" });
      }

      throw peg$buildException(
        null,
        peg$maxFailExpected,
        peg$maxFailPos < input.length ? input.charAt(peg$maxFailPos) : null,
        peg$maxFailPos < input.length
          ? peg$computeLocation(peg$maxFailPos, peg$maxFailPos + 1)
          : peg$computeLocation(peg$maxFailPos, peg$maxFailPos)
      );
    }
  }

  return {
    SyntaxError: peg$SyntaxError,
    parse:       peg$parse
  };
};

},{}],8:[function(require,module,exports){
module.exports = function(plywood) {
  "use strict";

  /*
   * Generated by PEG.js 0.9.0.
   *
   * http://pegjs.org/
   */

  function peg$subclass(child, parent) {
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor();
  }

  function peg$SyntaxError(message, expected, found, location) {
    this.message  = message;
    this.expected = expected;
    this.found    = found;
    this.location = location;
    this.name     = "SyntaxError";

    if (typeof Error.captureStackTrace === "function") {
      Error.captureStackTrace(this, peg$SyntaxError);
    }
  }

  peg$subclass(peg$SyntaxError, Error);

  function peg$parse(input) {
    var options = arguments.length > 1 ? arguments[1] : {},
        parser  = this,

        peg$FAILED = {},

        peg$startRuleFunctions = { start: peg$parsestart },
        peg$startRuleFunction  = peg$parsestart,

        peg$c0 = function(queryParse) { return queryParse; },
        peg$c1 = function(ex) {
              return {
                verb: null,
                expression: ex
              }
            },
        peg$c2 = function(verb, rest) {
              return {
                verb: verb,
                rest: rest
              };
            },
        peg$c3 = function(columns, from, where, groupBys, having, orderBy, limit) {
              return {
                verb: 'SELECT',
                expression: constructQuery(columns, from, where, groupBys, having, orderBy, limit),
                table: from
              };
            },
        peg$c4 = function(columns, where, groupBys, having, orderBy, limit) { return constructQuery(columns, null, where, groupBys, having, orderBy, limit); },
        peg$c5 = function() { return '*'; },
        peg$c6 = ",",
        peg$c7 = { type: "literal", value: ",", description: "\",\"" },
        peg$c8 = function(head, tail) { return makeListMap3(head, tail); },
        peg$c9 = function(ex, as) {
              return new ApplyAction({
                name: as || text().replace(/^\W+|\W+$/g, '').replace(/\W+/g, '_'),
                expression: ex
              });
            },
        peg$c10 = function(name) { return name; },
        peg$c11 = function(table) { return table; },
        peg$c12 = function(filter) { return filter; },
        peg$c13 = function(having) { return new FilterAction({ expression: having }); },
        peg$c14 = function(orderBy, direction, tail) {
              if (tail.length) error('plywood does not currently support multi-column ORDER BYs');
              return new SortAction({ expression: orderBy, direction: direction || 'ascending' });
            },
        peg$c15 = function(dir) { return dir; },
        peg$c16 = function(limit) { return new LimitAction({ limit: limit }); },
        peg$c17 = ";",
        peg$c18 = { type: "literal", value: ";", description: "\";\"" },
        peg$c19 = { type: "any", description: "any character" },
        peg$c20 = function(rest) { return rest; },
        peg$c21 = function(head, tail) { return naryExpressionFactory('or', head, tail); },
        peg$c22 = function(head, tail) { return naryExpressionFactory('and', head, tail); },
        peg$c23 = function(ex) { return ex.not(); },
        peg$c24 = function(lhs, not, start, end) {
              if (start.op !== 'literal') error('between start must be a literal');
              if (end.op !== 'literal') error('between end must be a literal');
              var ex = lhs.in({ start: start.value, end: end.value, bounds: '[]' });
              if (not) ex = ex.not();
              return ex;
            },
        peg$c25 = function(lhs, not, rhs) {
              var ex = lhs.is(rhs);
              if (not) ex = ex.not();
              return ex;
            },
        peg$c26 = function(lhs, not, list) {
              var ex = lhs.in(list);
              if (not) ex = ex.not();
              return ex;
            },
        peg$c27 = function(lhs, not, string) {
              var ex = lhs.contains(string, 'ignoreCase');
              if (not) ex = ex.not();
              return ex;
            },
        peg$c28 = function(lhs, not, string, escape) {
              var escapeStr = escape ? escape[3] : '\\';
              if (escapeStr.length > 1) error('Invalid escape string: ' + escapeStr);
              var ex = lhs.match(MatchAction.likeToRegExp(string, escapeStr));
              if (not) ex = ex.not();
              return ex;
            },
        peg$c29 = function(lhs, not, string) {
              var ex = lhs.match(string);
              if (not) ex = ex.not();
              return ex;
            },
        peg$c30 = function(lhs, rest) {
              if (!rest) return lhs;
              return lhs[rest[1]](rest[3]);
            },
        peg$c31 = "=",
        peg$c32 = { type: "literal", value: "=", description: "\"=\"" },
        peg$c33 = function() { return 'is'; },
        peg$c34 = "<>",
        peg$c35 = { type: "literal", value: "<>", description: "\"<>\"" },
        peg$c36 = function() { return 'isnt'; },
        peg$c37 = "!=",
        peg$c38 = { type: "literal", value: "!=", description: "\"!=\"" },
        peg$c39 = "<=",
        peg$c40 = { type: "literal", value: "<=", description: "\"<=\"" },
        peg$c41 = function() { return 'lessThanOrEqual'; },
        peg$c42 = ">=",
        peg$c43 = { type: "literal", value: ">=", description: "\">=\"" },
        peg$c44 = function() { return 'greaterThanOrEqual'; },
        peg$c45 = "<",
        peg$c46 = { type: "literal", value: "<", description: "\"<\"" },
        peg$c47 = function() { return 'lessThan'; },
        peg$c48 = ">",
        peg$c49 = { type: "literal", value: ">", description: "\">\"" },
        peg$c50 = function() { return 'greaterThan'; },
        peg$c51 = "(",
        peg$c52 = { type: "literal", value: "(", description: "\"(\"" },
        peg$c53 = ")",
        peg$c54 = { type: "literal", value: ")", description: "\")\"" },
        peg$c55 = function(head, tail) { return r(Set.fromJS(makeListMap3(head, tail))); },
        peg$c56 = function(head, tail) { return naryExpressionWithAltFactory('add', head, tail, '-', 'subtract'); },
        peg$c57 = /^[+\-]/,
        peg$c58 = { type: "class", value: "[+-]", description: "[+-]" },
        peg$c59 = /^[+]/,
        peg$c60 = { type: "class", value: "[+]", description: "[+]" },
        peg$c61 = function(op) { return op; },
        peg$c62 = function(head, tail) { return naryExpressionWithAltFactory('multiply', head, tail, '/', 'divide'); },
        peg$c63 = /^[*\/]/,
        peg$c64 = { type: "class", value: "[*/]", description: "[*/]" },
        peg$c65 = function(op, ex) {
              var negEx = ex.negate(); // Always negate (even with +) just to make sure it is possible
              return op === '-' ? negEx : ex;
            },
        peg$c66 = function(ex) { return ex; },
        peg$c67 = function(selectSubQuery) { return selectSubQuery; },
        peg$c68 = function(distinct, ex) {
              if (!ex || ex === '*') {
                if (distinct) error('COUNT DISTINCT must have expression');
                return dataRef.count();
              } else {
                return distinct ? dataRef.countDistinct(ex) : dataRef.filter(ex.isnt(null)).count()
              }
            },
        peg$c69 = function(fn, distinct, ex) {
              if (distinct) error('can not use DISTINCT for ' + fn + ' aggregator');
              return dataRef[fn](ex);
            },
        peg$c70 = function(ex, value) { return dataRef.quantile(ex, value); },
        peg$c71 = function(value) { return dataRef.custom(value); },
        peg$c72 = function(operand, size, offset) { return operand.numberBucket(size, offset); },
        peg$c73 = function(operand, duration, timezone) { return operand.timeBucket(duration, timezone); },
        peg$c74 = function(operand, part, timezone) { return operand.timePart(part, timezone); },
        peg$c75 = function(operand, position, length) { return operand.substr(position, length); },
        peg$c76 = function(operand, regexp) { return operand.extract(regexp); },
        peg$c77 = function(operand, lookup) { return operand.lookup(lookup); },
        peg$c78 = function(head, tail) { return Expression.concat(makeListMap3(head, tail)); },
        peg$c79 = function(ref) { return $(ref); },
        peg$c80 = ".",
        peg$c81 = { type: "literal", value: ".", description: "\".\"" },
        peg$c82 = function(name) {
              return name; // ToDo: do not ignore namespace
            },
        peg$c83 = function(name) { return reserved(name); },
        peg$c84 = function(name) { return name },
        peg$c85 = "`",
        peg$c86 = { type: "literal", value: "`", description: "\"`\"" },
        peg$c87 = /^[^`]/,
        peg$c88 = { type: "class", value: "[^`]", description: "[^`]" },
        peg$c89 = function(number) { return r(number); },
        peg$c90 = function(string) {
              if (dateRegExp.test(string)) {
                var date = new Date(string);
                if (!isNaN(date)) {
                  return r(date);
                } else {
                  return r(string);
                }
              } else {
                return r(string);
              }
            },
        peg$c91 = function(v) { return r(v); },
        peg$c92 = { type: "other", description: "String" },
        peg$c93 = "'",
        peg$c94 = { type: "literal", value: "'", description: "\"'\"" },
        peg$c95 = function(chars) { return chars; },
        peg$c96 = function(chars) { error("Unmatched single quote"); },
        peg$c97 = "\"",
        peg$c98 = { type: "literal", value: "\"", description: "\"\\\"\"" },
        peg$c99 = function(chars) { error("Unmatched double quote"); },
        peg$c100 = "null",
        peg$c101 = { type: "literal", value: "NULL", description: "\"NULL\"" },
        peg$c102 = function() { return null; },
        peg$c103 = "true",
        peg$c104 = { type: "literal", value: "TRUE", description: "\"TRUE\"" },
        peg$c105 = function() { return true; },
        peg$c106 = "false",
        peg$c107 = { type: "literal", value: "FALSE", description: "\"FALSE\"" },
        peg$c108 = function() { return false; },
        peg$c109 = "select",
        peg$c110 = { type: "literal", value: "SELECT", description: "\"SELECT\"" },
        peg$c111 = function() { return 'SELECT'; },
        peg$c112 = "update",
        peg$c113 = { type: "literal", value: "UPDATE", description: "\"UPDATE\"" },
        peg$c114 = function() { return 'UPDATE'; },
        peg$c115 = "show",
        peg$c116 = { type: "literal", value: "SHOW", description: "\"SHOW\"" },
        peg$c117 = function() { return 'SHOW'; },
        peg$c118 = "set",
        peg$c119 = { type: "literal", value: "SET", description: "\"SET\"" },
        peg$c120 = function() { return 'SET'; },
        peg$c121 = "from",
        peg$c122 = { type: "literal", value: "FROM", description: "\"FROM\"" },
        peg$c123 = "as",
        peg$c124 = { type: "literal", value: "AS", description: "\"AS\"" },
        peg$c125 = "on",
        peg$c126 = { type: "literal", value: "ON", description: "\"ON\"" },
        peg$c127 = "left",
        peg$c128 = { type: "literal", value: "LEFT", description: "\"LEFT\"" },
        peg$c129 = "inner",
        peg$c130 = { type: "literal", value: "INNER", description: "\"INNER\"" },
        peg$c131 = "join",
        peg$c132 = { type: "literal", value: "JOIN", description: "\"JOIN\"" },
        peg$c133 = "union",
        peg$c134 = { type: "literal", value: "UNION", description: "\"UNION\"" },
        peg$c135 = "where",
        peg$c136 = { type: "literal", value: "WHERE", description: "\"WHERE\"" },
        peg$c137 = "group",
        peg$c138 = { type: "literal", value: "GROUP", description: "\"GROUP\"" },
        peg$c139 = "by",
        peg$c140 = { type: "literal", value: "BY", description: "\"BY\"" },
        peg$c141 = "order",
        peg$c142 = { type: "literal", value: "ORDER", description: "\"ORDER\"" },
        peg$c143 = "having",
        peg$c144 = { type: "literal", value: "HAVING", description: "\"HAVING\"" },
        peg$c145 = "limit",
        peg$c146 = { type: "literal", value: "LIMIT", description: "\"LIMIT\"" },
        peg$c147 = "asc",
        peg$c148 = { type: "literal", value: "ASC", description: "\"ASC\"" },
        peg$c149 = function() { return SortAction.ASCENDING;  },
        peg$c150 = "desc",
        peg$c151 = { type: "literal", value: "DESC", description: "\"DESC\"" },
        peg$c152 = function() { return SortAction.DESCENDING; },
        peg$c153 = "between",
        peg$c154 = { type: "literal", value: "BETWEEN", description: "\"BETWEEN\"" },
        peg$c155 = "in",
        peg$c156 = { type: "literal", value: "IN", description: "\"IN\"" },
        peg$c157 = "is",
        peg$c158 = { type: "literal", value: "IS", description: "\"IS\"" },
        peg$c159 = "like",
        peg$c160 = { type: "literal", value: "LIKE", description: "\"LIKE\"" },
        peg$c161 = "contains",
        peg$c162 = { type: "literal", value: "CONTAINS", description: "\"CONTAINS\"" },
        peg$c163 = "regexp",
        peg$c164 = { type: "literal", value: "REGEXP", description: "\"REGEXP\"" },
        peg$c165 = "escape",
        peg$c166 = { type: "literal", value: "ESCAPE", description: "\"ESCAPE\"" },
        peg$c167 = "not",
        peg$c168 = { type: "literal", value: "NOT", description: "\"NOT\"" },
        peg$c169 = "and",
        peg$c170 = { type: "literal", value: "AND", description: "\"AND\"" },
        peg$c171 = "or",
        peg$c172 = { type: "literal", value: "OR", description: "\"OR\"" },
        peg$c173 = "distinct",
        peg$c174 = { type: "literal", value: "DISTINCT", description: "\"DISTINCT\"" },
        peg$c175 = "*",
        peg$c176 = { type: "literal", value: "*", description: "\"*\"" },
        peg$c177 = "count",
        peg$c178 = { type: "literal", value: "COUNT", description: "\"COUNT\"" },
        peg$c179 = function() { return 'count'; },
        peg$c180 = "count_distinct",
        peg$c181 = { type: "literal", value: "COUNT_DISTINCT", description: "\"COUNT_DISTINCT\"" },
        peg$c182 = function() { return 'countDistinct'; },
        peg$c183 = "sum",
        peg$c184 = { type: "literal", value: "SUM", description: "\"SUM\"" },
        peg$c185 = function() { return 'sum'; },
        peg$c186 = "avg",
        peg$c187 = { type: "literal", value: "AVG", description: "\"AVG\"" },
        peg$c188 = function() { return 'average'; },
        peg$c189 = "min",
        peg$c190 = { type: "literal", value: "MIN", description: "\"MIN\"" },
        peg$c191 = function() { return 'min'; },
        peg$c192 = "max",
        peg$c193 = { type: "literal", value: "MAX", description: "\"MAX\"" },
        peg$c194 = function() { return 'max'; },
        peg$c195 = "quantile",
        peg$c196 = { type: "literal", value: "QUANTILE", description: "\"QUANTILE\"" },
        peg$c197 = function() { return 'quantile'; },
        peg$c198 = "custom",
        peg$c199 = { type: "literal", value: "CUSTOM", description: "\"CUSTOM\"" },
        peg$c200 = function() { return 'custom'; },
        peg$c201 = "time_bucket",
        peg$c202 = { type: "literal", value: "TIME_BUCKET", description: "\"TIME_BUCKET\"" },
        peg$c203 = "number_bucket",
        peg$c204 = { type: "literal", value: "NUMBER_BUCKET", description: "\"NUMBER_BUCKET\"" },
        peg$c205 = "time_part",
        peg$c206 = { type: "literal", value: "TIME_PART", description: "\"TIME_PART\"" },
        peg$c207 = "substr",
        peg$c208 = { type: "literal", value: "SUBSTR", description: "\"SUBSTR\"" },
        peg$c209 = "ing",
        peg$c210 = { type: "literal", value: "ING", description: "\"ING\"" },
        peg$c211 = "extract",
        peg$c212 = { type: "literal", value: "EXTRACT", description: "\"EXTRACT\"" },
        peg$c213 = "concat",
        peg$c214 = { type: "literal", value: "CONCAT", description: "\"CONCAT\"" },
        peg$c215 = "lookup",
        peg$c216 = { type: "literal", value: "LOOKUP", description: "\"LOOKUP\"" },
        peg$c217 = /^[A-Za-z_]/,
        peg$c218 = { type: "class", value: "[A-Za-z_]", description: "[A-Za-z_]" },
        peg$c219 = { type: "other", description: "Number" },
        peg$c220 = function(n) { return parseFloat(n); },
        peg$c221 = "-",
        peg$c222 = { type: "literal", value: "-", description: "\"-\"" },
        peg$c223 = /^[1-9]/,
        peg$c224 = { type: "class", value: "[1-9]", description: "[1-9]" },
        peg$c225 = "e",
        peg$c226 = { type: "literal", value: "e", description: "\"e\"" },
        peg$c227 = /^[0-9]/,
        peg$c228 = { type: "class", value: "[0-9]", description: "[0-9]" },
        peg$c229 = { type: "other", description: "(" },
        peg$c230 = { type: "other", description: ")" },
        peg$c231 = { type: "other", description: "Name" },
        peg$c232 = /^[a-zA-Z_]/,
        peg$c233 = { type: "class", value: "[a-zA-Z_]", description: "[a-zA-Z_]" },
        peg$c234 = /^[a-z0-9A-Z_]/,
        peg$c235 = { type: "class", value: "[a-z0-9A-Z_]", description: "[a-z0-9A-Z_]" },
        peg$c236 = { type: "other", description: "NotSQuote" },
        peg$c237 = /^[^']/,
        peg$c238 = { type: "class", value: "[^']", description: "[^']" },
        peg$c239 = { type: "other", description: "NotDQuote" },
        peg$c240 = /^[^"]/,
        peg$c241 = { type: "class", value: "[^\"]", description: "[^\"]" },
        peg$c242 = { type: "other", description: "Whitespace" },
        peg$c243 = /^[ \t\r\n]/,
        peg$c244 = { type: "class", value: "[ \\t\\r\\n]", description: "[ \\t\\r\\n]" },
        peg$c245 = { type: "other", description: "Mandatory Whitespace" },
        peg$c246 = "--",
        peg$c247 = { type: "literal", value: "--", description: "\"--\"" },
        peg$c248 = /^[\n\r]/,
        peg$c249 = { type: "class", value: "[\\n\\r]", description: "[\\n\\r]" },

        peg$currPos          = 0,
        peg$savedPos         = 0,
        peg$posDetailsCache  = [{ line: 1, column: 1, seenCR: false }],
        peg$maxFailPos       = 0,
        peg$maxFailExpected  = [],
        peg$silentFails      = 0,

        peg$result;

    if ("startRule" in options) {
      if (!(options.startRule in peg$startRuleFunctions)) {
        throw new Error("Can't start parsing from rule \"" + options.startRule + "\".");
      }

      peg$startRuleFunction = peg$startRuleFunctions[options.startRule];
    }

    function text() {
      return input.substring(peg$savedPos, peg$currPos);
    }

    function location() {
      return peg$computeLocation(peg$savedPos, peg$currPos);
    }

    function expected(description) {
      throw peg$buildException(
        null,
        [{ type: "other", description: description }],
        input.substring(peg$savedPos, peg$currPos),
        peg$computeLocation(peg$savedPos, peg$currPos)
      );
    }

    function error(message) {
      throw peg$buildException(
        message,
        null,
        input.substring(peg$savedPos, peg$currPos),
        peg$computeLocation(peg$savedPos, peg$currPos)
      );
    }

    function peg$computePosDetails(pos) {
      var details = peg$posDetailsCache[pos],
          p, ch;

      if (details) {
        return details;
      } else {
        p = pos - 1;
        while (!peg$posDetailsCache[p]) {
          p--;
        }

        details = peg$posDetailsCache[p];
        details = {
          line:   details.line,
          column: details.column,
          seenCR: details.seenCR
        };

        while (p < pos) {
          ch = input.charAt(p);
          if (ch === "\n") {
            if (!details.seenCR) { details.line++; }
            details.column = 1;
            details.seenCR = false;
          } else if (ch === "\r" || ch === "\u2028" || ch === "\u2029") {
            details.line++;
            details.column = 1;
            details.seenCR = true;
          } else {
            details.column++;
            details.seenCR = false;
          }

          p++;
        }

        peg$posDetailsCache[pos] = details;
        return details;
      }
    }

    function peg$computeLocation(startPos, endPos) {
      var startPosDetails = peg$computePosDetails(startPos),
          endPosDetails   = peg$computePosDetails(endPos);

      return {
        start: {
          offset: startPos,
          line:   startPosDetails.line,
          column: startPosDetails.column
        },
        end: {
          offset: endPos,
          line:   endPosDetails.line,
          column: endPosDetails.column
        }
      };
    }

    function peg$fail(expected) {
      if (peg$currPos < peg$maxFailPos) { return; }

      if (peg$currPos > peg$maxFailPos) {
        peg$maxFailPos = peg$currPos;
        peg$maxFailExpected = [];
      }

      peg$maxFailExpected.push(expected);
    }

    function peg$buildException(message, expected, found, location) {
      function cleanupExpected(expected) {
        var i = 1;

        expected.sort(function(a, b) {
          if (a.description < b.description) {
            return -1;
          } else if (a.description > b.description) {
            return 1;
          } else {
            return 0;
          }
        });

        while (i < expected.length) {
          if (expected[i - 1] === expected[i]) {
            expected.splice(i, 1);
          } else {
            i++;
          }
        }
      }

      function buildMessage(expected, found) {
        function stringEscape(s) {
          function hex(ch) { return ch.charCodeAt(0).toString(16).toUpperCase(); }

          return s
            .replace(/\\/g,   '\\\\')
            .replace(/"/g,    '\\"')
            .replace(/\x08/g, '\\b')
            .replace(/\t/g,   '\\t')
            .replace(/\n/g,   '\\n')
            .replace(/\f/g,   '\\f')
            .replace(/\r/g,   '\\r')
            .replace(/[\x00-\x07\x0B\x0E\x0F]/g, function(ch) { return '\\x0' + hex(ch); })
            .replace(/[\x10-\x1F\x80-\xFF]/g,    function(ch) { return '\\x'  + hex(ch); })
            .replace(/[\u0100-\u0FFF]/g,         function(ch) { return '\\u0' + hex(ch); })
            .replace(/[\u1000-\uFFFF]/g,         function(ch) { return '\\u'  + hex(ch); });
        }

        var expectedDescs = new Array(expected.length),
            expectedDesc, foundDesc, i;

        for (i = 0; i < expected.length; i++) {
          expectedDescs[i] = expected[i].description;
        }

        expectedDesc = expected.length > 1
          ? expectedDescs.slice(0, -1).join(", ")
              + " or "
              + expectedDescs[expected.length - 1]
          : expectedDescs[0];

        foundDesc = found ? "\"" + stringEscape(found) + "\"" : "end of input";

        return "Expected " + expectedDesc + " but " + foundDesc + " found.";
      }

      if (expected !== null) {
        cleanupExpected(expected);
      }

      return new peg$SyntaxError(
        message !== null ? message : buildMessage(expected, found),
        expected,
        found,
        location
      );
    }

    function peg$parsestart() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseSelectQuery();
        if (s2 !== peg$FAILED) {
          s3 = peg$parse_();
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c0(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parse_();
        if (s1 !== peg$FAILED) {
          s2 = peg$parseOtherQuery();
          if (s2 !== peg$FAILED) {
            s3 = peg$parse_();
            if (s3 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c0(s2);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parse_();
          if (s1 !== peg$FAILED) {
            s2 = peg$parseOrExpression();
            if (s2 !== peg$FAILED) {
              s3 = peg$parse_();
              if (s3 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c1(s2);
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        }
      }

      return s0;
    }

    function peg$parseOtherQuery() {
      var s0, s1, s2;

      s0 = peg$currPos;
      s1 = peg$parseUpdateToken();
      if (s1 === peg$FAILED) {
        s1 = peg$parseShowToken();
        if (s1 === peg$FAILED) {
          s1 = peg$parseSetToken();
        }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parseRest();
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c2(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseSelectQuery() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10;

      s0 = peg$currPos;
      s1 = peg$parseSelectToken();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseColumns();
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          s3 = peg$parseFromClause();
          if (s3 === peg$FAILED) {
            s3 = null;
          }
          if (s3 !== peg$FAILED) {
            s4 = peg$parseWhereClause();
            if (s4 === peg$FAILED) {
              s4 = null;
            }
            if (s4 !== peg$FAILED) {
              s5 = peg$parseGroupByClause();
              if (s5 === peg$FAILED) {
                s5 = null;
              }
              if (s5 !== peg$FAILED) {
                s6 = peg$parseHavingClause();
                if (s6 === peg$FAILED) {
                  s6 = null;
                }
                if (s6 !== peg$FAILED) {
                  s7 = peg$parseOrderByClause();
                  if (s7 === peg$FAILED) {
                    s7 = null;
                  }
                  if (s7 !== peg$FAILED) {
                    s8 = peg$parseLimitClause();
                    if (s8 === peg$FAILED) {
                      s8 = null;
                    }
                    if (s8 !== peg$FAILED) {
                      s9 = peg$parseQueryTerminator();
                      if (s9 === peg$FAILED) {
                        s9 = null;
                      }
                      if (s9 !== peg$FAILED) {
                        s10 = peg$parse_();
                        if (s10 !== peg$FAILED) {
                          peg$savedPos = s0;
                          s1 = peg$c3(s2, s3, s4, s5, s6, s7, s8);
                          s0 = s1;
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseSelectSubQuery() {
      var s0, s1, s2, s3, s4, s5, s6, s7;

      s0 = peg$currPos;
      s1 = peg$parseSelectToken();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseColumns();
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          s3 = peg$parseWhereClause();
          if (s3 === peg$FAILED) {
            s3 = null;
          }
          if (s3 !== peg$FAILED) {
            s4 = peg$parseGroupByClause();
            if (s4 !== peg$FAILED) {
              s5 = peg$parseHavingClause();
              if (s5 === peg$FAILED) {
                s5 = null;
              }
              if (s5 !== peg$FAILED) {
                s6 = peg$parseOrderByClause();
                if (s6 === peg$FAILED) {
                  s6 = null;
                }
                if (s6 !== peg$FAILED) {
                  s7 = peg$parseLimitClause();
                  if (s7 === peg$FAILED) {
                    s7 = null;
                  }
                  if (s7 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c4(s2, s3, s4, s5, s6, s7);
                    s0 = s1;
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseColumns() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8;

      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseStarToken();
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c5();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parse_();
        if (s1 !== peg$FAILED) {
          s2 = peg$parseColumn();
          if (s2 !== peg$FAILED) {
            s3 = [];
            s4 = peg$currPos;
            s5 = peg$parse_();
            if (s5 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 44) {
                s6 = peg$c6;
                peg$currPos++;
              } else {
                s6 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c7); }
              }
              if (s6 !== peg$FAILED) {
                s7 = peg$parse_();
                if (s7 !== peg$FAILED) {
                  s8 = peg$parseColumn();
                  if (s8 !== peg$FAILED) {
                    s5 = [s5, s6, s7, s8];
                    s4 = s5;
                  } else {
                    peg$currPos = s4;
                    s4 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s4;
                  s4 = peg$FAILED;
                }
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
            while (s4 !== peg$FAILED) {
              s3.push(s4);
              s4 = peg$currPos;
              s5 = peg$parse_();
              if (s5 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 44) {
                  s6 = peg$c6;
                  peg$currPos++;
                } else {
                  s6 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c7); }
                }
                if (s6 !== peg$FAILED) {
                  s7 = peg$parse_();
                  if (s7 !== peg$FAILED) {
                    s8 = peg$parseColumn();
                    if (s8 !== peg$FAILED) {
                      s5 = [s5, s6, s7, s8];
                      s4 = s5;
                    } else {
                      peg$currPos = s4;
                      s4 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s4;
                    s4 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s4;
                  s4 = peg$FAILED;
                }
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            }
            if (s3 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c8(s2, s3);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      }

      return s0;
    }

    function peg$parseColumn() {
      var s0, s1, s2;

      s0 = peg$currPos;
      s1 = peg$parseOrExpression();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseAs();
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c9(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseAs() {
      var s0, s1, s2, s3, s4;

      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseAsToken();
        if (s2 !== peg$FAILED) {
          s3 = peg$parse_();
          if (s3 !== peg$FAILED) {
            s4 = peg$parseString();
            if (s4 === peg$FAILED) {
              s4 = peg$parseRef();
            }
            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c10(s4);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseFromClause() {
      var s0, s1, s2, s3, s4;

      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseFromToken();
        if (s2 !== peg$FAILED) {
          s3 = peg$parse_();
          if (s3 !== peg$FAILED) {
            s4 = peg$parseNamespacedRef();
            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c11(s4);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseWhereClause() {
      var s0, s1, s2, s3, s4;

      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseWhereToken();
        if (s2 !== peg$FAILED) {
          s3 = peg$parse_();
          if (s3 !== peg$FAILED) {
            s4 = peg$parseOrExpression();
            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c12(s4);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseGroupByClause() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12;

      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseGroupToken();
        if (s2 !== peg$FAILED) {
          s3 = peg$parse__();
          if (s3 !== peg$FAILED) {
            s4 = peg$parseByToken();
            if (s4 !== peg$FAILED) {
              s5 = peg$parse_();
              if (s5 !== peg$FAILED) {
                s6 = peg$parseOrExpression();
                if (s6 !== peg$FAILED) {
                  s7 = [];
                  s8 = peg$currPos;
                  s9 = peg$parse_();
                  if (s9 !== peg$FAILED) {
                    if (input.charCodeAt(peg$currPos) === 44) {
                      s10 = peg$c6;
                      peg$currPos++;
                    } else {
                      s10 = peg$FAILED;
                      if (peg$silentFails === 0) { peg$fail(peg$c7); }
                    }
                    if (s10 !== peg$FAILED) {
                      s11 = peg$parse_();
                      if (s11 !== peg$FAILED) {
                        s12 = peg$parseOrExpression();
                        if (s12 !== peg$FAILED) {
                          s9 = [s9, s10, s11, s12];
                          s8 = s9;
                        } else {
                          peg$currPos = s8;
                          s8 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s8;
                        s8 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s8;
                      s8 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s8;
                    s8 = peg$FAILED;
                  }
                  while (s8 !== peg$FAILED) {
                    s7.push(s8);
                    s8 = peg$currPos;
                    s9 = peg$parse_();
                    if (s9 !== peg$FAILED) {
                      if (input.charCodeAt(peg$currPos) === 44) {
                        s10 = peg$c6;
                        peg$currPos++;
                      } else {
                        s10 = peg$FAILED;
                        if (peg$silentFails === 0) { peg$fail(peg$c7); }
                      }
                      if (s10 !== peg$FAILED) {
                        s11 = peg$parse_();
                        if (s11 !== peg$FAILED) {
                          s12 = peg$parseOrExpression();
                          if (s12 !== peg$FAILED) {
                            s9 = [s9, s10, s11, s12];
                            s8 = s9;
                          } else {
                            peg$currPos = s8;
                            s8 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s8;
                          s8 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s8;
                        s8 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s8;
                      s8 = peg$FAILED;
                    }
                  }
                  if (s7 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c8(s6, s7);
                    s0 = s1;
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseHavingClause() {
      var s0, s1, s2, s3, s4;

      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseHavingToken();
        if (s2 !== peg$FAILED) {
          s3 = peg$parse_();
          if (s3 !== peg$FAILED) {
            s4 = peg$parseOrExpression();
            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c13(s4);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseOrderByClause() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14;

      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseOrderToken();
        if (s2 !== peg$FAILED) {
          s3 = peg$parse__();
          if (s3 !== peg$FAILED) {
            s4 = peg$parseByToken();
            if (s4 !== peg$FAILED) {
              s5 = peg$parse_();
              if (s5 !== peg$FAILED) {
                s6 = peg$parseOrExpression();
                if (s6 !== peg$FAILED) {
                  s7 = peg$parseDirection();
                  if (s7 === peg$FAILED) {
                    s7 = null;
                  }
                  if (s7 !== peg$FAILED) {
                    s8 = [];
                    s9 = peg$currPos;
                    s10 = peg$parse_();
                    if (s10 !== peg$FAILED) {
                      if (input.charCodeAt(peg$currPos) === 44) {
                        s11 = peg$c6;
                        peg$currPos++;
                      } else {
                        s11 = peg$FAILED;
                        if (peg$silentFails === 0) { peg$fail(peg$c7); }
                      }
                      if (s11 !== peg$FAILED) {
                        s12 = peg$parse_();
                        if (s12 !== peg$FAILED) {
                          s13 = peg$parseOrExpression();
                          if (s13 !== peg$FAILED) {
                            s14 = peg$parseDirection();
                            if (s14 === peg$FAILED) {
                              s14 = null;
                            }
                            if (s14 !== peg$FAILED) {
                              s10 = [s10, s11, s12, s13, s14];
                              s9 = s10;
                            } else {
                              peg$currPos = s9;
                              s9 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s9;
                            s9 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s9;
                          s9 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s9;
                        s9 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s9;
                      s9 = peg$FAILED;
                    }
                    while (s9 !== peg$FAILED) {
                      s8.push(s9);
                      s9 = peg$currPos;
                      s10 = peg$parse_();
                      if (s10 !== peg$FAILED) {
                        if (input.charCodeAt(peg$currPos) === 44) {
                          s11 = peg$c6;
                          peg$currPos++;
                        } else {
                          s11 = peg$FAILED;
                          if (peg$silentFails === 0) { peg$fail(peg$c7); }
                        }
                        if (s11 !== peg$FAILED) {
                          s12 = peg$parse_();
                          if (s12 !== peg$FAILED) {
                            s13 = peg$parseOrExpression();
                            if (s13 !== peg$FAILED) {
                              s14 = peg$parseDirection();
                              if (s14 === peg$FAILED) {
                                s14 = null;
                              }
                              if (s14 !== peg$FAILED) {
                                s10 = [s10, s11, s12, s13, s14];
                                s9 = s10;
                              } else {
                                peg$currPos = s9;
                                s9 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s9;
                              s9 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s9;
                            s9 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s9;
                          s9 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s9;
                        s9 = peg$FAILED;
                      }
                    }
                    if (s8 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c14(s6, s7, s8);
                      s0 = s1;
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseDirection() {
      var s0, s1, s2;

      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseAscToken();
        if (s2 === peg$FAILED) {
          s2 = peg$parseDescToken();
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c15(s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseLimitClause() {
      var s0, s1, s2, s3, s4;

      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseLimitToken();
        if (s2 !== peg$FAILED) {
          s3 = peg$parse_();
          if (s3 !== peg$FAILED) {
            s4 = peg$parseNumber();
            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c16(s4);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseQueryTerminator() {
      var s0, s1, s2;

      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 59) {
          s2 = peg$c17;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c18); }
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseRest() {
      var s0, s1, s2, s3, s4;

      s0 = peg$currPos;
      s1 = peg$parse__();
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        s3 = [];
        if (input.length > peg$currPos) {
          s4 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s4 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c19); }
        }
        while (s4 !== peg$FAILED) {
          s3.push(s4);
          if (input.length > peg$currPos) {
            s4 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s4 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c19); }
          }
        }
        if (s3 !== peg$FAILED) {
          s2 = input.substring(s2, peg$currPos);
        } else {
          s2 = s3;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c20(s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseOrExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7;

      s0 = peg$currPos;
      s1 = peg$parseAndExpression();
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseOrToken();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseAndExpression();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$currPos;
          s4 = peg$parse_();
          if (s4 !== peg$FAILED) {
            s5 = peg$parseOrToken();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 !== peg$FAILED) {
                s7 = peg$parseAndExpression();
                if (s7 !== peg$FAILED) {
                  s4 = [s4, s5, s6, s7];
                  s3 = s4;
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c21(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseAndExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7;

      s0 = peg$currPos;
      s1 = peg$parseNotExpression();
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseAndToken();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseNotExpression();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$currPos;
          s4 = peg$parse_();
          if (s4 !== peg$FAILED) {
            s5 = peg$parseAndToken();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 !== peg$FAILED) {
                s7 = peg$parseNotExpression();
                if (s7 !== peg$FAILED) {
                  s4 = [s4, s5, s6, s7];
                  s3 = s4;
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c22(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseNotExpression() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      s1 = peg$parseNotToken();
      if (s1 !== peg$FAILED) {
        s2 = peg$parse_();
        if (s2 !== peg$FAILED) {
          s3 = peg$parseComparisonExpression();
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c23(s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$parseComparisonExpression();
      }

      return s0;
    }

    function peg$parseComparisonExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11;

      s0 = peg$currPos;
      s1 = peg$parseAdditiveExpression();
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        s3 = peg$parse_();
        if (s3 !== peg$FAILED) {
          s4 = peg$parseNotToken();
          if (s4 !== peg$FAILED) {
            s3 = [s3, s4];
            s2 = s3;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          s3 = peg$parse__();
          if (s3 !== peg$FAILED) {
            s4 = peg$parseBetweenToken();
            if (s4 !== peg$FAILED) {
              s5 = peg$parse__();
              if (s5 !== peg$FAILED) {
                s6 = peg$parseAdditiveExpression();
                if (s6 !== peg$FAILED) {
                  s7 = peg$parse__();
                  if (s7 !== peg$FAILED) {
                    s8 = peg$parseAndToken();
                    if (s8 !== peg$FAILED) {
                      s9 = peg$parse__();
                      if (s9 !== peg$FAILED) {
                        s10 = peg$parseAdditiveExpression();
                        if (s10 !== peg$FAILED) {
                          peg$savedPos = s0;
                          s1 = peg$c24(s1, s2, s6, s10);
                          s0 = s1;
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parseAdditiveExpression();
        if (s1 !== peg$FAILED) {
          s2 = peg$parse_();
          if (s2 !== peg$FAILED) {
            s3 = peg$parseIsToken();
            if (s3 !== peg$FAILED) {
              s4 = peg$currPos;
              s5 = peg$parse_();
              if (s5 !== peg$FAILED) {
                s6 = peg$parseNotToken();
                if (s6 !== peg$FAILED) {
                  s5 = [s5, s6];
                  s4 = s5;
                } else {
                  peg$currPos = s4;
                  s4 = peg$FAILED;
                }
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
              if (s4 === peg$FAILED) {
                s4 = null;
              }
              if (s4 !== peg$FAILED) {
                s5 = peg$parse_();
                if (s5 !== peg$FAILED) {
                  s6 = peg$parseLiteralExpression();
                  if (s6 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c25(s1, s4, s6);
                    s0 = s1;
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseAdditiveExpression();
          if (s1 !== peg$FAILED) {
            s2 = peg$currPos;
            s3 = peg$parse_();
            if (s3 !== peg$FAILED) {
              s4 = peg$parseNotToken();
              if (s4 !== peg$FAILED) {
                s3 = [s3, s4];
                s2 = s3;
              } else {
                peg$currPos = s2;
                s2 = peg$FAILED;
              }
            } else {
              peg$currPos = s2;
              s2 = peg$FAILED;
            }
            if (s2 === peg$FAILED) {
              s2 = null;
            }
            if (s2 !== peg$FAILED) {
              s3 = peg$parse_();
              if (s3 !== peg$FAILED) {
                s4 = peg$parseInToken();
                if (s4 !== peg$FAILED) {
                  s5 = peg$parse_();
                  if (s5 !== peg$FAILED) {
                    s6 = peg$parseListLiteral();
                    if (s6 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c26(s1, s2, s6);
                      s0 = s1;
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            s1 = peg$parseAdditiveExpression();
            if (s1 !== peg$FAILED) {
              s2 = peg$currPos;
              s3 = peg$parse_();
              if (s3 !== peg$FAILED) {
                s4 = peg$parseNotToken();
                if (s4 !== peg$FAILED) {
                  s3 = [s3, s4];
                  s2 = s3;
                } else {
                  peg$currPos = s2;
                  s2 = peg$FAILED;
                }
              } else {
                peg$currPos = s2;
                s2 = peg$FAILED;
              }
              if (s2 === peg$FAILED) {
                s2 = null;
              }
              if (s2 !== peg$FAILED) {
                s3 = peg$parse_();
                if (s3 !== peg$FAILED) {
                  s4 = peg$parseContainsToken();
                  if (s4 !== peg$FAILED) {
                    s5 = peg$parse_();
                    if (s5 !== peg$FAILED) {
                      s6 = peg$parseString();
                      if (s6 !== peg$FAILED) {
                        peg$savedPos = s0;
                        s1 = peg$c27(s1, s2, s6);
                        s0 = s1;
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              s1 = peg$parseAdditiveExpression();
              if (s1 !== peg$FAILED) {
                s2 = peg$currPos;
                s3 = peg$parse_();
                if (s3 !== peg$FAILED) {
                  s4 = peg$parseNotToken();
                  if (s4 !== peg$FAILED) {
                    s3 = [s3, s4];
                    s2 = s3;
                  } else {
                    peg$currPos = s2;
                    s2 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s2;
                  s2 = peg$FAILED;
                }
                if (s2 === peg$FAILED) {
                  s2 = null;
                }
                if (s2 !== peg$FAILED) {
                  s3 = peg$parse_();
                  if (s3 !== peg$FAILED) {
                    s4 = peg$parseLikeToken();
                    if (s4 !== peg$FAILED) {
                      s5 = peg$parse_();
                      if (s5 !== peg$FAILED) {
                        s6 = peg$parseString();
                        if (s6 !== peg$FAILED) {
                          s7 = peg$currPos;
                          s8 = peg$parse_();
                          if (s8 !== peg$FAILED) {
                            s9 = peg$parseEscapeToken();
                            if (s9 !== peg$FAILED) {
                              s10 = peg$parse_();
                              if (s10 !== peg$FAILED) {
                                s11 = peg$parseString();
                                if (s11 !== peg$FAILED) {
                                  s8 = [s8, s9, s10, s11];
                                  s7 = s8;
                                } else {
                                  peg$currPos = s7;
                                  s7 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s7;
                                s7 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s7;
                              s7 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s7;
                            s7 = peg$FAILED;
                          }
                          if (s7 === peg$FAILED) {
                            s7 = null;
                          }
                          if (s7 !== peg$FAILED) {
                            peg$savedPos = s0;
                            s1 = peg$c28(s1, s2, s6, s7);
                            s0 = s1;
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                s1 = peg$parseAdditiveExpression();
                if (s1 !== peg$FAILED) {
                  s2 = peg$currPos;
                  s3 = peg$parse_();
                  if (s3 !== peg$FAILED) {
                    s4 = peg$parseNotToken();
                    if (s4 !== peg$FAILED) {
                      s3 = [s3, s4];
                      s2 = s3;
                    } else {
                      peg$currPos = s2;
                      s2 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s2;
                    s2 = peg$FAILED;
                  }
                  if (s2 === peg$FAILED) {
                    s2 = null;
                  }
                  if (s2 !== peg$FAILED) {
                    s3 = peg$parse_();
                    if (s3 !== peg$FAILED) {
                      s4 = peg$parseRegExpToken();
                      if (s4 !== peg$FAILED) {
                        s5 = peg$parse_();
                        if (s5 !== peg$FAILED) {
                          s6 = peg$parseString();
                          if (s6 !== peg$FAILED) {
                            peg$savedPos = s0;
                            s1 = peg$c29(s1, s2, s6);
                            s0 = s1;
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
                if (s0 === peg$FAILED) {
                  s0 = peg$currPos;
                  s1 = peg$parseAdditiveExpression();
                  if (s1 !== peg$FAILED) {
                    s2 = peg$currPos;
                    s3 = peg$parse_();
                    if (s3 !== peg$FAILED) {
                      s4 = peg$parseComparisonOp();
                      if (s4 !== peg$FAILED) {
                        s5 = peg$parse_();
                        if (s5 !== peg$FAILED) {
                          s6 = peg$parseAdditiveExpression();
                          if (s6 !== peg$FAILED) {
                            s3 = [s3, s4, s5, s6];
                            s2 = s3;
                          } else {
                            peg$currPos = s2;
                            s2 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s2;
                          s2 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s2;
                        s2 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s2;
                      s2 = peg$FAILED;
                    }
                    if (s2 === peg$FAILED) {
                      s2 = null;
                    }
                    if (s2 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c30(s1, s2);
                      s0 = s1;
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                }
              }
            }
          }
        }
      }

      return s0;
    }

    function peg$parseComparisonOp() {
      var s0, s1;

      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 61) {
        s1 = peg$c31;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c32); }
      }
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c33();
      }
      s0 = s1;
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        if (input.substr(peg$currPos, 2) === peg$c34) {
          s1 = peg$c34;
          peg$currPos += 2;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c35); }
        }
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c36();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          if (input.substr(peg$currPos, 2) === peg$c37) {
            s1 = peg$c37;
            peg$currPos += 2;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c38); }
          }
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c36();
          }
          s0 = s1;
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            if (input.substr(peg$currPos, 2) === peg$c39) {
              s1 = peg$c39;
              peg$currPos += 2;
            } else {
              s1 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c40); }
            }
            if (s1 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c41();
            }
            s0 = s1;
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              if (input.substr(peg$currPos, 2) === peg$c42) {
                s1 = peg$c42;
                peg$currPos += 2;
              } else {
                s1 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c43); }
              }
              if (s1 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c44();
              }
              s0 = s1;
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                if (input.charCodeAt(peg$currPos) === 60) {
                  s1 = peg$c45;
                  peg$currPos++;
                } else {
                  s1 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c46); }
                }
                if (s1 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c47();
                }
                s0 = s1;
                if (s0 === peg$FAILED) {
                  s0 = peg$currPos;
                  if (input.charCodeAt(peg$currPos) === 62) {
                    s1 = peg$c48;
                    peg$currPos++;
                  } else {
                    s1 = peg$FAILED;
                    if (peg$silentFails === 0) { peg$fail(peg$c49); }
                  }
                  if (s1 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c50();
                  }
                  s0 = s1;
                }
              }
            }
          }
        }
      }

      return s0;
    }

    function peg$parseListLiteral() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8;

      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 40) {
        s1 = peg$c51;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c52); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parseStringOrNumber();
        if (s2 !== peg$FAILED) {
          s3 = [];
          s4 = peg$currPos;
          s5 = peg$parse_();
          if (s5 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 44) {
              s6 = peg$c6;
              peg$currPos++;
            } else {
              s6 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c7); }
            }
            if (s6 !== peg$FAILED) {
              s7 = peg$parse_();
              if (s7 !== peg$FAILED) {
                s8 = peg$parseStringOrNumber();
                if (s8 !== peg$FAILED) {
                  s5 = [s5, s6, s7, s8];
                  s4 = s5;
                } else {
                  peg$currPos = s4;
                  s4 = peg$FAILED;
                }
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          } else {
            peg$currPos = s4;
            s4 = peg$FAILED;
          }
          while (s4 !== peg$FAILED) {
            s3.push(s4);
            s4 = peg$currPos;
            s5 = peg$parse_();
            if (s5 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 44) {
                s6 = peg$c6;
                peg$currPos++;
              } else {
                s6 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c7); }
              }
              if (s6 !== peg$FAILED) {
                s7 = peg$parse_();
                if (s7 !== peg$FAILED) {
                  s8 = peg$parseStringOrNumber();
                  if (s8 !== peg$FAILED) {
                    s5 = [s5, s6, s7, s8];
                    s4 = s5;
                  } else {
                    peg$currPos = s4;
                    s4 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s4;
                  s4 = peg$FAILED;
                }
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          }
          if (s3 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 41) {
              s4 = peg$c53;
              peg$currPos++;
            } else {
              s4 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c54); }
            }
            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c55(s2, s3);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseAdditiveExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7;

      s0 = peg$currPos;
      s1 = peg$parseMultiplicativeExpression();
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseAdditiveOp();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseMultiplicativeExpression();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$currPos;
          s4 = peg$parse_();
          if (s4 !== peg$FAILED) {
            s5 = peg$parseAdditiveOp();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 !== peg$FAILED) {
                s7 = peg$parseMultiplicativeExpression();
                if (s7 !== peg$FAILED) {
                  s4 = [s4, s5, s6, s7];
                  s3 = s4;
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c56(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseAdditiveOp() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (peg$c57.test(input.charAt(peg$currPos))) {
        s1 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c58); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        if (peg$c59.test(input.charAt(peg$currPos))) {
          s3 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c60); }
        }
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c61(s1);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseMultiplicativeExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7;

      s0 = peg$currPos;
      s1 = peg$parseUnaryExpression();
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseMultiplicativeOp();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseUnaryExpression();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$currPos;
          s4 = peg$parse_();
          if (s4 !== peg$FAILED) {
            s5 = peg$parseMultiplicativeOp();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 !== peg$FAILED) {
                s7 = peg$parseUnaryExpression();
                if (s7 !== peg$FAILED) {
                  s4 = [s4, s5, s6, s7];
                  s3 = s4;
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c62(s1, s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseMultiplicativeOp() {
      var s0;

      if (peg$c63.test(input.charAt(peg$currPos))) {
        s0 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c64); }
      }

      return s0;
    }

    function peg$parseUnaryExpression() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      s1 = peg$parseAdditiveOp();
      if (s1 !== peg$FAILED) {
        s2 = peg$parse_();
        if (s2 !== peg$FAILED) {
          s3 = peg$parseBasicExpression();
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c65(s1, s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$parseBasicExpression();
      }

      return s0;
    }

    function peg$parseBasicExpression() {
      var s0, s1, s2, s3, s4;

      s0 = peg$parseLiteralExpression();
      if (s0 === peg$FAILED) {
        s0 = peg$parseAggregateExpression();
        if (s0 === peg$FAILED) {
          s0 = peg$parseFunctionCallExpression();
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            s1 = peg$parseOpenParen();
            if (s1 !== peg$FAILED) {
              s2 = peg$parse_();
              if (s2 !== peg$FAILED) {
                s3 = peg$parseOrExpression();
                if (s3 !== peg$FAILED) {
                  s4 = peg$parseCloseParen();
                  if (s4 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c66(s3);
                    s0 = s1;
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              s1 = peg$parseOpenParen();
              if (s1 !== peg$FAILED) {
                s2 = peg$parse_();
                if (s2 !== peg$FAILED) {
                  s3 = peg$parseSelectSubQuery();
                  if (s3 !== peg$FAILED) {
                    s4 = peg$parseCloseParen();
                    if (s4 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c67(s3);
                      s0 = s1;
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
              if (s0 === peg$FAILED) {
                s0 = peg$parseRefExpression();
              }
            }
          }
        }
      }

      return s0;
    }

    function peg$parseAggregateExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9;

      s0 = peg$currPos;
      s1 = peg$parseCountToken();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseOpenParen();
        if (s2 !== peg$FAILED) {
          s3 = peg$currPos;
          s4 = peg$parse_();
          if (s4 !== peg$FAILED) {
            s5 = peg$parseDistinctToken();
            if (s5 !== peg$FAILED) {
              s4 = [s4, s5];
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
          if (s3 === peg$FAILED) {
            s3 = null;
          }
          if (s3 !== peg$FAILED) {
            s4 = peg$parse_();
            if (s4 !== peg$FAILED) {
              s5 = peg$parseStarToken();
              if (s5 === peg$FAILED) {
                s5 = peg$parseOrExpression();
              }
              if (s5 === peg$FAILED) {
                s5 = null;
              }
              if (s5 !== peg$FAILED) {
                s6 = peg$parseCloseParen();
                if (s6 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c68(s3, s5);
                  s0 = s1;
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parseAggregateFn();
        if (s1 !== peg$FAILED) {
          s2 = peg$parseOpenParen();
          if (s2 !== peg$FAILED) {
            s3 = peg$currPos;
            s4 = peg$parse_();
            if (s4 !== peg$FAILED) {
              s5 = peg$parseDistinctToken();
              if (s5 !== peg$FAILED) {
                s4 = [s4, s5];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
            if (s3 === peg$FAILED) {
              s3 = null;
            }
            if (s3 !== peg$FAILED) {
              s4 = peg$parse_();
              if (s4 !== peg$FAILED) {
                s5 = peg$parseOrExpression();
                if (s5 !== peg$FAILED) {
                  s6 = peg$parseCloseParen();
                  if (s6 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c69(s1, s3, s5);
                    s0 = s1;
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseQuantileToken();
          if (s1 !== peg$FAILED) {
            s2 = peg$parseOpenParen();
            if (s2 !== peg$FAILED) {
              s3 = peg$parse_();
              if (s3 !== peg$FAILED) {
                s4 = peg$parseOrExpression();
                if (s4 !== peg$FAILED) {
                  s5 = peg$parse_();
                  if (s5 !== peg$FAILED) {
                    if (input.charCodeAt(peg$currPos) === 44) {
                      s6 = peg$c6;
                      peg$currPos++;
                    } else {
                      s6 = peg$FAILED;
                      if (peg$silentFails === 0) { peg$fail(peg$c7); }
                    }
                    if (s6 !== peg$FAILED) {
                      s7 = peg$parse_();
                      if (s7 !== peg$FAILED) {
                        s8 = peg$parseNumber();
                        if (s8 !== peg$FAILED) {
                          s9 = peg$parseCloseParen();
                          if (s9 !== peg$FAILED) {
                            peg$savedPos = s0;
                            s1 = peg$c70(s4, s8);
                            s0 = s1;
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            s1 = peg$parseCustomToken();
            if (s1 !== peg$FAILED) {
              s2 = peg$parseOpenParen();
              if (s2 !== peg$FAILED) {
                s3 = peg$parse_();
                if (s3 !== peg$FAILED) {
                  s4 = peg$parseString();
                  if (s4 !== peg$FAILED) {
                    s5 = peg$parseCloseParen();
                    if (s5 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c71(s4);
                      s0 = s1;
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          }
        }
      }

      return s0;
    }

    function peg$parseAggregateFn() {
      var s0;

      s0 = peg$parseSumToken();
      if (s0 === peg$FAILED) {
        s0 = peg$parseAvgToken();
        if (s0 === peg$FAILED) {
          s0 = peg$parseMinToken();
          if (s0 === peg$FAILED) {
            s0 = peg$parseMaxToken();
            if (s0 === peg$FAILED) {
              s0 = peg$parseCountDistinctToken();
            }
          }
        }
      }

      return s0;
    }

    function peg$parseFunctionCallExpression() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13;

      s0 = peg$currPos;
      s1 = peg$parseNumberBucketToken();
      if (s1 !== peg$FAILED) {
        s2 = peg$parseOpenParen();
        if (s2 !== peg$FAILED) {
          s3 = peg$parse_();
          if (s3 !== peg$FAILED) {
            s4 = peg$parseOrExpression();
            if (s4 !== peg$FAILED) {
              s5 = peg$parse_();
              if (s5 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 44) {
                  s6 = peg$c6;
                  peg$currPos++;
                } else {
                  s6 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c7); }
                }
                if (s6 !== peg$FAILED) {
                  s7 = peg$parse_();
                  if (s7 !== peg$FAILED) {
                    s8 = peg$parseNumber();
                    if (s8 !== peg$FAILED) {
                      s9 = peg$parse_();
                      if (s9 !== peg$FAILED) {
                        if (input.charCodeAt(peg$currPos) === 44) {
                          s10 = peg$c6;
                          peg$currPos++;
                        } else {
                          s10 = peg$FAILED;
                          if (peg$silentFails === 0) { peg$fail(peg$c7); }
                        }
                        if (s10 !== peg$FAILED) {
                          s11 = peg$parse_();
                          if (s11 !== peg$FAILED) {
                            s12 = peg$parseNumber();
                            if (s12 !== peg$FAILED) {
                              s13 = peg$parseCloseParen();
                              if (s13 !== peg$FAILED) {
                                peg$savedPos = s0;
                                s1 = peg$c72(s4, s8, s12);
                                s0 = s1;
                              } else {
                                peg$currPos = s0;
                                s0 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s0;
                              s0 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parseTimeBucketToken();
        if (s1 !== peg$FAILED) {
          s2 = peg$parseOpenParen();
          if (s2 !== peg$FAILED) {
            s3 = peg$parse_();
            if (s3 !== peg$FAILED) {
              s4 = peg$parseOrExpression();
              if (s4 !== peg$FAILED) {
                s5 = peg$parse_();
                if (s5 !== peg$FAILED) {
                  if (input.charCodeAt(peg$currPos) === 44) {
                    s6 = peg$c6;
                    peg$currPos++;
                  } else {
                    s6 = peg$FAILED;
                    if (peg$silentFails === 0) { peg$fail(peg$c7); }
                  }
                  if (s6 !== peg$FAILED) {
                    s7 = peg$parse_();
                    if (s7 !== peg$FAILED) {
                      s8 = peg$parseNameOrString();
                      if (s8 !== peg$FAILED) {
                        s9 = peg$parse_();
                        if (s9 !== peg$FAILED) {
                          if (input.charCodeAt(peg$currPos) === 44) {
                            s10 = peg$c6;
                            peg$currPos++;
                          } else {
                            s10 = peg$FAILED;
                            if (peg$silentFails === 0) { peg$fail(peg$c7); }
                          }
                          if (s10 !== peg$FAILED) {
                            s11 = peg$parse_();
                            if (s11 !== peg$FAILED) {
                              s12 = peg$parseNameOrString();
                              if (s12 !== peg$FAILED) {
                                s13 = peg$parseCloseParen();
                                if (s13 !== peg$FAILED) {
                                  peg$savedPos = s0;
                                  s1 = peg$c73(s4, s8, s12);
                                  s0 = s1;
                                } else {
                                  peg$currPos = s0;
                                  s0 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s0;
                                s0 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s0;
                              s0 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseTimePartToken();
          if (s1 !== peg$FAILED) {
            s2 = peg$parseOpenParen();
            if (s2 !== peg$FAILED) {
              s3 = peg$parse_();
              if (s3 !== peg$FAILED) {
                s4 = peg$parseOrExpression();
                if (s4 !== peg$FAILED) {
                  s5 = peg$parse_();
                  if (s5 !== peg$FAILED) {
                    if (input.charCodeAt(peg$currPos) === 44) {
                      s6 = peg$c6;
                      peg$currPos++;
                    } else {
                      s6 = peg$FAILED;
                      if (peg$silentFails === 0) { peg$fail(peg$c7); }
                    }
                    if (s6 !== peg$FAILED) {
                      s7 = peg$parse_();
                      if (s7 !== peg$FAILED) {
                        s8 = peg$parseNameOrString();
                        if (s8 !== peg$FAILED) {
                          s9 = peg$parse_();
                          if (s9 !== peg$FAILED) {
                            if (input.charCodeAt(peg$currPos) === 44) {
                              s10 = peg$c6;
                              peg$currPos++;
                            } else {
                              s10 = peg$FAILED;
                              if (peg$silentFails === 0) { peg$fail(peg$c7); }
                            }
                            if (s10 !== peg$FAILED) {
                              s11 = peg$parse_();
                              if (s11 !== peg$FAILED) {
                                s12 = peg$parseNameOrString();
                                if (s12 !== peg$FAILED) {
                                  s13 = peg$parseCloseParen();
                                  if (s13 !== peg$FAILED) {
                                    peg$savedPos = s0;
                                    s1 = peg$c74(s4, s8, s12);
                                    s0 = s1;
                                  } else {
                                    peg$currPos = s0;
                                    s0 = peg$FAILED;
                                  }
                                } else {
                                  peg$currPos = s0;
                                  s0 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s0;
                                s0 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s0;
                              s0 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            s1 = peg$parseSubstrToken();
            if (s1 !== peg$FAILED) {
              s2 = peg$parseOpenParen();
              if (s2 !== peg$FAILED) {
                s3 = peg$parse_();
                if (s3 !== peg$FAILED) {
                  s4 = peg$parseOrExpression();
                  if (s4 !== peg$FAILED) {
                    s5 = peg$parse_();
                    if (s5 !== peg$FAILED) {
                      if (input.charCodeAt(peg$currPos) === 44) {
                        s6 = peg$c6;
                        peg$currPos++;
                      } else {
                        s6 = peg$FAILED;
                        if (peg$silentFails === 0) { peg$fail(peg$c7); }
                      }
                      if (s6 !== peg$FAILED) {
                        s7 = peg$parse_();
                        if (s7 !== peg$FAILED) {
                          s8 = peg$parseNumber();
                          if (s8 !== peg$FAILED) {
                            s9 = peg$parse_();
                            if (s9 !== peg$FAILED) {
                              if (input.charCodeAt(peg$currPos) === 44) {
                                s10 = peg$c6;
                                peg$currPos++;
                              } else {
                                s10 = peg$FAILED;
                                if (peg$silentFails === 0) { peg$fail(peg$c7); }
                              }
                              if (s10 !== peg$FAILED) {
                                s11 = peg$parse_();
                                if (s11 !== peg$FAILED) {
                                  s12 = peg$parseNumber();
                                  if (s12 !== peg$FAILED) {
                                    s13 = peg$parseCloseParen();
                                    if (s13 !== peg$FAILED) {
                                      peg$savedPos = s0;
                                      s1 = peg$c75(s4, s8, s12);
                                      s0 = s1;
                                    } else {
                                      peg$currPos = s0;
                                      s0 = peg$FAILED;
                                    }
                                  } else {
                                    peg$currPos = s0;
                                    s0 = peg$FAILED;
                                  }
                                } else {
                                  peg$currPos = s0;
                                  s0 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s0;
                                s0 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s0;
                              s0 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              s1 = peg$parseExtractToken();
              if (s1 !== peg$FAILED) {
                s2 = peg$parseOpenParen();
                if (s2 !== peg$FAILED) {
                  s3 = peg$parse_();
                  if (s3 !== peg$FAILED) {
                    s4 = peg$parseOrExpression();
                    if (s4 !== peg$FAILED) {
                      s5 = peg$parse_();
                      if (s5 !== peg$FAILED) {
                        if (input.charCodeAt(peg$currPos) === 44) {
                          s6 = peg$c6;
                          peg$currPos++;
                        } else {
                          s6 = peg$FAILED;
                          if (peg$silentFails === 0) { peg$fail(peg$c7); }
                        }
                        if (s6 !== peg$FAILED) {
                          s7 = peg$parse_();
                          if (s7 !== peg$FAILED) {
                            s8 = peg$parseString();
                            if (s8 !== peg$FAILED) {
                              s9 = peg$parseCloseParen();
                              if (s9 !== peg$FAILED) {
                                peg$savedPos = s0;
                                s1 = peg$c76(s4, s8);
                                s0 = s1;
                              } else {
                                peg$currPos = s0;
                                s0 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s0;
                              s0 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                s1 = peg$parseLookupToken();
                if (s1 !== peg$FAILED) {
                  s2 = peg$parseOpenParen();
                  if (s2 !== peg$FAILED) {
                    s3 = peg$parse_();
                    if (s3 !== peg$FAILED) {
                      s4 = peg$parseOrExpression();
                      if (s4 !== peg$FAILED) {
                        s5 = peg$parse_();
                        if (s5 !== peg$FAILED) {
                          if (input.charCodeAt(peg$currPos) === 44) {
                            s6 = peg$c6;
                            peg$currPos++;
                          } else {
                            s6 = peg$FAILED;
                            if (peg$silentFails === 0) { peg$fail(peg$c7); }
                          }
                          if (s6 !== peg$FAILED) {
                            s7 = peg$parse_();
                            if (s7 !== peg$FAILED) {
                              s8 = peg$parseString();
                              if (s8 !== peg$FAILED) {
                                s9 = peg$parseCloseParen();
                                if (s9 !== peg$FAILED) {
                                  peg$savedPos = s0;
                                  s1 = peg$c77(s4, s8);
                                  s0 = s1;
                                } else {
                                  peg$currPos = s0;
                                  s0 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s0;
                                s0 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s0;
                              s0 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
                if (s0 === peg$FAILED) {
                  s0 = peg$currPos;
                  s1 = peg$parseConcatToken();
                  if (s1 !== peg$FAILED) {
                    s2 = peg$parseOpenParen();
                    if (s2 !== peg$FAILED) {
                      s3 = peg$parseOrExpression();
                      if (s3 !== peg$FAILED) {
                        s4 = [];
                        s5 = peg$currPos;
                        s6 = peg$parse_();
                        if (s6 !== peg$FAILED) {
                          if (input.charCodeAt(peg$currPos) === 44) {
                            s7 = peg$c6;
                            peg$currPos++;
                          } else {
                            s7 = peg$FAILED;
                            if (peg$silentFails === 0) { peg$fail(peg$c7); }
                          }
                          if (s7 !== peg$FAILED) {
                            s8 = peg$parse_();
                            if (s8 !== peg$FAILED) {
                              s9 = peg$parseOrExpression();
                              if (s9 !== peg$FAILED) {
                                s6 = [s6, s7, s8, s9];
                                s5 = s6;
                              } else {
                                peg$currPos = s5;
                                s5 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s5;
                              s5 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s5;
                            s5 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s5;
                          s5 = peg$FAILED;
                        }
                        while (s5 !== peg$FAILED) {
                          s4.push(s5);
                          s5 = peg$currPos;
                          s6 = peg$parse_();
                          if (s6 !== peg$FAILED) {
                            if (input.charCodeAt(peg$currPos) === 44) {
                              s7 = peg$c6;
                              peg$currPos++;
                            } else {
                              s7 = peg$FAILED;
                              if (peg$silentFails === 0) { peg$fail(peg$c7); }
                            }
                            if (s7 !== peg$FAILED) {
                              s8 = peg$parse_();
                              if (s8 !== peg$FAILED) {
                                s9 = peg$parseOrExpression();
                                if (s9 !== peg$FAILED) {
                                  s6 = [s6, s7, s8, s9];
                                  s5 = s6;
                                } else {
                                  peg$currPos = s5;
                                  s5 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s5;
                                s5 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s5;
                              s5 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s5;
                            s5 = peg$FAILED;
                          }
                        }
                        if (s4 !== peg$FAILED) {
                          s5 = peg$parseCloseParen();
                          if (s5 !== peg$FAILED) {
                            peg$savedPos = s0;
                            s1 = peg$c78(s3, s4);
                            s0 = s1;
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                }
              }
            }
          }
        }
      }

      return s0;
    }

    function peg$parseRefExpression() {
      var s0, s1;

      s0 = peg$currPos;
      s1 = peg$parseNamespacedRef();
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c79(s1);
      }
      s0 = s1;

      return s0;
    }

    function peg$parseNamespacedRef() {
      var s0, s1, s2, s3, s4, s5;

      s0 = peg$currPos;
      s1 = peg$currPos;
      s2 = peg$parseRef();
      if (s2 !== peg$FAILED) {
        s3 = peg$parse_();
        if (s3 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 46) {
            s4 = peg$c80;
            peg$currPos++;
          } else {
            s4 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c81); }
          }
          if (s4 !== peg$FAILED) {
            s5 = peg$parse_();
            if (s5 !== peg$FAILED) {
              s2 = [s2, s3, s4, s5];
              s1 = s2;
            } else {
              peg$currPos = s1;
              s1 = peg$FAILED;
            }
          } else {
            peg$currPos = s1;
            s1 = peg$FAILED;
          }
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
      if (s1 === peg$FAILED) {
        s1 = null;
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parseRef();
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c82(s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseRef() {
      var s0, s1, s2, s3, s4;

      s0 = peg$currPos;
      s1 = peg$parseName();
      if (s1 !== peg$FAILED) {
        peg$savedPos = peg$currPos;
        s2 = peg$c83(s1);
        if (s2) {
          s2 = peg$FAILED;
        } else {
          s2 = void 0;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c84(s1);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 96) {
          s1 = peg$c85;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c86); }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          s3 = [];
          if (peg$c87.test(input.charAt(peg$currPos))) {
            s4 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s4 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c88); }
          }
          if (s4 !== peg$FAILED) {
            while (s4 !== peg$FAILED) {
              s3.push(s4);
              if (peg$c87.test(input.charAt(peg$currPos))) {
                s4 = input.charAt(peg$currPos);
                peg$currPos++;
              } else {
                s4 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c88); }
              }
            }
          } else {
            s3 = peg$FAILED;
          }
          if (s3 !== peg$FAILED) {
            s2 = input.substring(s2, peg$currPos);
          } else {
            s2 = s3;
          }
          if (s2 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 96) {
              s3 = peg$c85;
              peg$currPos++;
            } else {
              s3 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c86); }
            }
            if (s3 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c84(s2);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      }

      return s0;
    }

    function peg$parseNameOrString() {
      var s0;

      s0 = peg$parseName();
      if (s0 === peg$FAILED) {
        s0 = peg$parseString();
      }

      return s0;
    }

    function peg$parseStringOrNumber() {
      var s0;

      s0 = peg$parseString();
      if (s0 === peg$FAILED) {
        s0 = peg$parseNumber();
      }

      return s0;
    }

    function peg$parseLiteralExpression() {
      var s0, s1;

      s0 = peg$currPos;
      s1 = peg$parseNumber();
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c89(s1);
      }
      s0 = s1;
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parseString();
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c90(s1);
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseNullToken();
          if (s1 === peg$FAILED) {
            s1 = peg$parseTrueToken();
            if (s1 === peg$FAILED) {
              s1 = peg$parseFalseToken();
            }
          }
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c91(s1);
          }
          s0 = s1;
        }
      }

      return s0;
    }

    function peg$parseString() {
      var s0, s1, s2, s3;

      peg$silentFails++;
      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 39) {
        s1 = peg$c93;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c94); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parseNotSQuote();
        if (s2 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 39) {
            s3 = peg$c93;
            peg$currPos++;
          } else {
            s3 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c94); }
          }
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c95(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 39) {
          s1 = peg$c93;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c94); }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$parseNotSQuote();
          if (s2 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c96(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          if (input.charCodeAt(peg$currPos) === 34) {
            s1 = peg$c97;
            peg$currPos++;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c98); }
          }
          if (s1 !== peg$FAILED) {
            s2 = peg$parseNotDQuote();
            if (s2 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 34) {
                s3 = peg$c97;
                peg$currPos++;
              } else {
                s3 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c98); }
              }
              if (s3 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c95(s2);
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            if (input.charCodeAt(peg$currPos) === 34) {
              s1 = peg$c97;
              peg$currPos++;
            } else {
              s1 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c98); }
            }
            if (s1 !== peg$FAILED) {
              s2 = peg$parseNotDQuote();
              if (s2 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c99(s2);
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          }
        }
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c92); }
      }

      return s0;
    }

    function peg$parseNullToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4).toLowerCase() === peg$c100) {
        s1 = input.substr(peg$currPos, 4);
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c101); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c102();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseTrueToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4).toLowerCase() === peg$c103) {
        s1 = input.substr(peg$currPos, 4);
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c104); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c105();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseFalseToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 5).toLowerCase() === peg$c106) {
        s1 = input.substr(peg$currPos, 5);
        peg$currPos += 5;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c107); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c108();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseSelectToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 6).toLowerCase() === peg$c109) {
        s1 = input.substr(peg$currPos, 6);
        peg$currPos += 6;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c110); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c111();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseUpdateToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 6).toLowerCase() === peg$c112) {
        s1 = input.substr(peg$currPos, 6);
        peg$currPos += 6;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c113); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c114();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseShowToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4).toLowerCase() === peg$c115) {
        s1 = input.substr(peg$currPos, 4);
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c116); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c117();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseSetToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 3).toLowerCase() === peg$c118) {
        s1 = input.substr(peg$currPos, 3);
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c119); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c120();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseFromToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4).toLowerCase() === peg$c121) {
        s1 = input.substr(peg$currPos, 4);
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c122); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseAsToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 2).toLowerCase() === peg$c123) {
        s1 = input.substr(peg$currPos, 2);
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c124); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseOnToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 2).toLowerCase() === peg$c125) {
        s1 = input.substr(peg$currPos, 2);
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c126); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseLeftToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4).toLowerCase() === peg$c127) {
        s1 = input.substr(peg$currPos, 4);
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c128); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseInnerToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 5).toLowerCase() === peg$c129) {
        s1 = input.substr(peg$currPos, 5);
        peg$currPos += 5;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c130); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseJoinToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4).toLowerCase() === peg$c131) {
        s1 = input.substr(peg$currPos, 4);
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c132); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseUnionToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 5).toLowerCase() === peg$c133) {
        s1 = input.substr(peg$currPos, 5);
        peg$currPos += 5;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c134); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseWhereToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 5).toLowerCase() === peg$c135) {
        s1 = input.substr(peg$currPos, 5);
        peg$currPos += 5;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c136); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseGroupToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 5).toLowerCase() === peg$c137) {
        s1 = input.substr(peg$currPos, 5);
        peg$currPos += 5;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c138); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseByToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 2).toLowerCase() === peg$c139) {
        s1 = input.substr(peg$currPos, 2);
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c140); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseOrderToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 5).toLowerCase() === peg$c141) {
        s1 = input.substr(peg$currPos, 5);
        peg$currPos += 5;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c142); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseHavingToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 6).toLowerCase() === peg$c143) {
        s1 = input.substr(peg$currPos, 6);
        peg$currPos += 6;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c144); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseLimitToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 5).toLowerCase() === peg$c145) {
        s1 = input.substr(peg$currPos, 5);
        peg$currPos += 5;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c146); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseAscToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 3).toLowerCase() === peg$c147) {
        s1 = input.substr(peg$currPos, 3);
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c148); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c149();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseDescToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4).toLowerCase() === peg$c150) {
        s1 = input.substr(peg$currPos, 4);
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c151); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c152();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseBetweenToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 7).toLowerCase() === peg$c153) {
        s1 = input.substr(peg$currPos, 7);
        peg$currPos += 7;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c154); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseInToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 2).toLowerCase() === peg$c155) {
        s1 = input.substr(peg$currPos, 2);
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c156); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseIsToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 2).toLowerCase() === peg$c157) {
        s1 = input.substr(peg$currPos, 2);
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c158); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseLikeToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4).toLowerCase() === peg$c159) {
        s1 = input.substr(peg$currPos, 4);
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c160); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseContainsToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 8).toLowerCase() === peg$c161) {
        s1 = input.substr(peg$currPos, 8);
        peg$currPos += 8;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c162); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseRegExpToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 6).toLowerCase() === peg$c163) {
        s1 = input.substr(peg$currPos, 6);
        peg$currPos += 6;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c164); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseEscapeToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 6).toLowerCase() === peg$c165) {
        s1 = input.substr(peg$currPos, 6);
        peg$currPos += 6;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c166); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseNotToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 3).toLowerCase() === peg$c167) {
        s1 = input.substr(peg$currPos, 3);
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c168); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseAndToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 3).toLowerCase() === peg$c169) {
        s1 = input.substr(peg$currPos, 3);
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c170); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseOrToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 2).toLowerCase() === peg$c171) {
        s1 = input.substr(peg$currPos, 2);
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c172); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseDistinctToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 8).toLowerCase() === peg$c173) {
        s1 = input.substr(peg$currPos, 8);
        peg$currPos += 8;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c174); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseStarToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 42) {
        s1 = peg$c175;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c176); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c5();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseCountToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 5).toLowerCase() === peg$c177) {
        s1 = input.substr(peg$currPos, 5);
        peg$currPos += 5;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c178); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c179();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseCountDistinctToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 14).toLowerCase() === peg$c180) {
        s1 = input.substr(peg$currPos, 14);
        peg$currPos += 14;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c181); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c182();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseSumToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 3).toLowerCase() === peg$c183) {
        s1 = input.substr(peg$currPos, 3);
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c184); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c185();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseAvgToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 3).toLowerCase() === peg$c186) {
        s1 = input.substr(peg$currPos, 3);
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c187); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c188();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseMinToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 3).toLowerCase() === peg$c189) {
        s1 = input.substr(peg$currPos, 3);
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c190); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c191();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseMaxToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 3).toLowerCase() === peg$c192) {
        s1 = input.substr(peg$currPos, 3);
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c193); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c194();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseQuantileToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 8).toLowerCase() === peg$c195) {
        s1 = input.substr(peg$currPos, 8);
        peg$currPos += 8;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c196); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c197();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseCustomToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 6).toLowerCase() === peg$c198) {
        s1 = input.substr(peg$currPos, 6);
        peg$currPos += 6;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c199); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c200();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseTimeBucketToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 11).toLowerCase() === peg$c201) {
        s1 = input.substr(peg$currPos, 11);
        peg$currPos += 11;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c202); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseNumberBucketToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 13).toLowerCase() === peg$c203) {
        s1 = input.substr(peg$currPos, 13);
        peg$currPos += 13;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c204); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseTimePartToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 9).toLowerCase() === peg$c205) {
        s1 = input.substr(peg$currPos, 9);
        peg$currPos += 9;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c206); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseSubstrToken() {
      var s0, s1, s2, s3, s4;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 6).toLowerCase() === peg$c207) {
        s1 = input.substr(peg$currPos, 6);
        peg$currPos += 6;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c208); }
      }
      if (s1 !== peg$FAILED) {
        if (input.substr(peg$currPos, 3).toLowerCase() === peg$c209) {
          s2 = input.substr(peg$currPos, 3);
          peg$currPos += 3;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c210); }
        }
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          s3 = peg$currPos;
          peg$silentFails++;
          s4 = peg$parseIdentifierPart();
          peg$silentFails--;
          if (s4 === peg$FAILED) {
            s3 = void 0;
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
          if (s3 !== peg$FAILED) {
            s1 = [s1, s2, s3];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseExtractToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 7).toLowerCase() === peg$c211) {
        s1 = input.substr(peg$currPos, 7);
        peg$currPos += 7;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c212); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseConcatToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 6).toLowerCase() === peg$c213) {
        s1 = input.substr(peg$currPos, 6);
        peg$currPos += 6;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c214); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseLookupToken() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 6).toLowerCase() === peg$c215) {
        s1 = input.substr(peg$currPos, 6);
        peg$currPos += 6;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c216); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parseIdentifierPart();
        peg$silentFails--;
        if (s3 === peg$FAILED) {
          s2 = void 0;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseIdentifierPart() {
      var s0;

      if (peg$c217.test(input.charAt(peg$currPos))) {
        s0 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c218); }
      }

      return s0;
    }

    function peg$parseNumber() {
      var s0, s1, s2, s3, s4, s5;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = peg$currPos;
      s2 = peg$currPos;
      s3 = peg$parseInt();
      if (s3 !== peg$FAILED) {
        s4 = peg$parseFraction();
        if (s4 === peg$FAILED) {
          s4 = null;
        }
        if (s4 !== peg$FAILED) {
          s5 = peg$parseExp();
          if (s5 === peg$FAILED) {
            s5 = null;
          }
          if (s5 !== peg$FAILED) {
            s3 = [s3, s4, s5];
            s2 = s3;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
      } else {
        peg$currPos = s2;
        s2 = peg$FAILED;
      }
      if (s2 !== peg$FAILED) {
        s1 = input.substring(s1, peg$currPos);
      } else {
        s1 = s2;
      }
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c220(s1);
      }
      s0 = s1;
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c219); }
      }

      return s0;
    }

    function peg$parseInt() {
      var s0, s1, s2, s3, s4;

      s0 = peg$currPos;
      s1 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 45) {
        s2 = peg$c221;
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c222); }
      }
      if (s2 === peg$FAILED) {
        s2 = null;
      }
      if (s2 !== peg$FAILED) {
        if (peg$c223.test(input.charAt(peg$currPos))) {
          s3 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c224); }
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parseDigits();
          if (s4 !== peg$FAILED) {
            s2 = [s2, s3, s4];
            s1 = s2;
          } else {
            peg$currPos = s1;
            s1 = peg$FAILED;
          }
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 45) {
          s2 = peg$c221;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c222); }
        }
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          s3 = peg$parseDigit();
          if (s3 !== peg$FAILED) {
            s2 = [s2, s3];
            s1 = s2;
          } else {
            peg$currPos = s1;
            s1 = peg$FAILED;
          }
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
        if (s1 !== peg$FAILED) {
          s0 = input.substring(s0, peg$currPos);
        } else {
          s0 = s1;
        }
      }

      return s0;
    }

    function peg$parseFraction() {
      var s0, s1, s2, s3;

      s0 = peg$currPos;
      s1 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 46) {
        s2 = peg$c80;
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c81); }
      }
      if (s2 !== peg$FAILED) {
        s3 = peg$parseDigits();
        if (s3 !== peg$FAILED) {
          s2 = [s2, s3];
          s1 = s2;
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }

      return s0;
    }

    function peg$parseExp() {
      var s0, s1, s2, s3, s4;

      s0 = peg$currPos;
      s1 = peg$currPos;
      if (input.substr(peg$currPos, 1).toLowerCase() === peg$c225) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c226); }
      }
      if (s2 !== peg$FAILED) {
        if (peg$c57.test(input.charAt(peg$currPos))) {
          s3 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c58); }
        }
        if (s3 === peg$FAILED) {
          s3 = null;
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parseDigits();
          if (s4 !== peg$FAILED) {
            s2 = [s2, s3, s4];
            s1 = s2;
          } else {
            peg$currPos = s1;
            s1 = peg$FAILED;
          }
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }

      return s0;
    }

    function peg$parseDigits() {
      var s0, s1, s2;

      s0 = peg$currPos;
      s1 = [];
      s2 = peg$parseDigit();
      if (s2 !== peg$FAILED) {
        while (s2 !== peg$FAILED) {
          s1.push(s2);
          s2 = peg$parseDigit();
        }
      } else {
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }

      return s0;
    }

    function peg$parseDigit() {
      var s0;

      if (peg$c227.test(input.charAt(peg$currPos))) {
        s0 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c228); }
      }

      return s0;
    }

    function peg$parseOpenParen() {
      var s0, s1;

      peg$silentFails++;
      if (input.charCodeAt(peg$currPos) === 40) {
        s0 = peg$c51;
        peg$currPos++;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c52); }
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c229); }
      }

      return s0;
    }

    function peg$parseCloseParen() {
      var s0, s1, s2;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = peg$parse_();
      if (s1 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 41) {
          s2 = peg$c53;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c54); }
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c230); }
      }

      return s0;
    }

    function peg$parseName() {
      var s0, s1, s2, s3, s4;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = peg$currPos;
      if (peg$c232.test(input.charAt(peg$currPos))) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c233); }
      }
      if (s2 !== peg$FAILED) {
        s3 = [];
        if (peg$c234.test(input.charAt(peg$currPos))) {
          s4 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s4 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c235); }
        }
        while (s4 !== peg$FAILED) {
          s3.push(s4);
          if (peg$c234.test(input.charAt(peg$currPos))) {
            s4 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s4 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c235); }
          }
        }
        if (s3 !== peg$FAILED) {
          s2 = [s2, s3];
          s1 = s2;
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c231); }
      }

      return s0;
    }

    function peg$parseNotSQuote() {
      var s0, s1, s2;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = [];
      if (peg$c237.test(input.charAt(peg$currPos))) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c238); }
      }
      while (s2 !== peg$FAILED) {
        s1.push(s2);
        if (peg$c237.test(input.charAt(peg$currPos))) {
          s2 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c238); }
        }
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c236); }
      }

      return s0;
    }

    function peg$parseNotDQuote() {
      var s0, s1, s2;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = [];
      if (peg$c240.test(input.charAt(peg$currPos))) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c241); }
      }
      while (s2 !== peg$FAILED) {
        s1.push(s2);
        if (peg$c240.test(input.charAt(peg$currPos))) {
          s2 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c241); }
        }
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c239); }
      }

      return s0;
    }

    function peg$parse_() {
      var s0, s1, s2;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = [];
      if (peg$c243.test(input.charAt(peg$currPos))) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c244); }
      }
      if (s2 === peg$FAILED) {
        s2 = peg$parseSingleLineComment();
      }
      while (s2 !== peg$FAILED) {
        s1.push(s2);
        if (peg$c243.test(input.charAt(peg$currPos))) {
          s2 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c244); }
        }
        if (s2 === peg$FAILED) {
          s2 = peg$parseSingleLineComment();
        }
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c242); }
      }

      return s0;
    }

    function peg$parse__() {
      var s0, s1, s2;

      peg$silentFails++;
      s0 = peg$currPos;
      s1 = [];
      if (peg$c243.test(input.charAt(peg$currPos))) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c244); }
      }
      if (s2 === peg$FAILED) {
        s2 = peg$parseSingleLineComment();
      }
      if (s2 !== peg$FAILED) {
        while (s2 !== peg$FAILED) {
          s1.push(s2);
          if (peg$c243.test(input.charAt(peg$currPos))) {
            s2 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s2 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c244); }
          }
          if (s2 === peg$FAILED) {
            s2 = peg$parseSingleLineComment();
          }
        }
      } else {
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s0 = input.substring(s0, peg$currPos);
      } else {
        s0 = s1;
      }
      peg$silentFails--;
      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c245); }
      }

      return s0;
    }

    function peg$parseSingleLineComment() {
      var s0, s1, s2, s3, s4, s5;

      s0 = peg$currPos;
      if (input.substr(peg$currPos, 2) === peg$c246) {
        s1 = peg$c246;
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c247); }
      }
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$currPos;
        s4 = peg$currPos;
        peg$silentFails++;
        s5 = peg$parseLineTerminator();
        peg$silentFails--;
        if (s5 === peg$FAILED) {
          s4 = void 0;
        } else {
          peg$currPos = s4;
          s4 = peg$FAILED;
        }
        if (s4 !== peg$FAILED) {
          if (input.length > peg$currPos) {
            s5 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c19); }
          }
          if (s5 !== peg$FAILED) {
            s4 = [s4, s5];
            s3 = s4;
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$currPos;
          s4 = peg$currPos;
          peg$silentFails++;
          s5 = peg$parseLineTerminator();
          peg$silentFails--;
          if (s5 === peg$FAILED) {
            s4 = void 0;
          } else {
            peg$currPos = s4;
            s4 = peg$FAILED;
          }
          if (s4 !== peg$FAILED) {
            if (input.length > peg$currPos) {
              s5 = input.charAt(peg$currPos);
              peg$currPos++;
            } else {
              s5 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c19); }
            }
            if (s5 !== peg$FAILED) {
              s4 = [s4, s5];
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseLineTerminator() {
      var s0;

      if (peg$c248.test(input.charAt(peg$currPos))) {
        s0 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c249); }
      }

      return s0;
    }

    // starts with function(plywood)
    var ply = plywood.ply;
    var $ = plywood.$;
    var r = plywood.r;
    var Expression = plywood.Expression;
    var FilterAction = plywood.FilterAction;
    var ApplyAction = plywood.ApplyAction;
    var SortAction = plywood.SortAction;
    var LimitAction = plywood.LimitAction;
    var MatchAction = plywood.MatchAction;
    var Set = plywood.Set;

    var dataRef = $('data');
    var dateRegExp = /^\d\d\d\d-\d\d-\d\d(?:T(?:\d\d)?(?::\d\d)?(?::\d\d)?(?:.\d\d\d)?)?Z?$/;

    // See here: https://www.drupal.org/node/141051
    var reservedWords = {
      ALL: 1, AND: 1,  AS: 1, ASC: 1, AVG: 1,
      BETWEEN: 1, BY: 1,
      CONTAINS: 1, CREATE: 1,
      DELETE: 1, DESC: 1, DISTINCT: 1, DROP: 1,
      EXISTS: 1, EXPLAIN: 1, ESCAPE: 1, EXTRACT: 1,
      FALSE: 1, FROM: 1,
      GROUP: 1,
      HAVING: 1,
      IN: 1, INNER: 1,  INSERT: 1, INTO: 1, IS: 1,
      JOIN: 1,
      LEFT: 1, LIKE: 1, LIMIT: 1, LOOKUP: 1,
      MAX: 1, MIN: 1,
      NOT: 1, NULL: 1, NUMBER_BUCKET: 1,
      ON: 1, OR: 1, ORDER: 1,
      QUANTILE: 1,
      REPLACE: 1, REGEXP: 1,
      SELECT: 1, SET: 1, SHOW: 1, SUM: 1, SUBSTR: 1, SUBSTRING: 1,
      TABLE: 1, TIME_BUCKET: 1, TRUE: 1,
      UNION: 1, UPDATE: 1,
      VALUES: 1,
      WHERE: 1
    }

    var aggregates = {
      count: 1,
      sum: 1, min: 1, max: 1,
      average: 1,
      countDistinct: 1,
      quantile: 1,
      custom: 1,
      split: 1
    }

    var objectHasOwnProperty = Object.prototype.hasOwnProperty;
    function reserved(str) {
      return objectHasOwnProperty.call(reservedWords, str.toUpperCase());
    }

    function extractGroupByColumn(columns, groupBy, index) {
      var label = null;
      var otherColumns = [];
      for (var i = 0; i < columns.length; i++) {
        var column = columns[i];
        if (groupBy.equals(column.expression)) {
          if (label) error('already have a label');
          label = column.name;
        } else {
          otherColumns.push(column);
        }
      }
      if (!label) label = 'split' + index;
      return {
        label: label,
        otherColumns: otherColumns
      };
    }

    function constructQuery(columns, from, where, groupBys, having, orderBy, limit) {
      if (!columns) error('Can not have empty column list');
      from = from ? $(from) : dataRef;

      if (where) {
        from = from.filter(where);
      }

      if (Array.isArray(columns)) { // Not *
        if (!groupBys) {
          // Support for not having a group by clause if there are aggregates in the columns
          // A having an aggregate columns is the same as having "GROUP BY ''"

          var hasAggregate = columns.some(function(column) {
            var columnExpression = column.expression;
            return columnExpression.isOp('chain') &&
              columnExpression.actions.length &&
              aggregates[columnExpression.actions[0].action];
          })
          if (hasAggregate) groupBys = [Expression.EMPTY_STRING];

        } else {
          groupBys = groupBys.map(function(groupBy) {
            if (groupBy.isOp('literal') && groupBy.type === 'NUMBER') {
              // Support for not having a group by clause refer to a select column by index

              var groupByColumn = columns[groupBy.value - 1];
              if (!groupByColumn) error("Unknown column '" + groupBy.value + "' in group by statement");

              return groupByColumn.expression;
            } else {
              return groupBy;
            }
          });
        }
      }

      var query = null;
      if (!groupBys) {
        query = from;
      } else {
        if (columns === '*') error('can not SELECT * with a GROUP BY');

        if (groupBys.length === 1 && groupBys[0].isOp('literal')) {
          query = ply().apply('data', from);
        } else {
          var splits = {};
          for (var i = 0; i < groupBys.length; i++) {
            var groupBy = groupBys[i];
            var extract = extractGroupByColumn(columns, groupBy, i);
            columns = extract.otherColumns;
            splits[extract.label] = groupBy;
          }
          query = from.split(splits, 'data');
        }

        if (Array.isArray(columns)) {
          for (var i = 0; i < columns.length; i++) {
            query = query.performAction(columns[i]);
          }
        }
      }

      if (having) {
        query = query.performAction(having);
      }
      if (orderBy) {
        query = query.performAction(orderBy);
      }
      if (limit) {
        query = query.performAction(limit);
      }

      return query;
    }

    function makeListMap3(head, tail) {
      return [head].concat(tail.map(function(t) { return t[3] }));
    }

    function naryExpressionFactory(op, head, tail) {
      if (!tail.length) return head;
      return head[op].apply(head, tail.map(function(t) { return t[3]; }));
    }

    function naryExpressionWithAltFactory(op, head, tail, altToken, altOp) {
      if (!tail.length) return head;
      for (var i = 0; i < tail.length; i++) {
        var t = tail[i];
        head = head[t[1] === altToken ? altOp : op].call(head, t[3]);
      }
      return head;
    }



    peg$result = peg$startRuleFunction();

    if (peg$result !== peg$FAILED && peg$currPos === input.length) {
      return peg$result;
    } else {
      if (peg$result !== peg$FAILED && peg$currPos < input.length) {
        peg$fail({ type: "end", description: "end of input" });
      }

      throw peg$buildException(
        null,
        peg$maxFailExpected,
        peg$maxFailPos < input.length ? input.charAt(peg$maxFailPos) : null,
        peg$maxFailPos < input.length
          ? peg$computeLocation(peg$maxFailPos, peg$maxFailPos + 1)
          : peg$computeLocation(peg$maxFailPos, peg$maxFailPos)
      );
    }
  }

  return {
    SyntaxError: peg$SyntaxError,
    parse:       peg$parse
  };
};

},{}]},{},[1])(1)
});