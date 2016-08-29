'use strict';

var Q = require('q');

var immutableClass = require('immutable-class');
var isInstanceOf = immutableClass.isInstanceOf;
var generalEqual = immutableClass.generalEqual;
var isImmutableClass = immutableClass.isImmutableClass;
var immutableEqual = immutableClass.immutableEqual;
var immutableArraysEqual = immutableClass.immutableArraysEqual;
var immutableLookupsEqual = immutableClass.immutableLookupsEqual;
var SimpleArray = immutableClass.SimpleArray;
var NamedArray = immutableClass.NamedArray;

var Chronoshift = require('chronoshift');
var Timezone = Chronoshift.Timezone;
var Duration = Chronoshift.Duration;
var WallTime = Chronoshift.WallTime;
var isDate = Chronoshift.isDate;
var parseISODate = Chronoshift.parseISODate;

var dummyObject = {};

