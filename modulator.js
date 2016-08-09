//  ./compile-tsc | grep "Cannot find name 'PlywoodValue'"
//  ./compile-tsc | grep "Cannot find name 'PlywoodValue'" | cut -d'(' -f 1 | sort | uniq | xargs -n 1 node modulator.js PlywoodValue src/datatypes/index.ts

var path = require('path');
var fs = require('fs');

// node modulator.js hasOwnProperty src/helper/utils.ts src/datatypes/set.ts


var prop = process.argv[2];
var source = process.argv[3];
var target = process.argv[4];
if (!target) throw new Error('bad param');

var data = fs.readFileSync(target, 'utf-8');

var absoluteTarget = path.resolve(target);
var absoluteSource = path.resolve(source);

console.log('absoluteTarget', absoluteTarget);
console.log('absoluteSource', absoluteSource);

var sourceRelative = path.relative(path.dirname(absoluteTarget), absoluteSource).replace(/\.ts$/, '');
if (sourceRelative[0] !== '.') sourceRelative = `./${sourceRelative}`;
console.log('sourceRelative', sourceRelative);

var insertFull;
var insertPoint;
var idx;

if ((idx = data.indexOf(` } from '${sourceRelative}'`)) !== -1) {
  insertFull = false;
  insertPoint = idx;
} else if ((idx = data.lastIndexOf('import {')) !== -1) {
  insertFull = true;
  insertPoint = data.indexOf('\n', idx) + 1;
} else if ((idx = data.indexOf('\n */\n')) !== -1) {
  insertFull = true;
  insertPoint = data.indexOf('\n', idx + 1) + 2;
} else {
  throw new Error('no insert point');
}

function strSplice(str, idx, newSubStr) {
  return str.slice(0, idx) + newSubStr + str.slice(idx);
}

// data = strSplice(data, insertPoint, '<>');

if (insertFull) {
  data = strSplice(data, insertPoint, `import { ${prop} } from '${sourceRelative}';\n`);
} else {
  data = strSplice(data, insertPoint, `, ${prop}`);
}

//console.log(data)
fs.writeFileSync(target, data);
