var fs = require('fs');

var packageFilename = './package.json';
var defFilename = './build/plywood.d.ts';
var jsFilename = './build/plywood.js';
try {
  var packageData = JSON.parse(fs.readFileSync(packageFilename, 'utf8'));
  var defData = fs.readFileSync(defFilename, 'utf8');
  var jsData = fs.readFileSync(jsFilename, 'utf8');
} catch (e) {
  process.exit(0);
}

defData += '\n';

// Delete:
// initial crud
defData = defData.replace(/interface DELETE_START[\s\S]+interface DELETE_END[^}]+}\n/, '');

// Ensure it was deleted
if (defData.indexOf('declare var module') !== -1) {
  throw new Error("failed to delete require declaration");
}

var i = 0;
defData = defData.replace(/}\ndeclare module Plywood \{\n/g, function(str) {
  i++;
  return i === 1 ? str : '';
});

// remove protected
defData = defData.replace(/ +protected [^\n]+\n/g, '');

// Make explicit node module
var defDataCommonJS = defData.replace(/declare module Plywood/, 'declare module "plywood"');


// Version
var plywoodVersion = packageData.version;

// Ensure version is correct
if (!/^\d+\.\d+\.\d+$/.test(plywoodVersion)) {
  throw new Error("version is not right");
}

// Fill in version
jsData = jsData.replace('###_VERSION_###', plywoodVersion);

// Ensure it was filled in
if (jsData.indexOf(plywoodVersion) === -1) {
  throw new Error("failed to fill in version");
}

// Delete the _delete_me_
jsData = jsData.replace(/_delete_me_/g, '');

// Ensure it was deleted
if (jsData.indexOf('_delete_me_') !== -1) {
  throw new Error("failed to delete _delete_me_");
}

fs.writeFileSync(defFilename, defDataCommonJS, 'utf8');
fs.writeFileSync(jsFilename, jsData, 'utf8');
