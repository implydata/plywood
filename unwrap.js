var fs = require('fs');

var file = process.argv[2];

var data = fs.readFileSync(file, 'utf-8');

var search = 'module Plywood {';
if (data.indexOf(search) === -1) process.exit();

data = data.replace(search, '');
data = data.replace(/\n\}/, '');
data = data.replace(/\n  /g, '\n');

fs.writeFileSync(file, data);
