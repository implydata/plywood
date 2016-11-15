var path = require('path');
var plywoodPath = process.env.PLYWOOD_PATH || 'build/plywood';
module.exports = require(path.join('..', plywoodPath));
