let path = require('path');
let plywoodPath = process.env.PLYWOOD_PATH || 'build/plywood';
module.exports = require(path.join('..', plywoodPath));
