Expression.expressionParser = require("./expressionParser")(exports, Chronoshift);
Expression.plyqlParser = require("./plyqlParser")(exports, Chronoshift);

function addHasMoved(obj, name, fn) {
  obj[name] = function() {
    console.warn(name + ' has moved, please update your code');
    return fn.apply(this, arguments);
  };
}

var helper = {};
addHasMoved(helper, 'parseJSON', Dataset.parseJSON);
addHasMoved(helper, 'find', SimpleArray.find);
addHasMoved(helper, 'findIndex', SimpleArray.findIndex);
addHasMoved(helper, 'findByName', NamedArray.findByName);
addHasMoved(helper, 'findIndexByName', NamedArray.findIndexByName);
addHasMoved(helper, 'overrideByName', NamedArray.overrideByName);
addHasMoved(helper, 'overridesByName', NamedArray.overridesByName);
addHasMoved(helper, 'shallowCopy', shallowCopy);
addHasMoved(helper, 'deduplicateSort', deduplicateSort);
addHasMoved(helper, 'mapLookup', mapLookup);
addHasMoved(helper, 'emptyLookup', emptyLookup);
addHasMoved(helper, 'nonEmptyLookup', nonEmptyLookup);
addHasMoved(helper, 'verboseRequesterFactory', verboseRequesterFactory);
addHasMoved(helper, 'retryRequesterFactory', retryRequesterFactory);
addHasMoved(helper, 'concurrentLimitRequesterFactory', concurrentLimitRequesterFactory);
addHasMoved(helper, 'promiseWhile', promiseWhile);

exports.helper = helper;
addHasMoved(exports, 'find', SimpleArray.find);
addHasMoved(exports, 'findIndex', SimpleArray.findIndex);
addHasMoved(exports, 'findByName', NamedArray.findByName);
addHasMoved(exports, 'findIndexByName', NamedArray.findIndexByName);
addHasMoved(exports, 'overrideByName', NamedArray.overrideByName);
addHasMoved(exports, 'overridesByName', NamedArray.overridesByName);

for (var k in exports) {
  if (/^[A-Z]\w+Expression$/.test(k)) {
    exports[k.replace('Expression', 'Action')] = exports[k];
  }
}
