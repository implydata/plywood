Expression.expressionParser = require("./expressionParser")(exports, Chronoshift);
Expression.plyqlParser = require("./plyqlParser")(exports, Chronoshift);

var helper = {};
function addHasMoved(name, fn) {
  helper[name] = function() {
    console.warn(name + ' has moved, please update your code');
    return fn.apply(this, arguments);
  };
}

addHasMoved('parseJSON', Dataset.parseJSON);
addHasMoved('find', find);
addHasMoved('findIndex', findIndex);
addHasMoved('findByName', findByName);
addHasMoved('findIndexByName', findIndexByName);
addHasMoved('overrideByName', overrideByName);
addHasMoved('overridesByName', overridesByName);
addHasMoved('shallowCopy', shallowCopy);
addHasMoved('deduplicateSort', deduplicateSort);
addHasMoved('mapLookup', mapLookup);
addHasMoved('emptyLookup', emptyLookup);
addHasMoved('nonEmptyLookup', nonEmptyLookup);
addHasMoved('verboseRequesterFactory', verboseRequesterFactory);
addHasMoved('retryRequesterFactory', retryRequesterFactory);
addHasMoved('concurrentLimitRequesterFactory', concurrentLimitRequesterFactory);
addHasMoved('promiseWhile', promiseWhile);

exports.helper = helper;
