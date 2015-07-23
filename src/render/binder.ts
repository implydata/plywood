module Plywood {

  export interface BindSpec {
    selectionInput: string;
    selector: string;
    selectionName: string;
    data?: string;
    key?: string;

    attr?: string[];
    style?: string[];
    text?: boolean;
  }

  interface TagAndClasses {
    tag: string;
    classes: string[];
  }

  function parseSelector(selector: string): TagAndClasses {
    var classes = selector.split('.');
    var tag = classes.shift();
    if (tag === '') throw new Error('Empty tag');
    return {
      tag,
      classes
    };
  }

  function bindOne(selection: d3.Selection<Datum>, selector: string, datum: Datum): d3.Selection<Datum> {
    var empties = selection.filter(() => d3.select(this).select(selector).empty());
    if (!empties.empty()) {
      var tagAndClasses = parseSelector(selector);
      var enterSelection = empties.append(tagAndClasses.tag);
      var classes = tagAndClasses.classes;
      if (classes.length) enterSelection.attr('class', classes.join(' '));
      //if (arg.onEnter) {
      //  if (datumFn) enterSelection.datum(datumFn);
      //  arg.onEnter.call(enterSelection, enterSelection);
      //}
    }

    var updateSelection = selection.select(selector);
    if (datum) updateSelection.datum(datum);

    return updateSelection;
  }


  function bindMany(selection: d3.Selection<Datum>, selector: string, data: string, key: string): d3.Selection<Datum> {
    var tagAndClasses = parseSelector(selector);
    var classes = tagAndClasses.classes;

    var update = selection.selectAll(selector).data(((d: Datum) => d[data].data), ((d: Datum) => d[key]));

    var enterSelection = update.enter().append(tagAndClasses.tag);
    if (classes.length) enterSelection.attr('class', classes.join(' '));

    //var onEnter = arg.onEnter;
    //if (onEnter) onEnter.call(enterSelection, enterSelection);

    var exitSelection = update.exit();
    //var onExit = arg.onExit;
    //if (onExit) {
    //  onExit.call(exitSelection, exitSelection);
    //} else {
    //  exitSelection.remove();
    //}

    exitSelection.remove();

    return update;
  }

  export function binder(baseSelection: d3.Selection<Datum>, dataset: NativeDataset, bindSpecs: BindSpec[]): void {
    var selections: Lookup<d3.Selection<Datum>> = {
      __base__: baseSelection
    };

    for (var bindSpec of bindSpecs) {
      var selection = selections[bindSpec.selectionInput];
      if (bindSpec.data) {
        selection = bindMany(selection, bindSpec.selector, bindSpec.data, bindSpec.key);
      } else {
        var datum = bindSpec.selectionInput === '__base__' ? dataset.data[0] : null;
        selection = bindOne(selection, bindSpec.selector, datum);
      }
      selections[bindSpec.selectionName] = selection;
    }
  }
}
