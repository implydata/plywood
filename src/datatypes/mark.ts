module Plywood {
  export function fillMethods(mark: Lookup<any>, bindSpec: BindSpec): void {
    var attr = mark['attr'];
    if (attr) {
      bindSpec.attr = Object.keys(attr);
    }

    var style = mark['style'];
    if (style) {
      bindSpec.style = Object.keys(style);
    }

    if (hasOwnProperty(mark, 'text')) {
      bindSpec.text = true;
    }
  }

  export interface MarkJS {
    selector: string;
    prop: Lookup<any>;
  }

  var check: Class<MarkJS, MarkJS>;
  export class Mark implements Instance<MarkJS, MarkJS> {
    static type = 'MARK';

    static isMark(candidate: any): boolean {
      return isInstanceOf(candidate, Mark);
    }

    static fromJS(parameters: MarkJS): Mark {
      return new Mark(parameters);
    }

    public selector: string;
    public prop: Lookup<any>;
    public children: Lookup<Mark[]>;

    constructor(parameters: MarkJS) {
      this.selector = parameters.selector;
      this.prop = parameters.prop;
    }

    public valueOf(): MarkJS {
      return {
        selector: this.selector,
        prop: this.prop
      };
    }

    public toJS(): MarkJS {
      return this.valueOf();
    }

    public toJSON(): MarkJS {
      return this.valueOf();
    }

    public toString(): string {
      return "Mark(" + this.selector + ")";
    }

    public equals(other: Mark): boolean {
      return Mark.isMark(other) &&
        this.selector === other.selector;
    }

    /*
     this.selector = selector;
     var classes = selector.split('.');
     var selector = classes.shift();
     if (selector === '') throw new Error('Empty tag');
     this.selector = selector;
     this.classes = classes.join(' ');
     */

    public attach(selector: string, prop: Lookup<any>): Mark {
      return new Mark({
        selector,
        prop
      });
    }
  }
  check = Mark;
}
