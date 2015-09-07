module Plywood {
  function margin1d(left: number, width: number, right: number, parentWidth: number): number[] {
    if (left != null) {
      if (width != null) {
        if (right != null) throw new Error("over-constrained");
        return [left, width];
      } else {
        return [left, parentWidth - left - (right || 0)];
      }
    } else {
      if (width != null) {
        if (right != null) {
          return [parentWidth - width - right, width];
        } else {
          return [(parentWidth + width) / 2, width];
        }
      } else {
        return [0, parentWidth - right];
      }
    }
  }

  export interface ShapeJS {
    shape?: string;
    x: number;
    y: number;
    width?: number;
    height?: number;
  }

  var check: Class<ShapeJS, ShapeJS>;
  export class Shape implements Instance<ShapeJS, ShapeJS> {
    static type = 'SHAPE';

    static isShape(candidate: any): boolean {
      return isInstanceOf(candidate, Shape);
    }

    static rectangle(width: number, height: number): RectangleShape {
      return new RectangleShape({
        x: 0,
        y: 0,
        width: width,
        height: height
      });
    }

    static classMap: any = {};

    static fromJS(parameters: ShapeJS): Shape {
      if (typeof parameters !== "object") {
        throw new Error("unrecognizable shape");
      }
      if (!hasOwnProperty(parameters, "shape")) {
        throw new Error("shape must be defined");
      }
      if (typeof parameters.shape !== "string") {
        throw new Error("shape must be a string");
      }
      var ClassFn = Shape.classMap[parameters.shape];
      if (!ClassFn) {
        throw new Error("unsupported shape '" + parameters.shape + "'");
      }
      return ClassFn.fromJS(parameters);
    }

    public shape: string;
    public x: number;
    public y: number;

    constructor(parameters: ShapeJS, dummy: Dummy = null) {
      if (dummy !== dummyObject) {
        throw new TypeError("can not call `new Shape` directly use Shape.fromJS instead");
      }
      this.x = parameters.x;
      this.y = parameters.y;
    }

    public _ensureShape(shape: string): void {
      if (!this.shape) {
        this.shape = shape;
        return;
      }
      if (this.shape !== shape) {
        throw new TypeError("incorrect shape '" + this.shape + "' (needs to be: '" + shape + "')");
      }
    }

    public valueOf(): ShapeJS {
      return {
        shape: this.shape,
        x: this.x,
        y: this.y
      };
    }

    public toJS(): ShapeJS {
      return this.valueOf();
    }

    public toJSON(): ShapeJS {
      return this.valueOf();
    }

    public toString(): string {
      return "Shape(" + this.x + ',' + this.y + ")";
    }

    public equals(other: Shape): boolean {
      return Shape.isShape(other) &&
        this.shape === other.shape &&
        this.x === other.x &&
        this.y === other.y;
    }
  }
  check = Shape;


  export interface MarginParameters {
    left: any;
    width: any;
    right: any;
    top: any;
    height: any;
    bottom: any;
  }

  export class RectangleShape extends Shape {
    static type = 'SHAPE';

    static fromJS(parameters: ShapeJS): RectangleShape {
      return new RectangleShape(parameters);
    }

    public width: any;
    public height: any;

    constructor(parameters: ShapeJS) {
      super(parameters, dummyObject);
      this.width = parameters.width;
      this.height = parameters.height;
      this._ensureShape('rectangle');
    }

    public valueOf(): ShapeJS {
      var value = super.valueOf();
      value.width = this.width;
      value.height = this.height;
      return value;
    }

    public toString(): string {
      return "RectangleShape(" + this.width + ',' + this.height + ")";
    }

    public equals(other: RectangleShape): boolean {
      return super.equals(other) &&
        this.width === other.width &&
        this.height === other.height;
    }

    public margin(parameters: MarginParameters) {
      var xw = margin1d(parameters.left, parameters.width, parameters.right, this.width);
      var yh = margin1d(parameters.top, parameters.height, parameters.bottom, this.height);
      return new RectangleShape({
        x: xw[0],
        y: yh[0],
        width: xw[1],
        height: yh[1]
      });
    }
  }

  Shape.classMap['rectangle'] = RectangleShape;
}
