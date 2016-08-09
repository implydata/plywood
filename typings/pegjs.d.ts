declare interface PEGParserOptions {
  cache?: boolean;
  allowedStartRules?: string;
  output?: string;
  optimize?: string;
  plugins?: any;
  [key: string]: any;
}

declare interface PEGParser {
  parse: (str: string, options?: PEGParserOptions) => any;
}

declare interface PEGParserFactory {
  (plywood: any, chronoshift: any): PEGParser;
}
