export default {
  entry: 'build/index.js',
  format: 'cjs',
  dest: 'build/plywood.js',
  treeshake: false,
  external: [
    'chronoshift',
    'immutable-class',
    'q'
  ]
};
