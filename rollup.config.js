export default {
  entry: 'build/index.js',
  format: 'cjs',
  dest: 'build/plywood.js',
  external: [
    'immutable-class'
  ]
};
