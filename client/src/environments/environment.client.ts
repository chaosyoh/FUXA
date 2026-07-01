declare function require(moduleName: string): any;
console.log('client');
export const environment = {
  version: require('../../package.json').version,
  production: true,
  apiEndpoint: 'http://211.97.63.51:1881',
  apiPort: 1881,
  serverEnabled: true,
  type: null
};
