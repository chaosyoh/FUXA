declare function require(moduleName: string): any;
export const environment = {
  version: require('../../package.json').version,
  production: true,
  apiEndpoint: 'http://211.97.63.51:1881',
  apiPort: null,
  serverEnabled: true,
  type: null
};
