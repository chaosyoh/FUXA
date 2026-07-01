declare function require(moduleName: string): any;
console.log('prod');
export const environment = {
  version: require('../../package.json').version,
  production: true,
  apiEndpoint: null,
  apiPort: require('../../package.json').apiPort,
  serverEnabled: true,
  type: null
};
