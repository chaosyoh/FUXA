declare function require(moduleName: string): any;
export const environment = {
  version: require('../../package.json').version,
  production: true,
  /**
   * 手机app需要设api请求地址
   * 荣昌: http://211.97.63.51:1881
   */
  //apiEndpoint: 'http://8.138.120.145:1882',
  //apiEndpoint:'http://10.180.2.100:1881',
  apiEndpoint:'http://127.0.0.1:1881',
  //apiEndpoint:null,
  apiPort: null,
  serverEnabled: true,
  type: null
};
