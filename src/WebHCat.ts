import requestPromiseNative from 'request-promise-native';
import { StatusCodeError } from 'request-promise-native/errors';

interface WebHCatOptions {
  username?: string;
  port?: number;
  hosts?: string[];
}

export class WebHCat {
  private activeHostIndex = -1;
  private username = 'APP';
  private port = 50111;
  private hosts = ['localhost'];
  private baseURL = '';

  constructor(options: WebHCatOptions) {
    if (options.username !== undefined) { this.username = options.username; }
    if (options.port !== undefined) { this.port = options.port; }
    if (options.hosts !== undefined) { this.hosts = options.hosts; }
    this.changeHost();
  }

  private changeHost() {
    this.activeHostIndex += 1;
    if (this.activeHostIndex === this.hosts.length) {
      this.activeHostIndex = 0;
    }
    const activeHost = this.hosts[this.activeHostIndex];
    this.baseURL = `http://${activeHost}:${this.port}/templeton/v1`;
  }

  private get(path: string, queryParams?: {[key: string]: string}) {
    return this.request(path, 'GET', queryParams);
  }

  private request(
    path: string,
    method: string,
    queryParams: {[key: string]: string} = {},
    body?: object,
  ): Promise<any> {
    queryParams['user.name'] = this.username;
    return requestPromiseNative({
      method,
      uri: path,
      baseUrl: this.baseURL,
      qs: queryParams,
      json: true,
      form: body,
    })
      .catch((error) => {
        // if the server is busy, switch hosts (if possible) and try again
        if (error instanceof StatusCodeError && error.statusCode === 503) {
          this.changeHost();
          return this.request(path, method);
        }
        throw error;
      });
  }

  listResponseTypes(): Promise<string[]> {
    return this.get('/')
      .then(res => res.responseTypes);
  }
}
