import requestPromiseNative from 'request-promise-native';
import { StatusCodeError } from 'request-promise-native/errors';

interface StringMap {
  [s: string]: string;
}

interface WebHCatOptions {
  username?: string;
  port?: number;
  hosts?: string[];
}

interface DatabaseDescription {
  location: string; // the database location
  params: string;  // the database parameters
  comment: string; // the database comment
  database: string; // the database name
}

interface ColumnDescription {
  name: string; // the column name
  type: string; // the type of data in the column
  comment?: string; // the column comment
}

interface TableDescription {
  columns: ColumnDescription[]; // list of column names and types
  database: string; // the database name
  table: string; // the table name
  partitioned?: boolean; // true if the table is partitioned
  location?: string; // location of table
  outputFormat?: string; // output format
  owner?: string; // the owner's username
  partitionColumns?: ColumnDescription[]; // list of the partition columns
  inputFormat?: string; // input format
}

interface PartitionValue {
  columnName: string; // the partition value name
  columnValue: string; // the partition value value
}

interface PartitionOverview {
  name: string; // the partition name
  values: PartitionValue[]; // list of partition values
}

interface PartitionDescription {
  database: string; // the database name
  table: string; // the table name
  partition: string; // the partition name
  partitioned: boolean; // true if the table is partitioned
  location: string; // location of table
  outputFormat: string; // output format
  columns: ColumnDescription[]; // list of column names, types, and comments
  owner: string; // the owner's username
  partitionColumns: ColumnDescription[]; // list of the partition columns
  inputFormat: string; // input format
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

  private get(path: string, queryParams?: StringMap) {
    return this.request(path, 'GET', queryParams);
  }

  private request(
    path: string,
    method: string,
    queryParams: StringMap = {},
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

  /**
   * Return a list of supported response types.
   */
  listResponseTypes(): Promise<string[]> {
    return this.get('/')
      .then(res => res.responseTypes);
  }

  /**
   * Return the WebHCat server status.
   * @return 'ok' if working
   */
  getServerStatus(): Promise<string> {
    return this.get('/status')
      .then(res => res.status);
  }

  /**
   * Return the Hive version being run.
   */
  getHiveVersion(): Promise<string> {
    return this.get('/version/hive')
      .then(res => res.version);
  }

  /**
   * Return the Hadoop version being run.
   */
  getHadoopVersion(): Promise<string> {
    return this.get('/version/hadoop')
      .then(res => res.version);
  }

  /**
   * List HCatalog databases.
   * @return list of database names
   */
  listDatabases(): Promise<String> {
    return this.get('/ddl/database')
      .then(res => res.databases);
  }

  /**
   * Describe an HCatalog database.
   * @param database the database name
   */
  describeDatabase(database: string): Promise<DatabaseDescription> {
    return this.get(`/ddl/database/${database}`);
  }

  /**
   * List the tables in an HCatalog database.
   * @param database the database name
   * @return list of table names
   */
  listTables(database: string): Promise<string[]> {
    return this.get(`/ddl/database/${database}/table`)
      .then(res => res.tables);
  }

  /**
   * Describe an HCatalog table.
   * @param database the database name
   * @param table the table name
   * @param extended set to true to see additional information
   */
  describeTable(database: string, table: string, extended= false): Promise<TableDescription> {
    const queryParams: StringMap = {};
    if (extended) queryParams.format = 'extended';
    return this.get(`/ddl/database/${database}/table/${table}`, queryParams);
  }

  /**
   * List all partitions in an HCatalog table.
   * @param {String} database the database name
   * @param {String} table the table name
   * @return {Promise<PartitionOverview[]>} list of partition names and values
   * @throws {StatusCodeError} with errorCode 10241 if table is not partitioned
   */
  listPartitions(database: string, table: string): Promise<PartitionOverview[]> {
    return this.get(`/ddl/database/${database}/table/${table}/partition`)
      .then(res => res.partitions);
  }

  /**
   * Describe a single partition in an HCatalog table.
   * @param {String} database the database name
   * @param {String} table the table name
   * @param {String} partition the partition name
   */
  describePartition(database: string, table: string, partition: string): Promise<PartitionDescription> {
    return this.get(`/ddl/database/${database}/table/${table}/partition/${partition}`);
  }

  /**
   * List the columns in an HCatalog table.
   * @param {String} database the database name
   * @param {String} table the table name
   */
  listColumns(database: string, table: string): Promise<ColumnDescription[]> {
    return this.get(`/ddl/database/${database}/table/${table}/column`)
      .then(res => res.columns);
  }

  /**
   * Describe a single column in an HCatalog table.
   * @param {String} database - the database name
   * @param {String} table - the table name
   * @param {String} column - the column name
   */
  describeColumn(database: string, table: string, column: string): Promise<ColumnDescription> {
    return this.get(`/ddl/database/${database}/table/${table}/column/${column}`)
      .then(res => res.column);
  }

  /**
   * List table properties.
   * @param {String} database the database name
   * @param {String} table the table name
   * @return key-value pairs
   */
  listProperties(database: string, table: string): Promise<StringMap> {
    return this.get(`/ddl/database/${database}/table/${table}/property`)
      .then(res => res.properties);
  }

  /**
   * Return the value of a single table property.
   * @param {String} database the database name
   * @param {String} table the table name
   * @param {String} property the property value
   */
  getPropertyValue(database: string, table: string, property: string): Promise<string> {
    return this.get(`/ddl/database/${database}/table/${table}/property/${property}`)
      .then(res => res.property[property]);
  }
}
