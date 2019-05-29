/*!
 * Copyright 2016 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as common from '@google-cloud/common-grpc';
import {promisifyAll} from '@google-cloud/promisify';
import arrify = require('arrify');

const concat = require('concat-stream');
import * as is from 'is';
const pumpify = require('pumpify');
import * as through from 'through2';
import {Family} from './family';
import {Filter} from './filter';
import {Mutation, Data, MutationSettingsObj, Value} from './mutation';
import {Row} from './row';
import {ChunkTransformer} from './chunktransformer';
import {CreateTableOptions, GetFamiliesCallback, GetFamiliesResponse, GetTableCallback, GetTableResponse, CreateTableCallback, CreateTableResponse, GetTableOptions, CreateReadStreamTableOptions, CreateFamilyTableOptions, CreateFamilyCallback, CreateFamilyResponse, DeleteTableCallback, DeleteTableRowsCallback, EmptyResponse, ExistsCallback, ExistsResponse, Entry, GetReplicationStatesCallback, GetReplicationStatesResponse, GetMetadataOptions, GetTableMetadataCallback, GetTableMetadataTableResponse, GetTableRowsOptions, GetTableRowsCallback, GetTableRowsResponse, InsertTableRowsCallback, InsertTableRowsResponse, MutateTableRowsOptions, MutateTableRowsCallback, MutateTableRowsResponse, SampleRowKeysCallback, SampleRowKeysResponse, TruncateTableCallback, TruncateTableResponse, WaitForReplicationCallback, WaitForReplicationResponse, GenerateConsistencyTokenCallback, GenerateConsistencyTokenResponse, CheckConsistencyCallback, CheckConsistencyResponse, Bigtable} from '.';
import {Instance} from './instance';
import {CallOptions} from 'google-gax';
import {google} from '../proto/bigtable';
import {ServiceError} from 'grpc';

export interface RangeOptions {
  value?: string|number|boolean|MutationSettingsObj|Value[]|Uint8Array;
  inclusive: boolean;
}

// See protos/google/rpc/code.proto
// (4=DEADLINE_EXCEEDED, 10=ABORTED, 14=UNAVAILABLE)
const RETRYABLE_STATUS_CODES = new Set([4, 10, 14]);

/**
 * Create a Table object to interact with a Cloud Bigtable table.
 *
 * @class
 * @param {Instance} instance Instance Object.
 * @param {string} id Unique identifier of the table.
 *
 * @example
 * const Bigtable = require('@google-cloud/bigtable');
 * const bigtable = new Bigtable();
 * const instance = bigtable.instance('my-instance');
 * const table = instance.table('prezzy');
 */
export class Table {
  bigtable: Bigtable;
  instance: Instance;
  name: string;
  id: string;
  metadata!: google.bigtable.admin.v2.ITable;
  maxRetries!: number;
  constructor(instance: Instance, id: string) {
    this.bigtable = instance.bigtable;
    this.instance = instance;

    let name: string;

    if (id.includes('/')) {
      if (id.startsWith(`${instance.name}/tables/`)) {
        name = id;
      } else {
        throw new Error(`Table id '${id}' is not formatted correctly.
Please use the format 'prezzy' or '${instance.name}/tables/prezzy'.`);
      }
    } else {
      name = `${instance.name}/tables/${id}`;
    }

    this.name = name;
    this.id = name.split('/').pop()!;
  }

  /**
   * Formats the table name to include the Bigtable cluster.
   *
   * @private
   *
   * @param {string} instanceName The formatted instance name.
   * @param {string} name The table name.
   *
   * @example
   * Table.formatName_(
   *   'projects/my-project/zones/my-zone/instances/my-instance',
   *   'my-table'
   * );
   * //
   * 'projects/my-project/zones/my-zone/instances/my-instance/tables/my-table'
   */
  static formatName_(instanceName: string, id: string) {
    if (id.includes('/')) {
      return id;
    }

    return `${instanceName}/tables/${id}`;
  }

  /**
   * Creates a range based off of a key prefix.
   *
   * @private
   *
   * @param {string} start The key prefix/starting bound.
   * @returns {object} range
   *
   * @example
   * const Bigtable = require('@google-cloud/bigtable');
   * const bigtable = new Bigtable();
   * const instance = bigtable.instance('my-instance');
   * const table = instance.table('prezzy');
   * table.createPrefixRange('start');
   * // => {
   * //   start: 'start',
   * //   end: {
   * //     value: 'staru',
   * //     inclusive: false
   * //   }
   * // }
   */
  static createPrefixRange(start: string): PrefixRange {
    const prefix = start.replace(new RegExp('[\xff]+$'), '');
    let endKey = '';
    if (prefix) {
      const position = prefix.length - 1;
      const charCode = prefix.charCodeAt(position);
      const nextChar = String.fromCharCode(charCode + 1);
      endKey = prefix.substring(0, position) + nextChar;
    }
    return {
      start,
      end: {
        value: endKey,
        inclusive: !endKey,
      },
    };
  }

  /**
   * Create a table.
   *
   * @param {object} [options] See {@link Instance#createTable}.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/global.html#CallOptions.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {Table} callback.table The newly created table.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_create_table
   */
  create(options?: CreateTableOptions): Promise<CreateTableResponse>;
  create(callback: CreateTableCallback): void;
  create(options: CreateTableOptions, callback: CreateTableCallback): void;
  create(
      optionsOrCallback?: CreateTableOptions|CreateTableCallback,
      callback?: CreateTableCallback): void|Promise<CreateTableResponse> {
    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    this.instance.createTable(this.id, options, callback);
  }

  /**
   * Create a column family.
   *
   * Optionally you can send garbage collection rules and when creating a
   * family. Garbage collection executes opportunistically in the background, so
   * it's possible for reads to return a cell even if it matches the active
   * expression for its family.
   *
   * @see [Garbage Collection Proto Docs]{@link https://github.com/googleapis/googleapis/blob/master/google/bigtable/admin/table/v1/bigtable_table_data.proto#L59}
   *
   * @throws {error} If a name is not provided.
   *
   * @param {string} id The unique identifier of column family.
   * @param {object} [options] Configuration object.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/global.html#CallOptions.
   * @param {object} [options.rule] Garbage collection rule
   * @param {object} [options.rule.age] Delete cells in a column older than the
   *     given age. Values must be at least 1 millisecond.
   * @param {number} [options.rule.versions] Maximum number of versions to delete
   *     cells in a column, except for the most recent.
   * @param {boolean} [options.rule.intersect] Cells to delete should match all
   *     rules.
   * @param {boolean} [options.rule.union] Cells to delete should match any of the
   *     rules.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {Family} callback.family The newly created Family.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_create_family
   */
  createFamily(id: string, options?: CreateFamilyTableOptions):
      Promise<CreateFamilyResponse>;
  createFamily(id: string, callback: CreateFamilyCallback): void;
  createFamily(
      id: string, options: CreateFamilyTableOptions,
      callback: CreateFamilyCallback): void;
  createFamily(
      id: string,
      optionsOrCallback?: CreateFamilyTableOptions|CreateFamilyCallback,
      callback?: CreateFamilyCallback): void|Promise<CreateFamilyResponse> {
    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    if (!id) {
      throw new Error('An id is required to create a family.');
    }
    // tslint:disable-next-line no-any
    const mod: any = {
      id,
      create: {},
    };

    if (options.rule) {
      mod.create.gcRule = Family.formatRule_(options.rule);
    }

    const reqOpts = {
      name: this.name,
      modifications: [mod],
    };

    this.bigtable.request(
        {
          client: 'BigtableTableAdminClient',
          method: 'modifyColumnFamilies',
          reqOpts,
          gaxOpts: options.gaxOptions,
        },
        (err: Error, resp: google.bigtable.v2.IFamily) => {
          if (err) {
            callback!(err, null, resp);
            return;
          }

          const family = this.family(id);
          family.metadata = resp;

          callback!(null, family, resp);
        });
  }

  /**
   * Get {@link Row} objects for the rows currently in your table as a
   * readable object stream.
   *
   * @param {object} [options] Configuration object.
   * @param {boolean} [options.decode=true] If set to `false` it will not decode
   *     Buffer values returned from Bigtable.
   * @param {boolean} [options.encoding] The encoding to use when converting
   *     Buffer values to a string.
   * @param {string} [options.end] End value for key range.
   * @param {Filter} [options.filter] Row filters allow you to
   *     both make advanced queries and format how the data is returned.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {string[]} [options.keys] A list of row keys.
   * @param {number} [options.limit] Maximum number of rows to be returned.
   * @param {string} [options.prefix] Prefix that the row key must match.
   * @param {string[]} [options.prefixes] List of prefixes that a row key must
   *     match.
   * @param {object[]} [options.ranges] A list of key ranges.
   * @param {string} [options.start] Start value for key range.
   * @returns {stream}
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_table_readstream
   */
  createReadStream(options?: CreateReadStreamTableOptions):
      NodeJS.ReadWriteStream {
    options = options || {};
    const maxRetries = is.number(this.maxRetries) ? this.maxRetries : 3;

    let rowKeys: string[]|null;
    const ranges = options.ranges || [];
    let filter: Filter;
    let rowsLimit: number;
    let rowsRead = 0;
    let numRequestsMade = 0;

    if (options.start || options.end) {
      if (options.ranges || options.prefix || options.prefixes) {
        throw new Error(
            'start/end should be used exclusively to ranges/prefix/prefixes.');
      }
      ranges.push({
        start: options.start,
        end: options.end,
      });
    }

    if (options.keys) {
      rowKeys = options.keys;
    }

    if (options.prefix) {
      if (options.ranges || options.start || options.end || options.prefixes) {
        throw new Error(
            'prefix should be used exclusively to ranges/start/end/prefixes.');
      }
      ranges.push(Table.createPrefixRange(options.prefix));
    }

    if (options.prefixes) {
      if (options.ranges || options.start || options.end || options.prefix) {
        throw new Error(
            'prefixes should be used exclusively to ranges/start/end/prefix.');
      }
      options.prefixes.forEach(prefix => {
        ranges.push(Table.createPrefixRange(prefix));
      });
    }

    if (options.filter) {
      filter = Filter.parse(options.filter);
    }
    if (options.limit) {
      rowsLimit = options.limit;
    }

    const userStream = through.obj();
    let chunkTransformer: ChunkTransformer;

    const makeNewRequest = () => {
      const lastRowKey = chunkTransformer ? chunkTransformer.lastRowKey : '';
      // tslint:disable-next-line no-any
      chunkTransformer = new ChunkTransformer({decode: options!.decode} as any);
      // tslint:disable-next-line no-any
      const reqOpts: any = {
        tableName: this.name,
        appProfileId: this.bigtable.appProfileId,
      };

      const retryOpts = {
        currentRetryAttempt: numRequestsMade,
      };

      if (lastRowKey) {
        const lessThan = (lhs: Buffer|Data, rhs: Buffer|Data) => {
          const lhsBytes = Mutation.convertToBytes(lhs);
          const rhsBytes = Mutation.convertToBytes(rhs);
          return (lhsBytes as Buffer).compare(rhsBytes as Uint8Array) === -1;
        };
        const greaterThan = (lhs: Buffer|Data, rhs: Buffer|Data) =>
            lessThan(rhs, lhs);
        const greaterThanOrEqualTo = (lhs: Buffer|Data, rhs: Buffer|Data) =>
            !lessThan(rhs, lhs);

        if (ranges.length === 0) {
          ranges.push({
            start: {
              value: lastRowKey as string,
              inclusive: false,
            },
          });
        } else {
          // Readjust and/or remove ranges based on previous valid row reads.

          // Iterate backward since items may need to be removed.
          for (let index = ranges.length - 1; index >= 0; index--) {
            const range =
                ranges[index] as {start: RangeOptions, end: RangeOptions};
            const startValue =
                (is.object(range.start) ? range.start.value : range.start) as
                    Buffer |
                Data;
            const endValue =
                (is.object(range.end) ? range.end.value : range.end) as Buffer |
                Data;
            const isWithinStart = !startValue ||
                greaterThanOrEqualTo(startValue, lastRowKey as Buffer | Data);
            const isWithinEnd =
                !endValue || lessThan(lastRowKey as Buffer | Data, endValue);
            if (isWithinStart) {
              if (isWithinEnd) {
                // The lastRowKey is within this range, adjust the start
                // value.
                range.start = {
                  value: lastRowKey,
                  inclusive: false,
                };
              } else {
                // The lastRowKey is past this range, remove this range.
                ranges.splice(index, 1);
              }
            }
          }
        }

        // Remove rowKeys already read.
        if (rowKeys) {
          rowKeys = rowKeys.filter(
              rowKey => greaterThan(rowKey, lastRowKey as Buffer | Data));
          if (rowKeys.length === 0) {
            rowKeys = null;
          }
        }
      }
      if (rowKeys || ranges.length) {
        reqOpts.rows = {};

        if (rowKeys) {
          reqOpts.rows.rowKeys = rowKeys.map(Mutation.convertToBytes);
        }

        if (ranges.length) {
          reqOpts.rows.rowRanges = ranges.map(
              range => Filter.createRange(range.start, range.end, 'Key'));
        }
      }

      if (filter) {
        reqOpts.filter = filter;
      }

      if (rowsLimit) {
        reqOpts.rowsLimit = rowsLimit - rowsRead;
      }

      const requestStream = this.bigtable.request({
        client: 'BigtableClient',
        method: 'readRows',
        reqOpts,
        gaxOpts: options!.gaxOptions,
        retryOpts,
      });

      requestStream.on('request', () => numRequestsMade++);

      const rowStream = pumpify.obj([
        requestStream,
        chunkTransformer,
        through.obj((rowData, enc, next) => {
          if (chunkTransformer._destroyed ||
              // tslint:disable-next-line no-any
              (userStream as any)._writableState.ended) {
            return next();
          }
          numRequestsMade = 0;
          rowsRead++;
          const row = this.row(rowData.key);
          row.data = rowData.data;
          next(null, row);
        }),
      ]);

      rowStream.on('error', (error: ServiceError) => {
        rowStream.unpipe(userStream);
        if (numRequestsMade <= maxRetries &&
            RETRYABLE_STATUS_CODES.has(error.code!)) {
          makeNewRequest();
        } else {
          userStream.emit('error', error);
        }
      });
      rowStream.pipe(userStream);
    };

    makeNewRequest();
    return userStream;
  }

  /**
   * Delete the table.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} [callback] The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_del_table
   */
  delete(gaxOptions?: CallOptions): Promise<EmptyResponse>;
  delete(callback: DeleteTableCallback): void;
  delete(gaxOptions: CallOptions, callback: DeleteTableCallback): void;
  delete(
      gaxOptionsOrcallback?: CallOptions|DeleteTableCallback,
      callback?: DeleteTableCallback): void|Promise<EmptyResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    this.bigtable.request(
        {
          client: 'BigtableTableAdminClient',
          method: 'deleteTable',
          reqOpts: {
            name: this.name,
          },
          gaxOpts: gaxOptions,
        },
        callback);
  }

  /**
   * Delete all rows in the table, optionally corresponding to a particular
   * prefix.
   *
   * @throws {error} If a prefix is not provided.
   *
   * @param {string} prefix Row key prefix.
   * @param {object} [gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_del_rows
   */
  deleteRows(prefix: string, gaxOptions?: CallOptions): Promise<EmptyResponse>;
  deleteRows(prefix: string, callback: DeleteTableRowsCallback): void;
  deleteRows(
      prefix: string, gaxOptions: CallOptions,
      callback: DeleteTableRowsCallback): void;
  deleteRows(
      prefix: string,
      gaxOptionsOrcallback?: CallOptions|DeleteTableRowsCallback,
      callback?: DeleteTableRowsCallback): void|Promise<EmptyResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    if (!prefix || is.fn(prefix)) {
      throw new Error('A prefix is required for deleteRows.');
    }

    const reqOpts = {
      name: this.name,
      rowKeyPrefix: Mutation.convertToBytes(prefix),
    };

    this.bigtable.request(
        {
          client: 'BigtableTableAdminClient',
          method: 'dropRowRange',
          reqOpts,
          gaxOpts: gaxOptions,
        },
        callback);
  }

  /**
   * Check if a table exists.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {boolean} callback.exists Whether the table exists or not.
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_exists_table
   */
  exists(gaxOptions?: CallOptions): Promise<ExistsResponse>;
  exists(callback: ExistsCallback): void;
  exists(gaxOptions: CallOptions, callback: ExistsCallback): void;
  exists(
      gaxOptionsOrcallback?: CallOptions|ExistsCallback,
      callback?: ExistsCallback): void|Promise<ExistsResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    const reqOpts = {
      view: 'name',
      gaxOptions,
    };

    this.getMetadata(reqOpts, err => {
      if (err) {
        if (err.code === 5) {
          callback!(null, false);
          return;
        }

        callback!(err);
        return;
      }

      callback!(null, true);
    });
  }

  /**
   * Get a reference to a Table Family.
   *
   * @throws {error} If a name is not provided.
   *
   * @param {string} id The family unique identifier.
   * @returns {Family}
   *
   * @example
   * const family = table.family('my-family');
   */
  family(id: string): Family {
    if (!id) {
      throw new Error('A family id must be provided.');
    }
    return new Family(this, id);
  }

  /**
   * Get a reference to a Table Family.
   *
   * @throws {error} If a name is not provided.
   *
   * @param {string} id The family unique identifier.
   * @returns {Family}
   *
   * @example
   * const family = table.family('my-family');
   */
  /**
   * Get a table if it exists.
   *
   * You may optionally use this to "get or create" an object by providing an
   * object with `autoCreate` set to `true`. Any extra configuration that is
   * object as well.
   *
   * @param {object} [options] Configuration object.
   * @param {boolean} [options.autoCreate=false] Automatically create the
   *     instance if it does not already exist.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {?error} callback.error An error returned while making this request.
   * @param {Table} callback.table The Table object.
   * @param {object} callback.apiResponse The resource as it exists in the API.
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_get_table
   */
  get(options?: GetTableOptions): Promise<GetTableResponse>;
  get(callback: GetTableCallback): void;
  get(options: GetTableOptions, callback: GetTableCallback): void;
  get(optionsOrCallback?: GetTableOptions|GetTableCallback,
      callback?: GetTableCallback): void|Promise<GetTableResponse> {
    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    const autoCreate = !!options.autoCreate;
    const gaxOptions = options.gaxOptions;

    this.getMetadata({gaxOptions}, (err, metadata) => {
      if (err) {
        if (err.code === 5 && autoCreate) {
          this.create({gaxOptions}, callback!);
          return;
        }

        callback!(err);
        return;
      }

      callback!(null, this, metadata);
    });
  }

  /**
   * Get Family objects for all the column familes in your table.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {Family[]} callback.families The list of families.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_get_families
   */
  getFamilies(gaxOptions?: CallOptions): Promise<GetFamiliesResponse>;
  getFamilies(callback: GetFamiliesCallback): void;
  getFamilies(gaxOptions: CallOptions, callback: GetFamiliesCallback): void;
  getFamilies(
      gaxOptionsOrcallback?: CallOptions|GetFamiliesCallback,
      callback?: GetFamiliesCallback): void|Promise<GetFamiliesResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    this.getMetadata({gaxOptions}, (err, metadata) => {
      if (err) {
        callback!(err);
        return;
      }

      const families = Object.keys(metadata!.columnFamilies!).map(familyId => {
        const family = this.family(familyId);
        family.metadata = metadata!.columnFamilies![familyId];
        return family;
      });

      callback!(null, families, metadata!.columnFamilies);
    });
  }

  /**
   * Get replication states of the clusters for this table.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {Family[]} callback.clusterStates The map of clusterId and its replication state.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example
   * const Bigtable = require('@google-cloud/bigtable');
   * const bigtable = new Bigtable();
   * const instance = bigtable.instance('my-instance');
   * const table = instance.table('prezzy');
   *
   * table.getReplicationStates(function(err, clusterStates, apiResponse) {
   *   // `clusterStates` is an map of clusterId and its replication state.
   * });
   *
   * //-
   * // If the callback is omitted, we'll return a Promise.
   * //-
   * table.getReplicationStates().then(function(data) {
   *   var clusterStates = data[0];
   *   var apiResponse = data[1];
   * });
   */
  getReplicationStates(gaxOptions?: CallOptions):
      Promise<GetReplicationStatesResponse>;
  getReplicationStates(callback: GetReplicationStatesCallback): void;
  getReplicationStates(
      gaxOptions: CallOptions, callback: GetReplicationStatesCallback): void;
  getReplicationStates(
      gaxOptionsOrcallback?: CallOptions|GetReplicationStatesCallback,
      callback?: GetReplicationStatesCallback):
      void|Promise<GetReplicationStatesResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;
    const reqOpts = {
      view: 'replication',
      gaxOptions,
    };
    this.getMetadata(reqOpts, (err, metadata) => {
      if (err) {
        callback!(err);
        return;
      }
      const clusterStates = new Map();
      Object.keys(metadata!.clusterStates!)
          .map(
              clusterId => clusterStates.set(
                  clusterId, metadata!.clusterStates![clusterId]));
      callback!(null, clusterStates, metadata);
    });
  }

  /**
   * Get the table's metadata.
   *
   * @param {object} [options] Table request options.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {string} [options.view] The view to be applied to the table fields.
   * @param {function} [callback] The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {object} callback.metadata The table's metadata.
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_get_table_meta
   */
  getMetadata(options?: GetMetadataOptions):
      Promise<GetTableMetadataTableResponse>;
  getMetadata(callback: GetTableMetadataCallback): void;
  getMetadata(options: GetMetadataOptions, callback: GetTableMetadataCallback):
      void;
  getMetadata(
      optionsOrCallback?: GetMetadataOptions|GetTableMetadataCallback,
      callback?: GetTableMetadataCallback):
      void|Promise<GetTableMetadataTableResponse> {
    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    const reqOpts = {
      name: this.name,
      // tslint:disable-next-line no-any
      view: (Table as any).VIEWS[options.view || 'unspecified'],
    };

    this.bigtable.request(
        {
          client: 'BigtableTableAdminClient',
          method: 'getTable',
          reqOpts,
          gaxOpts: options.gaxOptions,
        },
        (...args: [Error, google.bigtable.admin.v2.ITable]) => {
          if (args[1]) {
            this.metadata = args[1];
          }

          callback!(...args);
        });
  }

  /**
   * Get {@link Row} objects for the rows currently in your table.
   *
   * This method is not recommended for large datasets as it will buffer all
   * rows before returning the results. Instead we recommend using the streaming
   * API via {@link Table#createReadStream}.
   *
   * @param {object} [options] Configuration object. See
   *     {@link Table#createReadStream} for a complete list of options.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {Row[]} callback.rows List of Row objects.
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_get_rows
   */
  getRows(options: GetTableRowsOptions): Promise<GetTableRowsResponse>;
  getRows(callback: GetTableRowsCallback): void;
  getRows(options: GetTableRowsOptions, callback: GetTableRowsCallback): void;
  getRows(
      optionsOrCallback?: GetTableRowsOptions|GetTableRowsCallback,
      callback?: GetTableRowsCallback): void|Promise<GetTableRowsResponse> {
    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    this.createReadStream(options)
        .on('error', callback!)
        .pipe(concat((rows: Row[]) => {
          callback!(null, rows);
        }));
  }

  /**
   * Insert or update rows in your table. It should be noted that gRPC only
   * allows you to send payloads that are less than or equal to 4MB. If you're
   * inserting more than that you may need to send smaller individual requests.
   *
   * @param {object|object[]} entries List of entries to be inserted.
   *     See {@link Table#mutate}.
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {object[]} callback.err.errors If present, these represent partial
   *     failures. It's possible for part of your request to be completed
   *     successfully, while the other part was not.
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_insert_rows
   */
  insert(entries: Entry|Entry[], gaxOptions?: CallOptions):
      Promise<InsertTableRowsResponse>;
  insert(entries: Entry|Entry[], callback: InsertTableRowsCallback): void;
  insert(
      entries: Entry|Entry[], gaxOptions: CallOptions,
      callback: InsertTableRowsCallback): void;
  insert(
      entries: Entry|Entry[],
      gaxOptionsOrcallback?: CallOptions|InsertTableRowsCallback,
      callback?: InsertTableRowsCallback):
      void|Promise<InsertTableRowsResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    entries = arrify(entries).map(entry => {
      entry.method = Mutation.methods.INSERT;
      return entry;
    });

    return this.mutate(entries, {gaxOptions}, callback!);
  }

  /**
   * Apply a set of changes to be atomically applied to the specified row(s).
   * Mutations are applied in order, meaning that earlier mutations can be
   * masked by later ones.
   *
   * @param {object|object[]} entries List of entities to be inserted or
   *     deleted.
   * @param {object} [options] Configuration object.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/global.html#CallOptions.
   * @param {boolean} [options.rawMutation] If set to `true` will treat entries
   *     as a raw Mutation object. See {@link Mutation#parse}.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {object[]} callback.err.errors If present, these represent partial
   *     failures. It's possible for part of your request to be completed
   *     successfully, while the other part was not.
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_mutate_rows
   */
  mutate(entries: Entry|Entry[], options?: MutateTableRowsOptions):
      Promise<MutateTableRowsResponse>;
  mutate(entries: Entry|Entry[], callback: MutateTableRowsCallback): void;
  mutate(
      entries: Entry|Entry[], options: MutateTableRowsOptions,
      callback: MutateTableRowsCallback): void;
  mutate(
      entries: Entry|Entry[],
      optionsOrCallback?: MutateTableRowsOptions|MutateTableRowsCallback,
      callback?: MutateTableRowsCallback):
      void|Promise<MutateTableRowsResponse> {
    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    entries = arrify(entries).reduce((a, b) => a.concat(b as []), []);

    let numRequestsMade = 0;

    const maxRetries = is.number(this.maxRetries) ? this.maxRetries : 3;
    const pendingEntryIndices = new Set(entries.map((entry, index) => index));
    const entryToIndex = new Map(entries.map((entry, index) => [entry, index]));
    const mutationErrorsByEntryIndex = new Map();

    const onBatchResponse = (err: ServiceError) => {
      if (err) {
        // The error happened before a request was even made, don't
        // retry.
        callback!(err);
        return;
      }
      if (pendingEntryIndices.size !== 0 && numRequestsMade <= maxRetries) {
        makeNextBatchRequest();
        return;
      }

      if (mutationErrorsByEntryIndex.size !== 0) {
        const mutationErrors = Array.from(mutationErrorsByEntryIndex.values());
        err = new common.util.PartialFailureError({
          errors: mutationErrors,
          // tslint:disable-next-line no-any
        } as any);
      }

      callback!(err);
    };

    const makeNextBatchRequest = () => {
      const entryBatch = (entries as Entry[]).filter((entry, index) => {
        return pendingEntryIndices.has(index);
      });

      const reqOpts = {
        tableName: this.name,
        appProfileId: this.bigtable.appProfileId,
        entries: options.rawMutation ?
            entryBatch :  // tslint:disable-next-line no-any
            (entryBatch as any).map(Mutation.parse),
      };

      const retryOpts = {
        currentRetryAttempt: numRequestsMade,
      };

      this.bigtable
          .request({
            client: 'BigtableClient',
            method: 'mutateRows',
            reqOpts,
            gaxOpts: options.gaxOptions,
            retryOpts,
          })
          .on('request', () => numRequestsMade++)
          .on('error',
              (err: ServiceError) => {
                if (numRequestsMade === 0) {
                  callback!(err);  // Likely a "projectId not detected" error.
                  return;
                }

                onBatchResponse(err);
              })
          .on('data',
              (obj: {
                entries: google.bigtable.v2.MutateRowsResponse.IEntry[]
              }) => {
                obj.entries.forEach(
                    (entry: google.bigtable.v2.MutateRowsResponse.IEntry) => {
                      const originalEntry = entryBatch[entry.index as number];
                      const originalEntriesIndex =
                          entryToIndex.get(originalEntry)!;

                      // Mutation was successful.
                      if (entry.status!.code === 0) {
                        pendingEntryIndices.delete(originalEntriesIndex);
                        mutationErrorsByEntryIndex.delete(originalEntriesIndex);
                        return;
                      }

                      if (!RETRYABLE_STATUS_CODES.has(
                              entry.status!.code as number)) {
                        pendingEntryIndices.delete(originalEntriesIndex);
                      }

                      const status =
                          // tslint:disable-next-line no-any
                          (common.Service as any).decorateStatus_(entry.status);
                      status.entry = originalEntry;

                      mutationErrorsByEntryIndex.set(
                          originalEntriesIndex, status);
                    });
              })
          .on('end', onBatchResponse);
    };
    makeNextBatchRequest();
  }

  /**
   * Get a reference to a table row.
   *
   * @throws {error} If a key is not provided.
   *
   * @param {string} key The row key.
   * @returns {Row}
   *
   * @example
   * var row = table.row('lincoln');
   */
  row(key: string): Row {
    if (!key) {
      throw new Error('A row key must be provided.');
    }

    return new Row(this, key);
  }

  /**
   * Returns a sample of row keys in the table. The returned row keys will
   * delimit contigous sections of the table of approximately equal size, which
   * can be used to break up the data for distributed tasks like mapreduces.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} [callback] The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {object[]} callback.keys The list of keys.
   *
   * @example <caption>include:samples/document-snippets/table.js</caption>
   * region_tag:bigtable_sample_row_keys
   */
  sampleRowKeys(gaxOptions?: CallOptions): Promise<SampleRowKeysResponse>;
  sampleRowKeys(callback?: SampleRowKeysCallback): void;
  sampleRowKeys(gaxOptions: CallOptions, callback?: SampleRowKeysCallback):
      void;
  sampleRowKeys(
      gaxOptionsOrcallback?: CallOptions|SampleRowKeysCallback,
      callback?: SampleRowKeysCallback): void|Promise<SampleRowKeysResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    this.sampleRowKeysStream(gaxOptions)
        .on('error', callback!)
        .pipe(concat((keys: SampleRowKeysResponse) => {
          callback!(null, keys);
        }));
  }

  /**
   * Returns a sample of row keys in the table as a readable object stream.
   *
   * See {@link Table#sampleRowKeys} for more details.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @returns {stream}
   *
   * @example
   * table.sampleRowKeysStream()
   *   .on('error', console.error)
   *   .on('data', function(key) {
   *     // Do something with the `key` object.
   *   });
   *
   * //-
   * // If you anticipate many results, you can end a stream early to prevent
   * // unnecessary processing.
   * //-
   * table.sampleRowKeysStream()
   *   .on('data', function(key) {
   *     this.end();
   *   });
   */
  sampleRowKeysStream(gaxOptions?: CallOptions): NodeJS.ReadWriteStream {
    const reqOpts = {
      tableName: this.name,
      appProfileId: this.bigtable.appProfileId,
    };

    return pumpify.obj([
      this.bigtable.request({
        client: 'BigtableClient',
        method: 'sampleRowKeys',
        reqOpts,
        gaxOpts: gaxOptions,
      }),
      through.obj((key, enc, next) => {
        next(null, {
          key: key.rowKey,
          offset: key.offsetBytes,
        });
      }),
    ]);
  }

  /**
   * Truncate the table.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example
   * table.truncate(function(err, apiResponse) {});
   *
   * //-
   * // If the callback is omitted, we'll return a Promise.
   * //-
   * table.truncate().then(function(data) {
   *   var apiResponse = data[0];
   * });
   */
  truncate(gaxOptions: CallOptions): Promise<TruncateTableResponse>;
  truncate(callback: TruncateTableCallback): void;
  truncate(gaxOptions: CallOptions, callback: TruncateTableCallback): void;
  truncate(
      gaxOptionsOrcallback?: CallOptions|TruncateTableCallback,
      callback?: TruncateTableCallback): void|Promise<TruncateTableResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    const reqOpts = {
      name: this.name,
      deleteAllDataFromTable: true,
    };

    this.bigtable.request(
        {
          client: 'BigtableTableAdminClient',
          method: 'dropRowRange',
          reqOpts,
          gaxOpts: gaxOptions,
        },
        callback);
  }

  /**
   * Generates Consistency-Token and check consistency for generated token
   * In-case consistency check returns false, retrial is done in interval
   * of 5 seconds till 10 minutes, after that it returns false.
   *
   * @param {function(?error, ?boolean)} callback The callback function.
   * @param {?Error} callback.err An error returned while making this request.
   * @param {?Boolean} callback.resp Boolean value.
   */
  waitForReplication(): Promise<WaitForReplicationResponse>;
  waitForReplication(callback: WaitForReplicationCallback): void;
  waitForReplication(callback?: WaitForReplicationCallback):
      void|Promise<WaitForReplicationResponse> {
    // handler for generated consistency-token
    const tokenHandler = (err: ServiceError|null, token?: string|null) => {
      if (err) {
        return callback!(err);
      }

      // set timeout for 10 minutes
      const timeoutAfterTenMinutes = setTimeout(() => {
        callback!(null, false);
      }, 10 * 60 * 1000);

      // method checks if retrial is required & init retrial with 5 sec
      // delay
      const retryIfNecessary = (err: ServiceError|null, res?: boolean|null) => {
        if (err) {
          clearTimeout(timeoutAfterTenMinutes);
          return callback!(err);
        }

        if (res === true) {
          clearTimeout(timeoutAfterTenMinutes);
          return callback!(null, true);
        }

        setTimeout(launchCheck, 5000);
      };

      // method to launch token consistency check
      const launchCheck = () => {
        this.checkConsistency(token!, retryIfNecessary);
      };

      launchCheck();
    };


    // generate consistency-token
    this.generateConsistencyToken(tokenHandler);
  }

  /**
   * Generates Consistency-Token
   * @param {function(?error, ?boolean)} callback The callback function.
   * @param {?Error} callback.err An error returned while making this request.
   * @param {?String} callback.token The generated consistency token.
   */
  generateConsistencyToken(): Promise<GenerateConsistencyTokenResponse>;
  generateConsistencyToken(callback: GenerateConsistencyTokenCallback): void;
  generateConsistencyToken(callback?: GenerateConsistencyTokenCallback):
      void|Promise<GenerateConsistencyTokenResponse> {
    const reqOpts = {
      name: this.name,
    };

    this.bigtable.request(
        {
          client: 'BigtableTableAdminClient',
          method: 'generateConsistencyToken',
          reqOpts,
        },
        (err: Error,
         res: google.bigtable.admin.v2.GenerateConsistencyTokenResponse) => {
          if (err) {
            callback!(err);
            return;
          }

          callback!(null, res.consistencyToken);
        });
  }

  /**
   * Checks consistency for given ConsistencyToken
   * @param {string} token consistency token
   * @param {function(?error, ?boolean)} callback The callback function.
   * @param {?Error} callback.err An error returned while making this request.
   * @param {?Boolean} callback.consistent Boolean value.
   */
  checkConsistency(token: string): Promise<CheckConsistencyResponse>;
  checkConsistency(token: string, callback: CheckConsistencyCallback): void;
  checkConsistency(token: string, callback?: CheckConsistencyCallback):
      void|Promise<CheckConsistencyResponse> {
    const reqOpts = {
      name: this.name,
      consistencyToken: token,
    };

    this.bigtable.request(
        {
          client: 'BigtableTableAdminClient',
          method: 'checkConsistency',
          reqOpts,
        },
        (err: Error,
         res: google.bigtable.admin.v2.CheckConsistencyResponse) => {
          if (err) {
            callback!(err);
            return;
          }

          callback!(null, res.consistent);
        });
  }
  /**
   * The view to be applied to the returned table's fields.
   * Defaults to schema if unspecified.
   *
   * @private
   */
  static VIEWS = {
    unspecified: 0,
    name: 1,
    schema: 2,
    replication: 3,
    full: 4,
  };
}

/*! Developer Documentation
 *
 * All async methods (except for streams) will return a Promise in the event
 * that a callback is omitted.
 */
promisifyAll(Table, {
  exclude: ['family', 'row'],
});

export interface PrefixRange {
  start: string;
  end: {value: string; inclusive: boolean;};
}
