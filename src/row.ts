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

import {promisifyAll} from '@google-cloud/promisify';
import arrify = require('arrify');

const dotProp = require('dot-prop');
import * as is from 'is';
import {Filter} from './filter';
import {Mutation, IMutation} from './mutation';
import {Bigtable, CreateRowOptions, CreateRowCallback, CreateRowResponse, CreateRulesCallback, CreateRulesResponse, MutateTableRowsOptions, DeleteRowCallback, EmptyResponse, DeleteRowResponse, DeleteRowCellsCallback, DeleteRowCellsResponse, ExistsCallback, ExistsResponse, Entry, SaveRowCallback, SaveRowResponse, IncrementRowCallback, IncrementRowResponse, GetRowOptions, GetRowMetadataCallback, GetRowMetadataResponse, GetRowCallback, GetTableRowsOptions, GetRowResponse, FilterRowConfigOptions, FilterRowCallback, FilterRowResponse} from '.';
import {Table} from './table';
import {google} from '../proto/bigtable';
import {CallOptions} from 'google-gax';
import {Chunk, Qualifier} from './chunktransformer';
import {Family} from './family';


/**
 * @private
 */
export class RowError extends Error {
  code: number;
  constructor(row: string) {
    super();
    this.name = 'RowError';
    this.message = `Unknown row: ${row}.`;
    this.code = 404;
  }
}

export interface RowRule extends google.bigtable.v2.IReadModifyWriteRule {
  column?: string;
  append?: boolean;
  increment?: number|Long;
}

export interface RowData {
  // tslint:disable-next-line no-any
  data?: any;
  key?: string|Buffer;
}

export interface UserOptions {
  decode?: boolean;
  encoding?: string;
}

export interface UnformattedFamily {
  name: string;
  columns: [{
    qualifier: string;
    cells: Chunk[];
  }];
}

/**
 * Create a Row object to interact with your table rows.
 *
 * @class
 * @param {Table} table The row's parent Table instance.
 * @param {string} key The key for this row.
 *
 * @example
 * const Bigtable = require('@google-cloud/bigtable');
 * const bigtable = new Bigtable();
 * const instance = bigtable.instance('my-instance');
 * const table = instance.table('prezzy');
 * const row = table.row('gwashington');
 */
export class Row {
  bigtable: Bigtable;
  table: Table;
  id: string;
  data: google.bigtable.v2.IRow;
  constructor(table: Table, key: string) {
    this.bigtable = table.bigtable;
    this.table = table;
    this.id = key;

    this.data = {};
  }

  /**
   * Formats the row chunks into friendly format. Chunks contain 3 properties:
   *
   * `rowContents` The row contents, this essentially is all data pertaining
   *     to a single family.
   *
   * `commitRow` This is a boolean telling us the all previous chunks for this
   *     row are ok to consume.
   *
   * `resetRow` This is a boolean telling us that all the previous chunks are to
   *     be discarded.
   *
   * @private
   *
   * @param {chunk[]} chunks The list of chunks.
   * @param {object} [options] Formatting options.
   *
   * @example
   * Row.formatChunks_(chunks);
   * // {
   * //   follows: {
   * //     gwashington: [
   * //       {
   * //         value: 2
   * //       }
   * //     ]
   * //   }
   * // }
   */
  static formatChunks_(chunks: Chunk[], options: UserOptions) {
    // tslint:disable-next-line no-any
    const rows: any[] = [];
    let familyName: string|null;
    let qualifierName: string|null;

    options = options || {};

    chunks.reduce((row: RowData, chunk: Chunk) => {
      let family!: Family;
      let qualifier!: Qualifier|Qualifier[];

      row.data = row.data! || {};

      if (chunk.rowKey) {
        row.key = Mutation.convertFromBytes(chunk.rowKey as string, {
          userOptions: options,
        }) as string |
            Buffer;
      }

      if (chunk.familyName) {
        familyName = chunk.familyName.value;
      }

      if (familyName) {
        family = row.data[familyName] = row.data[familyName] || {};
      }

      if (chunk.qualifier) {
        qualifierName =
            Mutation.convertFromBytes(chunk.qualifier.value as string, {
              userOptions: options,
            }) as string;
      }

      if (family && qualifierName) {
        // tslint:disable-next-line no-any
        qualifier = (family as any)[qualifierName] =
            // tslint:disable-next-line no-any
            (family as any)[qualifierName] || [];
      }

      if (qualifier && chunk.value) {
        (qualifier as Qualifier[]).push({
          value: Mutation.convertFromBytes(
                     chunk.value, {userOptions: options}) as string |
              Buffer,
          labels: chunk.labels,
          timestamp: chunk.timestampMicros,
          size: chunk.valueSize,
        });
      }

      if (chunk.commitRow) {
        rows.push(row);
      }

      if (chunk.commitRow || chunk.resetRow) {
        familyName = qualifierName = null;
        return {};
      }

      return row;
    }, {});

    return rows;
  }

  /**
   * Formats a rowContents object into friendly format.
   *
   * @private
   *
   * @param {object[]} families The row families.
   * @param {object} [options] Formatting options.
   *
   * @example
   * var families = [
   *   {
   *     name: 'follows',
   *     columns: [
   *       {
   *         qualifier: 'gwashington',
   *         cells: [
   *           {
   *             value: 2
   *           }
   *         ]
   *       }
   *     ]
   *   }
   * ];
   *
   * Row.formatFamilies_(families);
   * // {
   * //   follows: {
   * //     gwashington: [
   * //       {
   * //         value: 2
   * //       }
   * //     ]
   * //   }
   * // }
   */
  static formatFamilies_(
      families: google.bigtable.v2.IFamily[], options?: UserOptions) {
    const data = {};
    options = options || {};
    families.forEach(family => {
      // tslint:disable-next-line no-any
      const familyData = ((data as any)[family.name!] = {});
      (family as {} as UnformattedFamily).columns.forEach(column => {
        const qualifier = Mutation.convertFromBytes(column.qualifier);
        // tslint:disable-next-line no-any
        (familyData as any)[qualifier as any] = column.cells.map(cell => {
          let value = cell.value;
          if (options!.decode !== false) {
            value = Mutation.convertFromBytes(
                        value!, {isPossibleNumber: true}) as string |
                Buffer;
          }
          return {
            value,
            timestamp: cell.timestampMicros,
            labels: cell.labels,
          };
        });
      });
    });

    return data;
  }

  /**
   * Create a new row in your table.
   *
   * @param {object} [options] Configuration object.
   * @param {object} [options.entry] An entry. See {@link Table#insert}.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {Row} callback.row The newly created row object.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/row.js</caption>
   * region_tag:bigtable_create_row
   */
  create(options?: CreateRowOptions): Promise<CreateRowResponse>;
  create(callback: CreateRowCallback): void;
  create(options: CreateRowOptions, callback: CreateRowCallback): void;
  create(
      optionsOrCallback?: CreateRowOptions|CreateRowCallback,
      callback?: CreateRowCallback): void|Promise<CreateRowResponse> {
    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    const entry = {
      key: this.id,
      data: options.entry,
      method: Mutation.methods.INSERT,
    };
    this.data = {};

    this.table.mutate(
        entry, options.gaxOptions as MutateTableRowsOptions,
        // tslint:disable-next-line no-any
        (err, apiResponse: any) => {
          if (err) {
            callback!(err, null, apiResponse);
            return;
          }

          callback!(null, this, apiResponse);
        });
  }

  /**
   * Update a row with rules specifying how the row's contents are to be
   * transformed into writes. Rules are applied in order, meaning that earlier
   * rules will affect the results of later ones.
   *
   * @throws {error} If no rules are provided.
   *
   * @param {object|object[]} rules The rules to apply to this row.
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/row.js</caption>
   * region_tag:bigtable_create_rules
   */
  createRules(rules: RowRule|RowRule[], gaxOptions?: CallOptions):
      Promise<CreateRulesResponse>;
  createRules(rules: RowRule|RowRule[], callback: CreateRulesCallback): void;
  createRules(
      rules: RowRule|RowRule[], gaxOptions: CallOptions,
      callback: CreateRulesCallback): void;
  createRules(
      rules: RowRule|RowRule[],
      gaxOptionsOrcallback?: CallOptions|CreateRulesCallback,
      callback?: CreateRulesCallback): void|Promise<CreateRulesResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    if (!rules || (rules as RowRule[]).length === 0) {
      throw new Error('At least one rule must be provided.');
    }

    rules = arrify(rules).map(rule => {
      const column = Mutation.parseColumnName(rule.column!);
      const ruleData = {
        familyName: column.family,
        columnQualifier: Mutation.convertToBytes(column.qualifier!),
      } as google.bigtable.v2.IReadModifyWriteRule;

      if (rule.append) {
        ruleData.appendValue =
            Mutation.convertToBytes(rule.append) as Uint8Array;
      }
      if (rule.increment) {
        ruleData.incrementAmount = rule.increment;
      }

      return ruleData;
    });

    const reqOpts = {
      tableName: this.table.name,
      appProfileId: this.bigtable.appProfileId,
      rowKey: Mutation.convertToBytes(this.id),
      rules,
    };
    this.data = {};
    this.bigtable.request(
        {
          client: 'BigtableClient',
          method: 'readModifyWriteRow',
          reqOpts,
          gaxOpts: gaxOptions,
        },
        callback);
  }

  /**
   * Deletes all cells in the row.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/row.js</caption>
   * region_tag:bigtable_delete_all_cells
   */
  delete(gaxOptions?: CallOptions): Promise<DeleteRowResponse>;
  delete(callback: DeleteRowCallback): void;
  delete(gaxOptions: CallOptions, callback: DeleteRowCallback): void;
  delete(
      gaxOptionsOrcallback?: CallOptions|DeleteRowCallback,
      callback?: DeleteRowCallback): void|Promise<DeleteRowResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    const mutation = {
      key: this.id,
      method: Mutation.methods.DELETE,
    };
    this.data = {};
    this.table.mutate(
        mutation, gaxOptions as MutateTableRowsOptions, callback!);
  }

  /**
   * Delete specified cells from the row. See {@link Table#mutate}.
   *
   * @param {string[]} columns Column names for the cells to be deleted.
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/row.js</caption>
   * region_tag:bigtable_delete_particular_cells
   */
  deleteCells(columns: string[], gaxOptions?: CallOptions):
      Promise<DeleteRowCellsResponse>;
  deleteCells(columns: string[], callback: DeleteRowCellsCallback): void;
  deleteCells(
      columns: string[], gaxOptions: CallOptions,
      callback: DeleteRowCellsCallback): void;
  deleteCells(
      columns: string[],
      gaxOptionsOrcallback?: CallOptions|DeleteRowCellsCallback,
      callback?: DeleteRowCellsCallback): void|Promise<DeleteRowCellsResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    const mutation = {
      key: this.id,
      data: arrify(columns),
      method: Mutation.methods.DELETE,
    };
    this.data = {};
    this.table.mutate(
        mutation, gaxOptions as MutateTableRowsOptions, callback!);
  }

  /**
   * Check if the table row exists.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {boolean} callback.exists Whether the row exists or not.
   *
   * @example <caption>include:samples/document-snippets/row.js</caption>
   * region_tag:bigtable_row_exists
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

    this.getMetadata(gaxOptions as GetRowOptions, err => {
      if (err) {
        if (err instanceof RowError) {
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
   * Mutates a row atomically based on the output of a filter. Depending on
   * whether or not any results are yielded, either the `onMatch` or `onNoMatch`
   * callback will be executed.
   *
   * @param {Filter} filter Filter to be applied to the contents of the row.
   * @param {object} config Configuration object.
   * @param {?object[]} config.onMatch A list of entries to be ran if a match is
   *     found.
   * @param {object[]} [config.onNoMatch] A list of entries to be ran if no
   *     matches are found.
   * @param {object} [config.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/global.html#CallOptions.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {boolean} callback.matched Whether a match was found or not.
   *
   * @example <caption>include:samples/document-snippets/row.js</caption>
   * region_tag:bigtable_row_filter
   */
  filter(filter: Filter, config: FilterRowConfigOptions):
      Promise<FilterRowResponse>;
  filter(
      filter: Filter, config: FilterRowConfigOptions,
      callback: FilterRowCallback): void;
  filter(
      filter: Filter, config: FilterRowConfigOptions,
      callback?: FilterRowCallback): void|Promise<FilterRowResponse> {
    const reqOpts = {
      tableName: this.table.name,
      appProfileId: this.bigtable.appProfileId,
      rowKey: Mutation.convertToBytes(this.id),
      predicateFilter: Filter.parse(filter),
      trueMutations: createFlatMutationsList(config.onMatch!),
      falseMutations: createFlatMutationsList(config.onNoMatch!),
    };
    this.data = {};
    this.bigtable.request(
        {
          client: 'BigtableClient',
          method: 'checkAndMutateRow',
          reqOpts,
          gaxOpts: config.gaxOptions,
        },
        (err: Error,
         apiResponse: google.bigtable.v2.CheckAndMutateRowResponse) => {
          if (err) {
            callback!(err, null, apiResponse);
            return;
          }

          callback!(null, apiResponse.predicateMatched, apiResponse);
        });

    function createFlatMutationsList(entries: IMutation[]) {
      entries = arrify(entries).map(
                    entry => Mutation.parse(entry as Mutation).mutations) as [];
      return entries.reduce((a, b) => a.concat(b as []), []);
    }
  }

  /**
   * Get the row data. See {@link Table#getRows}.
   *
   * @param {string[]} [columns] List of specific columns to retrieve.
   * @param {object} [options] Configuration object.
   * @param {boolean} [options.decode=true] If set to `false` it will not decode Buffer
   *     values returned from Bigtable.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {Row} callback.row The updated Row object.
   *
   * @example <caption>include:samples/document-snippets/row.js</caption>
   * region_tag:bigtable_get_row
   */
  get(columns: string[], options: GetRowOptions,
      callback: GetRowCallback): void;
  get(options?: GetRowOptions): Promise<GetRowResponse>;
  get(columns: string[], options?: GetRowOptions): Promise<GetRowResponse>;
  get(callback: GetRowCallback): void;
  get(options: GetRowOptions, callback: GetRowCallback): void;
  get(columns: string[], callback: GetRowCallback): void;
  get(columnsOrOpts?: string[]|GetRowOptions|GetRowCallback,
      optionsOrCallback?: GetRowOptions|GetRowCallback,
      callback?: GetRowCallback): void|Promise<GetRowResponse> {
    let columns = (is.array(columnsOrOpts) ? columnsOrOpts : []) as string[];
    const options =
        (!is.array(columnsOrOpts) ?
             columnsOrOpts :
             typeof optionsOrCallback === 'object' ? optionsOrCallback : {}) as
        GetRowOptions;
    callback = typeof columnsOrOpts === 'function' ?
        columnsOrOpts :
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    let filter!: Array<{}>;
    columns = arrify(columns);

    // if there is column filter
    if (columns.length) {
      const filters = columns.map(Mutation.parseColumnName).map(column => {
        // tslint:disable-next-line no-any
        const colmFilters: any = [{family: column.family}];
        if (column.qualifier) {
          colmFilters.push({column: column.qualifier});
        }
        return colmFilters;
      });

      // if there is more then one filter, make it type inteleave filter
      if (filters.length > 1) {
        filter = [
          {
            interleave: filters,
          },
        ];
      } else {
        filter = filters[0];
      }
    }

    // if there is also a second option.filter append to filter array
    if (options.filter) {
      filter = arrify(filter).concat(options.filter);
    }

    const getRowsOptions = Object.assign({}, options, {
      keys: [this.id],
      filter,
    });

    this.table.getRows(getRowsOptions as GetTableRowsOptions, (err, rows) => {
      if (err) {
        callback!(err);
        return;
      }

      const row = rows![0];

      if (!row) {
        err = new RowError(this.id);
        callback!(err);
        return;
      }

      this.data = row.data;

      // If the user specifies column names, we'll return back the row data
      // we received. Otherwise, we'll return the row "this" in a typical
      // GrpcServiceObject#get fashion.
      callback!(null, (columns.length ? row.data : this) as Row);
    });
  }

  /**
   * Get the row's metadata.
   *
   * @param {object} [options] Configuration object.
   * @param {boolean} [options.decode=true] If set to `false` it will not decode
   *     Buffer values returned from Bigtable.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {object} callback.metadata The row's metadata.
   *
   * @example <caption>include:samples/document-snippets/row.js</caption>
   * region_tag:bigtable_get_row_meta
   */
  getMetadata(options?: GetRowOptions): Promise<GetRowMetadataResponse>;
  getMetadata(callback: GetRowMetadataCallback): void;
  getMetadata(options: GetRowOptions, callback: GetRowMetadataCallback): void;
  getMetadata(
      optionsOrCallback?: GetRowOptions|GetRowMetadataCallback,
      callback?: GetRowMetadataCallback): void|Promise<GetRowMetadataResponse> {
    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;
    // tslint:disable-next-line no-any
    this.get(options, (err, row: any) => {
      if (err) {
        callback!(err);
        return;
      }

      callback!(null, row.metadata);
    });
  }

  /**
   * Increment a specific column within the row. If the column does not
   * exist, it is automatically initialized to 0 before being incremented.
   *
   * @param {string} column The column we are incrementing a value in.
   * @param {number} [value] The amount to increment by, defaults to 1.
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {number} callback.value The updated value of the column.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/row.js</caption>
   * region_tag:bigtable_row_increment
   */
  increment(column: string, gaxOptions?: CallOptions):
      Promise<IncrementRowResponse>;
  increment(column: string, value: number, gaxOptions?: CallOptions):
      Promise<IncrementRowResponse>;
  increment(
      column: string, gaxOptions: CallOptions,
      callback: IncrementRowCallback): void;
  increment(column: string, callback: IncrementRowCallback): void;
  increment(column: string, value: number, callback: IncrementRowCallback):
      void;
  increment(
      column: string, value: number, gaxOptions: CallOptions,
      callback: IncrementRowCallback): void;
  increment(
      column: string, valueOrOptsOrCb?: number|CallOptions|IncrementRowCallback,
      gaxOptionsOrcallback?: CallOptions|IncrementRowCallback,
      callback?: IncrementRowCallback): void|Promise<IncrementRowResponse> {
    const value = typeof valueOrOptsOrCb === 'number' ? valueOrOptsOrCb : 1;
    const gaxOptions = typeof valueOrOptsOrCb === 'object' ?
        valueOrOptsOrCb :
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof valueOrOptsOrCb === 'function' ?
        valueOrOptsOrCb :
        typeof gaxOptionsOrcallback === 'function' ? gaxOptionsOrcallback :
                                                     callback;


    const reqOpts = {
      column,
      increment: value,
    } as RowRule;

    this.createRules(reqOpts, gaxOptions, (err, resp) => {
      if (err) {
        callback!(err, null, resp);
        return;
      }

      const data = Row.formatFamilies_(resp!.row!.families!);
      const value = dotProp.get(data, column.replace(':', '.'))[0].value;

      callback!(null, value, resp);
    });
  }

  /**
   * Update the row cells.
   *
   * @param {object} key An entry object to be inserted into the row. See
   *     {@link Table#insert}.
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/row.js</caption>
   * region_tag:bigtable_row_save
   */
  save(entry: Entry, gaxOptions?: CallOptions): Promise<SaveRowResponse>;
  save(entry: Entry, callback: SaveRowCallback): void;
  save(entry: Entry, gaxOptions: CallOptions, callback: SaveRowCallback): void;
  save(
      entry: Entry, gaxOptionsOrcallback?: CallOptions|SaveRowCallback,
      callback?: SaveRowCallback): void|Promise<SaveRowResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    const mutation = {
      key: this.id,
      data: entry,
      method: Mutation.methods.INSERT,
    };
    this.data = {};
    this.table.mutate(
        mutation, gaxOptions as MutateTableRowsOptions, callback!);
  }
}

/*! Developer Documentation
 *
 * All async methods (except for streams) will return a Promise in the event
 * that a callback is omitted.
 */
promisifyAll(Row);
