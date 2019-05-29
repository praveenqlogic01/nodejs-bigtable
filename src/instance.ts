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

import {paginator} from '@google-cloud/paginator';
import {promisifyAll} from '@google-cloud/promisify';
import * as is from 'is';
import snakeCase = require('lodash.snakecase');
import {AppProfile} from './app-profile';
import {Cluster, CreateClusterOptions, CreateClusterCallback, CreateClusterResponse, GetClusterResponse} from './cluster';
import {Family} from './family';
import {Table} from './table';
import {Bigtable, CreateInstanceOptions, CreateInstanceCallback, CreateInstanceResponse, AppProfileOptions, CreateAppProfileCallback, CreateAppProfileResponse, Arguments, CreateTableOptions, CreateTableCallback, CreateTableResponse, DeleteInstanceCallback, EmptyResponse, ExistsResponse, GetInstanceCallback, GetInstanceResponse, GetAppProfilesCallback, GetAppProfilesResponse, GetClustersCallback, GetTablesOptions, GetTablesCallback, GetTablesResponse, SetInstanceMetadataCallback, SetInstanceMetadataResponse} from '.';
import {google} from '../proto/bigtable';
import {CallOptions} from 'google-gax';
import {ExistsCallback, Metadata} from '@google-cloud/common';


/**
 * Create an Instance object to interact with a Cloud Bigtable instance.
 *
 * @class
 * @param {Bigtable} bigtable The parent {@link Bigtable} object of this
 *     instance.
 * @param {string} id Id of the instance.
 *
 * @example
 * const Bigtable = require('@google-cloud/bigtable');
 * const bigtable = new Bigtable();
 * const instance = bigtable.instance('my-instance');
 */

export class Instance {
  bigtable: Bigtable;
  id: string;
  name: string;
  metadata!: google.bigtable.admin.v2.IInstance;
  getTablesStream!: Function;
  constructor(bigtable: Bigtable, id: string) {
    this.bigtable = bigtable;

    let name;
    if (id.includes('/')) {
      if (id.startsWith(`${bigtable.projectName}/instances/`)) {
        name = id;
      } else {
        throw new Error(`Instance id '${id}' is not formatted correctly.
Please use the format 'my-instance' or '${
            bigtable.projectName}/instances/my-instance'.`);
      }
    } else {
      name = `${bigtable.projectName}/instances/${id}`;
    }

    this.id = name.split('/').pop()!;
    this.name = name;
  }

  /**
   * Maps the instance type to the proper integer.
   *
   * @private
   *
   * @param {string} type The instance type (production, development).
   * @returns {number}
   *
   * @example
   * Instance.getTypeType_('production');
   * // 1
   */
  static getTypeType_(type: string): number {
    const types: {[key: string]: number} = {
      unspecified: 0,
      production: 1,
      development: 2,
    };

    if (is.string(type)) {
      type = type.toLowerCase();
    }

    return types[type] || types.unspecified;
  }

  /**
   * Get a reference to a Bigtable App Profile.
   *
   * @param {string} name The name of the app profile.
   * @returns {AppProfile}
   */
  appProfile(name: string): AppProfile {
    return new AppProfile(this, name);
  }

  /**
   * Create an instance.
   *
   * @param {object} options See {@link Bigtable#createInstance}.
   * @param {object} [options.gaxOptions]  Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/global.html#CallOptions.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {Instance} callback.instance The newly created
   *     instance.
   * @param {Operation} callback.operation An operation object that can be used
   *     to check the status of the request.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/instance.js</caption>
   * region_tag:bigtable_create_instance
   */
  create(options: CreateInstanceOptions): Promise<CreateInstanceResponse>;
  create(callback: CreateInstanceCallback): void;
  create(options: CreateInstanceOptions, callback: CreateInstanceCallback):
      void;
  create(
      optionsOrCallback: CreateInstanceOptions|CreateInstanceCallback,
      callback?: CreateInstanceCallback): void|Promise<CreateInstanceResponse> {
    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    this.bigtable.createInstance(this.id, options, callback);
  }

  /**
   * Create an app profile.
   *
   * @param {string} id The name to be used when referring to the new
   *     app profile within its instance.
   * @param {object} options AppProfile creation options.
   * @param {'any'|Cluster} options.routing  The routing policy for all
   *     read/write requests which use this app profile. This can be either the
   *     string 'any' or a cluster of an instance. This value is required when
   *     creating the app profile and optional when setting the metadata.
   * @param {object} [options.gaxOptions]  Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/global.html#CallOptions.
   * @param {boolean} [options.allowTransactionalWrites] Whether or not
   *     CheckAndMutateRow and ReadModifyWriteRow requests are allowed by this
   *     app profile. It is unsafe to send these requests to the same
   *     table/row/column in multiple clusters. This is only used when the
   *     routing value is a cluster.
   * @param {string} [options.description] The long form description of the use
   *     case for this AppProfile.
   * @param {string} [options.ignoreWarnings] Whether to ignore safety checks
   *     when creating the app profile
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {Cluster} callback.appProfile The newly created app profile.
   *
   * @example <caption>include:samples/document-snippets/instance.js</caption>
   * region_tag:bigtable_create_app_profile
   */
  createAppProfile(
      id: string, options: AppProfileOptions,
      callback: CreateAppProfileCallback): void;
  createAppProfile(id: string, options: AppProfileOptions):
      Promise<CreateAppProfileResponse>;
  createAppProfile(id: string, callback: CreateAppProfileCallback): void;
  createAppProfile(
      id: string, optionsOrCallback: AppProfileOptions|CreateAppProfileCallback,
      callback?: CreateAppProfileCallback):
      void|Promise<CreateAppProfileResponse> {
    const options =
        (typeof optionsOrCallback === 'object' ? optionsOrCallback : {}) as
        AppProfileOptions;
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    if (!options.routing) {
      throw new Error('An app profile must contain a routing policy.');
    }

    const appProfile = AppProfile.formatAppProfile_(options);

    const reqOpts: any = {
      parent: this.name,
      appProfileId: id,
      appProfile,
    };


    if (is.boolean(options.ignoreWarnings)) {
      reqOpts.ignoreWarnings = options.ignoreWarnings;
    }

    this.bigtable.request(
        {
          client: 'BigtableInstanceAdminClient',
          method: 'createAppProfile',
          reqOpts,
          gaxOpts: options.gaxOptions,
        },
        (...args: Arguments<AppProfile>) => {
          if (args[1]) {
            args.splice(1, 0, this.appProfile(id));
          }

          callback!(...args);
        });
  }

  /**
   * Create a cluster.
   *
   * @param {string} id The id to be used when referring to the new
   *     cluster within its instance.
   * @param {object} options Cluster creation options.
   * @param {object} [options.gaxOptions]  Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/global.html#CallOptions.
   * @param {string} options.location The location where this cluster's nodes
   *     and storage reside. For best performance clients should be located as
   *     as close as possible to this cluster. Currently only zones are
   *     supported.
   * @param {number} options.nodes The number of nodes allocated to this
   *     cluster. More nodes enable higher throughput and more consistent
   *     performance.
   * @param {string} [options.storage] The type of storage used by this cluster
   *     to serve its parent instance's tables. Options are 'hdd' or 'ssd'.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {Cluster} callback.cluster The newly created
   *     cluster.
   * @param {Operation} callback.operation An operation object that can be used
   *     to check the status of the request.
   *
   * @example <caption>include:samples/document-snippets/instance.js</caption>
   * region_tag:bigtable_create_cluster
   */
  createCluster(id: string, callback: CreateClusterCallback): void;
  createCluster(
      id: string, options: CreateClusterOptions,
      callback: CreateClusterCallback): void;
  createCluster(id: string, options: CreateClusterOptions):
      Promise<CreateClusterResponse>;
  createCluster(
      id: string, optionsOrCallback: CreateClusterOptions|CreateClusterCallback,
      callback?: CreateClusterCallback): void|Promise<CreateClusterResponse> {
    const options =
        (typeof optionsOrCallback === 'object' ? optionsOrCallback : {}) as
        CreateClusterOptions;
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    const reqOpts: any = {
      parent: this.name,
      clusterId: id,
    };

    if (!is.empty(options)) {
      reqOpts.cluster = {};
    }
    if (options.location) {
      reqOpts.cluster.location =
          Cluster.getLocation_(this.bigtable.projectId, options.location);
    }

    if (options.nodes) {
      reqOpts.cluster.serveNodes = options.nodes;
    }
    if (options.storage) {
      const storageType = Cluster.getStorageType_(options.storage);
      reqOpts.cluster.defaultStorageType = storageType;
    }

    this.bigtable.request(
        {
          client: 'BigtableInstanceAdminClient',
          method: 'createCluster',
          reqOpts,
          gaxOpts: options.gaxOptions,
        },
        (...args: any[]) => {
          if (args[1]) {
            args.splice(1, 0, this.cluster(id));
          }

          callback!(...args);
        });
  }

  /**
   * Create a table on your Bigtable instance.
   *
   * @see [Designing Your Schema]{@link https://cloud.google.com/bigtable/docs/schema-design}
   * @see [Splitting Keys]{@link https://cloud.google.com/bigtable/docs/managing-tables#splits}
   *
   * @throws {error} If a id is not provided.
   *
   * @param {string} id Unique identifier of the table.
   * @param {object} [options] Table creation options.
   * @param {object|string[]} [options.families] Column families to be created
   *     within the table.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {string[]} [options.splits] Initial
   *    [split
   * keys](https://cloud.google.com/bigtable/docs/managing-tables#splits).
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {Table} callback.table The newly created table.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/instance.js</caption>
   * region_tag:bigtable_create_table
   */
  createTable(id: string, options?: CreateTableOptions):
      Promise<CreateTableResponse>;
  createTable(id: string, callback: CreateTableCallback): void;
  createTable(
      id: string, options: CreateTableOptions,
      callback: CreateTableCallback): void;
  createTable(
      id: string, optionsOrCallback?: CreateTableOptions|CreateTableCallback,
      callback?: CreateTableCallback): void|Promise<CreateTableResponse> {
    if (!id) {
      throw new Error('An id is required to create a table.');
    }

    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    const reqOpts: any = {
      parent: this.name,
      tableId: id,
      table: {
        // The granularity at which timestamps are stored in the table.
        // Currently only milliseconds is supported, so it's not
        // configurable.
        granularity: 0,
      },
    };

    if (options.splits) {
      reqOpts.initialSplits = options.splits.map(key => ({
                                                   key,
                                                 }));
    }

    if (options.families) {
      const columnFamilies = options.families.reduce((families, family) => {
        if (is.string(family)) {
          family = {
            name: family,
          } as Family;
        }

        const columnFamily: google.bigtable.admin.v2.IColumnFamily =
            ((families as any)[family.name] = {});

        if ((family as any).rule) {
          columnFamily.gcRule = Family.formatRule_((family as any).rule);
        }

        return families;
      }, {});

      reqOpts.table.columnFamilies = columnFamilies;
    }

    this.bigtable.request(
        {
          client: 'BigtableTableAdminClient',
          method: 'createTable',
          reqOpts,
          gaxOpts: options.gaxOptions,
        },
        (...args: Arguments<Table>) => {
          if (args[1]) {
            const table = this.table(args[1].name.split('/').pop());
            table.metadata = args[1];
            args.splice(1, 0, table);
          }

          callback!(...args);
        });
  }

  /**
   * Get a reference to a Bigtable Cluster.
   *
   * @param {string} id The id of the cluster.
   * @returns {Cluster}
   */
  cluster(id: string): Cluster {
    return new Cluster(this, id);
  }

  /**
   * Delete the instance.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} [callback] The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/instance.js</caption>
   * region_tag:bigtable_del_instance
   */
  delete(gaxOptions?: CallOptions): Promise<EmptyResponse>;
  delete(callback: DeleteInstanceCallback): void;
  delete(gaxOptions: CallOptions, callback: DeleteInstanceCallback): void;
  delete(
      gaxOptionsOrcallback?: CallOptions|DeleteInstanceCallback,
      callback?: DeleteInstanceCallback): void|Promise<EmptyResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    this.bigtable.request(
        {
          client: 'BigtableInstanceAdminClient',
          method: 'deleteInstance',
          reqOpts: {
            name: this.name,
          },
          gaxOpts: gaxOptions,
        },
        callback);
  }

  /**
   * Check if an instance exists.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {boolean} callback.exists Whether the instance exists or not.
   *
   * @example <caption>include:samples/document-snippets/instance.js</caption>
   * region_tag:bigtable_exists_instance
   */
  exists(gaxOptions: CallOptions): Promise<ExistsResponse>;
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

    this.getMetadata(gaxOptions, err => {
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
   * Get an instance if it exists.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.error An error returned while making this request.
   * @param {Instance} callback.instance The Instance object.
   * @param {object} callback.apiResponse The resource as it exists in the API.
   *
   * @example <caption>include:samples/document-snippets/instance.js</caption>
   * region_tag:bigtable_get_instance
   */
  get(gaxOptions: CallOptions): Promise<GetInstanceResponse>;
  get(callback?: GetInstanceCallback): void;
  get(gaxOptions: CallOptions, callback?: GetInstanceCallback): void;
  get(gaxOptionsOrcallback?: CallOptions|GetInstanceCallback,
      callback?: GetInstanceCallback): void|Promise<GetInstanceResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    this.getMetadata(gaxOptions, (err, metadata) => {
      callback!(err, err ? null : this, metadata);
    });
  }

  /**
   * Get App Profile objects for this instance.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.error An error returned while making this request.
   * @param {AppProfile[]} callback.appProfiles List of all AppProfiles.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/instance.js</caption>
   * region_tag:bigtable_get_app_profiles
   */
  getAppProfiles(gaxOptions?: CallOptions): Promise<GetAppProfilesResponse>;
  getAppProfiles(callback: GetAppProfilesCallback): void;
  getAppProfiles(gaxOptions: CallOptions, callback: GetAppProfilesCallback):
      void;
  getAppProfiles(
      gaxOptionsOrcallback?: CallOptions|GetAppProfilesCallback,
      callback?: GetAppProfilesCallback): void|Promise<GetAppProfilesResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    const reqOpts = {
      parent: this.name,
    };

    this.bigtable.request(
        {
          client: 'BigtableInstanceAdminClient',
          method: 'listAppProfiles',
          reqOpts,
          gaxOpts: gaxOptions,
        },
        (err: Error, resp: AppProfile[]) => {
          if (err) {
            callback!(err);
            return;
          }

          const appProfiles = resp.map((appProfileObj: AppProfile) => {
            const appProfile =
                this.appProfile(appProfileObj.name.split('/').pop()!);
            appProfile.metadata = appProfileObj;
            return appProfile;
          });

          callback!(null, appProfiles, resp);
        });
  }

  /**
   * Get Cluster objects for all of your clusters.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.error An error returned while making this request.
   * @param {Cluster[]} callback.clusters List of all
   *     Clusters.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/instance.js</caption>
   * region_tag:bigtable_get_clusters
   */
  getClusters(gaxOptions: CallOptions): Promise<GetClusterResponse>;
  getClusters(callback: GetClustersCallback): void;
  getClusters(gaxOptions: CallOptions, callback: GetClustersCallback): void;
  getClusters(
      gaxOptionsOrcallback?: CallOptions|GetClustersCallback,
      callback?: GetClustersCallback): void|Promise<GetClusterResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    const reqOpts = {
      parent: this.name,
    };

    this.bigtable.request(
        {
          client: 'BigtableInstanceAdminClient',
          method: 'listClusters',
          reqOpts,
          gaxOpts: gaxOptions,
        },
        (err: Error, resp: google.bigtable.admin.v2.IListClustersResponse) => {
          if (err) {
            callback!(err);
            return;
          }

          const clusters = resp.clusters!.map((clusterObj) => {
            const cluster = this.cluster(clusterObj.name!.split('/').pop()!);
            cluster.metadata = clusterObj as Metadata;
            return cluster;
          });

          callback!(null, clusters, resp);
        });
  }

  /**
   * Get the instance metadata.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {object} callback.metadata The metadata.
   *
   * @example <caption>include:samples/document-snippets/instance.js</caption>
   * region_tag:bigtable_get_instance_metadata
   */
  getMetadata(gaxOptions?: CallOptions): Promise<GetInstanceResponse>;
  getMetadata(callback: GetInstanceCallback): void;
  getMetadata(gaxOptions: CallOptions, callback: GetInstanceCallback): void;
  getMetadata(
      gaxOptionsOrcallback?: CallOptions|GetInstanceCallback,
      callback?: GetInstanceCallback): void|Promise<GetInstanceResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    this.bigtable.request(
        {
          client: 'BigtableInstanceAdminClient',
          method: 'getInstance',
          reqOpts: {
            name: this.name,
          },
          gaxOpts: gaxOptions,
        },
        (...args: Arguments<Instance>) => {
          if (args[1]) {
            this.metadata = args[1];
          }

          callback!(...args);
        });
  }

  /**
   * Get Table objects for all the tables in your Cloud Bigtable instance.
   *
   * @param {object} [options] Query object.
   * @param {boolean} [options.autoPaginate=true] Have pagination handled
   *     automatically.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/global.html#CallOptions.
   * @param {number} [options.maxApiCalls] Maximum number of API calls to make.
   * @param {number} [options.maxResults] Maximum number of items to return.
   * @param {string} [options.pageToken] A previously-returned page token
   *     representing part of a larger set of results to view.
   * @param {string} [options.view] View over the table's fields. Possible options
   *     are 'name', 'schema' or 'full'. Default: 'name'.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {Table[]} callback.tables List of all Table objects.These objects contains
   *     only table name & id but is not a complete representation of a table.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/instance.js</caption>
   * region_tag:bigtable_get_tables
   */
  getTables(options?: GetTablesOptions): Promise<GetTablesResponse>;
  getTables(callback: GetTablesCallback): void;
  getTables(options: GetTablesOptions, callback: GetTablesCallback): void;
  getTables(
      optionsOrCallback?: GetTablesOptions|GetTablesCallback,
      callback?: GetTablesCallback): void|Promise<GetTablesResponse> {
    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;

    const reqOpts = Object.assign({}, options, {
      parent: this.name,
      view: (Table.VIEWS as any)[options.view || 'unspecified'],
    });

    delete reqOpts.gaxOptions;

    this.bigtable.request(
        {
          client: 'BigtableTableAdminClient',
          method: 'listTables',
          reqOpts,
          gaxOpts: options.gaxOptions,
        },
        (...args: Arguments<Table[]>) => {
          if (args[1]) {
            args[1] = args[1].map(tableObj => {
              const table = this.table(tableObj.name.split('/').pop());
              table.metadata = tableObj;
              return table;
            });
          }
          callback!(...args);
        });
  }

  /**
   * Set the instance metadata.
   *
   * @param {object} metadata Metadata object.
   * @param {string} metadata.displayName The descriptive name for this
   *     instance as it appears in UIs. It can be changed at any time, but
   *     should be kept globally unique to avoid confusion.
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/global.html#CallOptions.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example <caption>include:samples/document-snippets/instance.js</caption>
   * region_tag:bigtable_set_meta_data
   */
  setMetadata(
      metadata: google.bigtable.admin.v2.IInstance,
      gaxOptions?: CallOptions): Promise<SetInstanceMetadataResponse>;
  setMetadata(
      metadata: google.bigtable.admin.v2.IInstance,
      callback: SetInstanceMetadataCallback): void;
  setMetadata(
      metadata: google.bigtable.admin.v2.IInstance, gaxOptions: CallOptions,
      callback: SetInstanceMetadataCallback): void;
  setMetadata(
      metadata: google.bigtable.admin.v2.IInstance,
      gaxOptionsOrcallback?: CallOptions|SetInstanceMetadataCallback,
      callback?: SetInstanceMetadataCallback):
      void|Promise<SetInstanceMetadataResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;
    const reqOpts: any = {
      instance: Object.assign({name: this.name}, metadata),
      updateMask: {
        paths: [],
      },
    };
    const fieldsForMask = ['displayName', 'type', 'labels'];

    fieldsForMask.forEach(field => {
      if (field in reqOpts.instance) {
        reqOpts.updateMask.paths.push(snakeCase(field));
      }
    });

    this.bigtable.request(
        {
          client: 'BigtableInstanceAdminClient',
          method: 'partialUpdateInstance',
          reqOpts,
          gaxOpts: gaxOptions,
        },
        (...args: Arguments<Instance>) => {
          if (args[1]) {
            this.metadata = args[1];
          }

          callback!(...args);
        });
  }

  /**
   * Get a reference to a Bigtable table.
   *
   * @param {string} id Unique identifier of the table.
   * @returns {Table}
   *
   * @example
   * const Bigtable = require('@google-cloud/bigtable');
   * const bigtable = new Bigtable();
   * const instance = bigtable.instance('my-instance');
   * const table = instance.table('presidents');
   */
  table(id: string): Table {
    return new Table(this, id);
  }
}

/**
 * Get {@link Table} objects for all the tables in your Cloud Bigtable
 * instance as a readable object stream.
 *
 * @param {object} [query] Configuration object. See
 *     {@link Instance#getTables} for a complete list of options.
 * @returns {stream}
 *
 * @example
 * const Bigtable = require('@google-cloud/bigtable');
 * const bigtable = new Bigtable();
 * const instance = bigtable.instance('my-instance');
 *
 * instance.getTablesStream()
 *   .on('error', console.error)
 *   .on('data', function(table) {
 *     // table is a Table object.
 *   })
 *   .on('end', function() {
 *     // All tables retrieved.
 *   });
 *
 * //-
 * // If you anticipate many results, you can end a stream early to prevent
 * // unnecessary processing and API requests.
 * //-
 * instance.getTablesStream()
 *   .on('data', function(table) {
 *     this.end();
 *   });
 */
Instance.prototype.getTablesStream = paginator.streamify('getTables');

/*! Developer Documentation
 *
 * These methods can be auto-paginated.
 */
paginator.extend(Instance, ['getTables']);

/*! Developer Documentation
 *
 * All async methods (except for streams) will return a Promise in the event
 * that a callback is omitted.
 */
promisifyAll(Instance, {
  exclude: ['appProfile', 'cluster', 'table'],
});

/**
 * Reference to the {@link Instance} class.
 * @name module:@google-cloud/bigtable.Instance
 * @see Instance
 */
