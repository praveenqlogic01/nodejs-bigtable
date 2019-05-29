/*!
 * Copyright 2018 Google Inc. All Rights Reserved.
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
import * as is from 'is';
import snakeCase = require('lodash.snakecase');
import {Cluster} from './cluster';
import {Bigtable, AppProfileOptions, CreateAppProfileCallback, CreateAppProfileResponse, DeleteAppProfileOptions, DeleteAppProfileCallback, EmptyResponse, ExistsCallback, ExistsResponse, GetAppProfileCallback, GetAppProfileResponse, GetAppProfileMetadataCallback, GetAppProfileMetadataResponse, Arguments, SetAppProfileMetadataCallback, SetAppProfileMetadataResponse} from '.';
import {Instance} from './instance';
import {Metadata} from '@google-cloud/common';
import {google} from '../proto/bigtable';
import {CallOptions} from 'grpc';
/**
 * Create an app profile object to interact with your app profile.
 *
 * @class
 * @param {Instance} instance The parent instance of this app profile.
 * @param {string} name Name of the app profile.
 *
 * @example
 * const Bigtable = require('@google-cloud/bigtable');
 * const bigtable = new Bigtable();
 * const instance = bigtable.instance('my-instance');
 * const appProfile = instance.appProfile('my-app-profile');
 */
export class AppProfile {
  bigtable: Bigtable;
  instance: Instance;
  name: string;
  id: string;
  metadata: Metadata;
  constructor(instance: Instance, id: string) {
    this.bigtable = instance.bigtable;
    this.instance = instance;

    let name;

    if (id.includes('/')) {
      if (id.startsWith(`${instance.name}/appProfiles/`)) {
        name = id;
      } else {
        throw new Error(`AppProfile id '${id}' is not formatted correctly.
Please use the format 'my-app-profile' or '${
            instance.name}/appProfiles/my-app-profile'.`);
      }
    } else {
      name = `${instance.name}/appProfiles/${id}`;
    }

    this.id = name.split('/').pop()!;
    this.name = name;
  }

  /**
   * Formats a app profile options object into proto format.
   *
   * @private
   *
   * @param {object} options The options object.
   * @returns {object}
   *
   * @example
   * // Any cluster routing:
   * Family.formatAppProfile_({
   *   routing: 'any',
   *   description: 'My App Profile',
   * });
   * // {
   * //   multiClusterRoutingUseAny: {},
   * //   description: 'My App Profile',
   * // }
   *
   * // Single cluster routing:
   * const cluster = myInstance.cluster('my-cluster');
   * Family.formatAppProfile_({
   *   routing: cluster,
   *   allowTransactionalWrites: true,
   *   description: 'My App Profile',
   * });
   * // {
   * //   singleClusterRouting: {
   * //     clusterId: 'my-cluster',
   * //     allowTransactionalWrites: true,
   * //   },
   * //   description: 'My App Profile',
   * // }
   */
  static formatAppProfile_(options: AppProfileOptions):
      google.bigtable.admin.v2.IAppProfile {
    const appProfile: google.bigtable.admin.v2.IAppProfile = {};

    if (options.routing) {
      if (options.routing === 'any') {
        appProfile.multiClusterRoutingUseAny = {};
      } else if (options.routing instanceof Cluster) {
        appProfile.singleClusterRouting = {
          clusterId: options.routing.id,
        };
        if (is.boolean(options.allowTransactionalWrites)) {
          appProfile.singleClusterRouting.allowTransactionalWrites =
              options.allowTransactionalWrites;
        }
      } else {
        throw new Error(
            'An app profile routing policy can only contain "any" or a `Cluster`.');
      }
    }

    if (is.string(options.description)) {
      appProfile.description = options.description;
    }

    return appProfile;
  }

  /**
   * Create an app profile.
   *
   * @param {object} [options] See {@link Instance#createAppProfile}.
   *
   * @example
   * <caption>include:samples/document-snippets/app-profile.js</caption>
   * region_tag:bigtable_create_app_profile
   */
  create(options?: AppProfileOptions): Promise<CreateAppProfileResponse>;
  create(callback: CreateAppProfileCallback): void;
  create(options: AppProfileOptions, callback: CreateAppProfileCallback): void;
  create(
      optionsOrCallback?: AppProfileOptions|CreateAppProfileCallback,
      callback?: CreateAppProfileCallback):
      void|Promise<CreateAppProfileResponse> {
    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;
    this.instance.createAppProfile(this.id, options, callback);
  }

  /**
   * Delete the app profile.
   *
   * @param {object} [options] Cluster creation options.
   * @param {object} [options.gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/global.html#CallOptions.
   * @param {boolean} [options.ignoreWarnings] Whether to ignore safety checks
   *     when deleting the app profile.
   * @param {function} [callback] The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example
   * <caption>include:samples/document-snippets/app-profile.js</caption>
   * region_tag:bigtable_delete_app_profile
   */
  delete(options?: DeleteAppProfileOptions): Promise<EmptyResponse>;
  delete(callback: DeleteAppProfileCallback): void;
  delete(options: DeleteAppProfileOptions, callback: DeleteAppProfileCallback):
      void;
  delete(
      optionsOrCallback?: DeleteAppProfileOptions|DeleteAppProfileCallback,
      callback?: DeleteAppProfileCallback): void|Promise<EmptyResponse> {
    const options =
        typeof optionsOrCallback === 'object' ? optionsOrCallback : {};
    callback =
        typeof optionsOrCallback === 'function' ? optionsOrCallback : callback;
    // tslint:disable-next-line no-any
    const reqOpts: any = {
      name: this.name,
    };

    if (is.boolean(options.ignoreWarnings)) {
      reqOpts.ignoreWarnings = options.ignoreWarnings;
    }

    this.bigtable.request(
        {
          client: 'BigtableInstanceAdminClient',
          method: 'deleteAppProfile',
          reqOpts,
          gaxOpts: options.gaxOptions,
        },
        callback);
  }

  /**
   * Check if an app profile exists.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {boolean} callback.exists Whether the app profile exists or not.
   *
   * @example
   * <caption>include:samples/document-snippets/app-profile.js</caption>
   * region_tag:bigtable_exists_app_profile
   */
  exists(gaxOptions?: CallOptions): Promise<ExistsResponse>;
  exists(callback: ExistsCallback): void;
  exists(gaxOptions: CallOptions, callback: ExistsCallback): void;
  exists(
      gaxOptionsOrcallback?: CallOptions|ExistsCallback,
      callback?: ExistsCallback): void|Promise<ExistsResponse> {
    const gaxOptions =
        (typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback :
                                                    {}) as CallOptions;
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
   * Get a appProfile if it exists.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   *
   * @example
   * <caption>include:samples/document-snippets/app-profile.js</caption>
   * region_tag:bigtable_get_app_profile
   */
  get(gaxOptions?: CallOptions): Promise<GetAppProfileResponse>;
  get(callback: GetAppProfileCallback): void;
  get(gaxOptions: CallOptions, callback: GetAppProfileCallback): void;
  get(gaxOptionsOrcallback?: CallOptions|GetAppProfileCallback,
      callback?: GetAppProfileCallback): void|Promise<GetAppProfileResponse> {
    const gaxOptions =
        (typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback :
                                                    {}) as CallOptions;
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    this.getMetadata(gaxOptions, (err, metadata) => {
      callback!(err, err ? null : this, metadata);
    });
  }

  /**
   * Get the app profile metadata.
   *
   * @param {object} [gaxOptions] Request configuration options, outlined
   *     here: https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this
   *     request.
   * @param {object} callback.metadata The metadata.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example
   * <caption>include:samples/document-snippets/app-profile.js</caption>
   * region_tag:bigtable_app_profile_get_meta
   */
  getMetadata(gaxOptions?: CallOptions): Promise<GetAppProfileMetadataResponse>;
  getMetadata(callback: GetAppProfileMetadataCallback): void;
  getMetadata(gaxOptions: CallOptions, callback: GetAppProfileMetadataCallback):
      void;
  getMetadata(
      gaxOptionsOrcallback?: CallOptions|GetAppProfileMetadataCallback,
      callback?: GetAppProfileMetadataCallback):
      void|Promise<GetAppProfileMetadataResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;

    this.bigtable.request(
        {
          client: 'BigtableInstanceAdminClient',
          method: 'getAppProfile',
          reqOpts: {
            name: this.name,
          },
          gaxOpts: gaxOptions,
        },
        (...args: Arguments<AppProfile>) => {
          if (args[1]) {
            this.metadata = args[1];
          }

          callback!(...args);
        });
  }

  /**
   * Set the app profile metadata.
   *
   * @param {object} metadata See {@link Instance#createAppProfile} for the
   *     available metadata options.
   * @param {object} [gaxOptions] Request configuration options, outlined here:
   *     https://googleapis.github.io/gax-nodejs/CallSettings.html.
   * @param {function} callback The callback function.
   * @param {?error} callback.err An error returned while making this request.
   * @param {object} callback.apiResponse The full API response.
   *
   * @example
   * <caption>include:samples/document-snippets/app-profile.js</caption>
   * region_tag:bigtable_app_profile_set_meta
   */
  setMetadata(metadata: AppProfileOptions, gaxOptions?: CallOptions):
      Promise<SetAppProfileMetadataResponse>;
  setMetadata(
      metadata: AppProfileOptions,
      callback: SetAppProfileMetadataCallback): void;
  setMetadata(
      metadata: AppProfileOptions, gaxOptions: CallOptions,
      callback: SetAppProfileMetadataCallback): void;
  setMetadata(
      metadata: AppProfileOptions,
      gaxOptionsOrcallback?: CallOptions|SetAppProfileMetadataCallback,
      callback?: SetAppProfileMetadataCallback):
      void|Promise<SetAppProfileMetadataResponse> {
    const gaxOptions =
        typeof gaxOptionsOrcallback === 'object' ? gaxOptionsOrcallback : {};
    callback = typeof gaxOptionsOrcallback === 'function' ?
        gaxOptionsOrcallback :
        callback;
    // tslint:disable-next-line no-any
    const reqOpts: any = {
      appProfile: AppProfile.formatAppProfile_(metadata),
      updateMask: {
        paths: [],
      },
    };
    reqOpts.appProfile.name = this.name;

    const fieldsForMask = [
      'description',
      'singleClusterRouting',
      'multiClusterRoutingUseAny',
      'allowTransactionalWrites',
    ];

    fieldsForMask.forEach(field => {
      if (reqOpts.appProfile[field]) {
        reqOpts.updateMask.paths.push(snakeCase(field));
      }
    });

    if (is.boolean(metadata.ignoreWarnings)) {
      reqOpts.ignoreWarnings = metadata.ignoreWarnings;
    }

    this.bigtable.request(
        {
          client: 'BigtableInstanceAdminClient',
          method: 'updateAppProfile',
          reqOpts,
          gaxOpts: gaxOptions,
        },
        callback);
  }
}

/*! Developer Documentation
 *
 * All async methods (except for streams) will return a Promise in the event
 * that a callback is omitted.
 */
promisifyAll(AppProfile);

/**
 * Reference to the {@link AppProfile} class.
 * @name module:@google-cloud/bigtable.AppProfile
 * @see AppProfile
 */
