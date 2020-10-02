/*
 * Copyright 2020 Craig Howard <craig@choward.ca>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const debug = require('debug')('signalk-to-timestream');
const trace = require('debug')('signalk-to-timestream:trace');
const aws = require('aws-sdk');
const timestream = new aws.TimestreamWrite({ apiVersion: '2018-11-01' });

module.exports = function(app) {
    let _database_name;
    let _table_name;
    let _handle_delta;
    let _publish_interval;

    let _value_to_type = function(point) {
        const value = point.value;

        if (typeof(value) == 'string') {
            return "VARCHAR";
        } else if (typeof(value) == 'number') {
            return "DOUBLE";
        } else if (typeof(value) == 'bool') {
            return "BOOLEAN";
        } else {
            debug(`could not determine type for ${point.name}=${value}`);
            return "VARCHAR";
        }
    };

    // publish the batch to timestream
    let _publish_to_timstream = function(batch_of_points) {
        let records = batch_of_points.map(function(point) {
            return {
                MeasureName: point.name,
                MeasureValue: `${point.value}`,
                MeasureValueType: _value_to_type(point),
                Time: `${point.timestamp}`
            };
        });
        // TODO: this assumes self only
        let common_attributes = {
            TimeUnit: "MILLISECONDS",
            Dimensions: [{
                Name: "context",
                Value: app.selfId
            }]
        };

        let params = {
            DatabaseName: _database_name,
            TableName: _table_name,
            Records: records,
            CommonAttributes: common_attributes
        };

        if (records.length > 0) {
            trace(`publishing ${JSON.stringify(params)}`);
            timestream.writeRecords(params, function(err, data) {
                if (err) {
                    debug(err);
                } else {
                    trace(`publish ok: ${JSON.stringify(data)}`);
                }
            });
        } else {
            debug('nothing to publish');
        }
    };

    let _construct_filter_function = function(options) {
        const regexes = options.filter_list.map(function(path) {
            // path is a glob pattern, and we need to convert it to a regex matcher
            let regex = path;
            // first convert '.' to '\.'
            regex = regex.replace(/\./gi, '\\.');
            // next convert '*' to '.*'
            regex = regex.replace(/\*/gi, '.*');
            // finally always do a full match
            regex = `^${regex}$`;
            trace(`created regex=${regex} from path=${path}`);
            // and create the regex
            return new RegExp(regex, 'g');
        });
        return function(value) {
            // TODO: it might be more efficient to create a single giant regex
            // on startup than to do .some() or .every()
            if (options.filter_list_type == 'include') {
                // if we're filtering to include elements, we'll include if at
                // least one regex matches (ie, the search finds something)
                return regexes.some(function(re) { return value.path.search(re) != -1; });
            } else {
                // if we're filtering to exclude, we'll include this in the
                // result if every regex doesn't match (ie search finds
                // nothing)
                return regexes.every(function(re) { return value.path.search(re) == -1; });
            }
        };
    };

    let _add_delta_to_batch = function(options) {
        // construct the filter function once and use the result
        let filter_function = _construct_filter_function(options);

        return function(delta, batch_of_points) {
            // filter out deltas not about us
            // TODO: should I just use subscriptionmanager and subscribe?
            if (delta.context !== 'vessels.self' && delta.context === app.selfId) {
                return;
            }

            if (!delta.updates) {
                return;
            }

            delta.updates.forEach(function(update) {
                if (!update.values) {
                    return;
                }

                // start with all update values
                points = update.values;
                // deal with the include/exclude list
                points = points.filter(filter_function);
                // convert from signalk delta format to individual data points
                points = points.map(function(value) {
                    return {
                        name: value.path,
                        value: value.value,
                        timestamp: Date.parse(update.timestamp)
                    };
                });

                // TODO: perhaps I should do some key-wise merging to reduce the data
                // points published?
                batch_of_points.push(...points);
            });
        };
    };

    let _create_handle_delta = function(options) {
        // construct the filter function once and use the result
        let add_to_batch = _add_delta_to_batch(options);

        // cache the points here for a batch upload
        let batch_of_points = [];

        // periodically publish the batched metrics to timestream
        _publish_interval = setInterval(function() {
            // publish
            _publish_to_timstream(batch_of_points);
            // reset the batch of points
            batch_of_points = [];
        }, options.write_interval * 1000);

        // add a delta to the batch
        return function(delta) {
            return add_to_batch(delta, batch_of_points);
        };
    };

    let _start = function(options) {
        debug('starting');
        _database_name = options.database;
        _table_name = options.table;

        // observe all the deltas
        app.signalk.on('delta', _create_handle_delta(options));
    };

    let _stop = function(options) {
        debug('stopping');
        // TODO: publish last batch
        clearInterval(_publish_interval);
    };

    return {
        id: 'signalk-to-timestream',
        name: 'Amazon Timestream publisher',
        description: 'SignalK server plugin that publishes data to Amazon Timestream',

        schema: {
            type: 'object',
            required: ['database', 'table'],
            properties: {
                database: {
                    type: 'string',
                    title: 'Timestream Database Name to Publish to'
                },
                table: {
                    type: 'string',
                    title: 'Timestream Table Name to Publish to'
                },
                write_interval: {
                    type: 'number',
                    title: 'Frequency to push updates (in seconds)',
                    default: 60
                },
                filter_list_type: {
                    type: 'string',
                    title: 'Type of List',
                    description: 'Either include or exclude the paths when publishing to Timestream',
                    default: ['exclude'],
                    enum: ['include', 'exclude']
                },
                filter_list: {
                    title: 'SignalK Paths',
                    description: 'A list of paths to be excluded or included',
                    type: 'array',
                    default: [],
                    items: {
                        type: 'string',
                        title: 'Path'
                    }
                }
            }
        },

        start: _start,
        stop: _stop
    };
};
