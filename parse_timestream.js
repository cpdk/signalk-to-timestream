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

const trace = require('debug')('signalk-to-timestream:parse_timestream:trace');
const _ = require('lodash');

/*
{
  "QueryId": "AEDACAMXZ5LXWH5BTZB77IYJJ366FPX4SUYY43DVEEWSTM4V5CEHUOAG7IQC6VY",
  "Rows": [
    {
      "Data": [
        {
          "ScalarValue": "urn:mrn:imo:mmsi:368107960"
        },
        {
          "ScalarValue": "4.294967167E7"
        },
        {
          "NullValue": true
        },
        {
          "ScalarValue": "navigation.courseRhumbline.nextPoint.distance"
        },
        {
          "ScalarValue": "2020-10-17 16:59:50.892000000"
        }
      ]
    }
  ],
  "ColumnInfo": [
    {
      "Name": "context",
      "Type": {
        "ScalarType": "VARCHAR"
      }
    },
    {
      "Name": "measure_value::double",
      "Type": {
        "ScalarType": "DOUBLE"
      }
    },
    {
      "Name": "measure_value::varchar",
      "Type": {
        "ScalarType": "VARCHAR"
      }
    },
    {
      "Name": "measure_name",
      "Type": {
        "ScalarType": "VARCHAR"
      }
    },
    {
      "Name": "time",
      "Type": {
        "ScalarType": "TIMESTAMP"
      }
    }
  ]
}
*/


let _build_mapper = function(columns) {
    let _get_column_idx = function(columns, name) {
        return _.findIndex(columns, col => col.Name === name);
    };

    let _get_field = function(i, data) {
        return data[i].ScalarValue;
    };

    const _measure_idx = _get_column_idx(columns, 'measure_name');
    const _time_idx = _get_column_idx(columns, 'time');
    const _double_idx = _get_column_idx(columns, 'measure_value::double');
    const _varchar_idx = _get_column_idx(columns, 'measure_value::varchar');

    let _get_value = function(data) {
        const value_double = _get_field(_double_idx, data);
        const value_varchar = _get_field(_varchar_idx, data);

        if (value_double) {
            return parseFloat(value_double);
        } else {
            return value_varchar;
        }
    };

    return {
        get_measure_name: function(data) { return _get_field(_measure_idx, data); },
        get_timestamp: function(data) { return new Date(_get_field(_time_idx, data)); },
        get_value: _get_value
    };
};

let _parse = function(mapper, data) {
    const measure_name = mapper.get_measure_name(data);
    const timestamp = mapper.get_timestamp(data);
    const value = mapper.get_value(data);

    // TODO: assumes that we don't have source data, so we are going to set
    // signalk-to-timestream as the source
    const update = {
        timestamp: timestamp.toISOString(),
        values: [{
            path: measure_name,
            value: value
        }]
    };

    return update;
};

// given: 
//  [
//      {
//          "timestamp":"2020-10-17T16:55:29.162Z",
//          "values":[{"path":"environment.rpi.cpu.utilisation","value":0.07}]
//      },
//      {
//          "timestamp":"2020-10-17T16:55:29.162Z",
//          "values":[{"path":"environment.rpi.cpu.temperature","value":329.53}]
//      },
//      ...
//  ]
//
// returns:
//
//  [
//      {
//          "timestamp":"2020-10-17T16:55:29.162Z",
//          "values":[
//              {"path":"environment.rpi.cpu.utilisation","value":0.07},
//              {"path":"environment.rpi.cpu.temperature","value":329.53},
//              ...
//          ]
//      }
//  ]
let _lift_values = function(entry) {
    const timestamp = entry[0];
    const update_list = entry[1];
    const values_nested_list = update_list.map(u => u.values);
    let   flattened_values = _.flatten(values_nested_list);

    // Ugh, find lat/long which are separate datapoints and recombine them,
    // since that's what signalk wants.
    const latitude  = flattened_values.find(v => v.path === 'navigation.position.latitude');
    const longitude = flattened_values.find(v => v.path === 'navigation.position.longitude');

    // if we found lat/long, remove the values and replace with a combined
    // element, yes this is inefficient, but these lists are small, so
    // whatever.
    if (!_.isUndefined(latitude) && !_.isUndefined(latitude.value)) {
        trace(`found lat/long, correcting object lat=${latitude.value} long=${longitude.value}`);

        // remove the lat/long values
        flattened_values = flattened_values.filter(v => v.path !== 'navigation.position.latitude');
        flattened_values = flattened_values.filter(v => v.path !== 'navigation.position.longitude');

        // add a combined value
        flattened_values.push({
            path: 'navigation.position',
            value: {
                latitude: latitude.value,
                longitude: longitude.value
            }
        });
    }

    return {
        source: {
            label: 'signalk-to-timestream'
        },
        timestamp: timestamp,
        values: flattened_values
    };
};

// TODO: assumes that only self was archived
module.exports = function(self_id, result) {
    const mapper = _build_mapper(result.ColumnInfo);
    const parsed_updates = result.Rows.map(row => _parse(mapper, row.Data));
    const updates_by_time = _.groupBy(parsed_updates, u => u.timestamp);
    const updates = _.map(Object.entries(updates_by_time), u => _lift_values(u));

    return {
        context: `vessels.${self_id}`,
        updates: updates
    };
};
