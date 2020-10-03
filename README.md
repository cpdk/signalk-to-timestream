# SignalK to Amazon Timestream Plugin

SignalK plugin to publish to [Amazon Timestream](https://aws.amazon.com/timestream/),
a serverless time series database.

Timestream integrates with all the rest of AWS, as well as
[Grafana](http://grafana.org) with the [Grafana Timestream
Plugin](https://grafana.com/grafana/plugins/grafana-timestream-datasource).

# Setup

This plugin assumes you have a Timestream database and table pre-created.  If
you're running in AWS you can use a role.  Otherwise you'll need to create a
user with an access key and secret key.  The role/user needs permission to
`timestream:DescribeEndpoints` and `timestream:WriteRecords`, but that's it.

Note that if you're using an AWS config file, since this runs in node.js,
you'll need to set the environment variable `AWS_SDK_CONFIG_FILE`.

# Configuration

At the moment the plugin is hardcoded to only write `self`.  The configuration
consists of the following parameters

- __Database__: this is the name of your database, not the ARN

- __Table__: this is the name of the table, not the ARN

- __Write Interval__: a write to timestream will happen on this frequency, in
  seconds.  Note that there is a cost to the size of the writes, as well as the
  volume of data stores and this is the biggest lever to control costs.

- __Filter List__: this controls what signalk paths are published, the list
  either contains glob patterns describing the paths that should be included or
  excluded from publishing, for example, you might publish `"environment.*"`.

# Historical Data

At the moment, this plugin does not deal with historical data.  There are two
reasons for this.

1. I just haven't done it yet.

2. Timestream has a limited ingest window.  Timestream will only write to its
   memory store and once data has aged out of the memory store, the time period
   is frozen and writes will be rejected.  In practice, I assume most people
   will set the memory store to <= 24 hours (I have).  This means that history
   will be valuable for back filling during a connectivity gap, but will
   probably be unusable for back filling an entire trip.
