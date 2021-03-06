cubism_contextPrototype.opentsdb = function (address) {
    if (!arguments.length) {
        address = "";
    }
    var source = {},
        context = this;

    source.metrics = function (start, end, metric, rateString, tagMap, name) {
        // if tag map identifies a singular line (ie no multi-values) then we can return a metric for the expression
        // if otherwise, we need to find out what lines there are and return a metric for each, which is fine for static
        //    graphs, but not for streaming, where additional lines might appear later, for now request that the caller specify
        //    a time range to query over, they can always re-query?

        var singular = true;
        for (var tagk in tagMap) {
            if (tagMap.hasOwnProperty(tagk)) {
                if (tagMap[tagk] == '*' || tagMap[tagk].contains('|')) {
                    singular = false;
                    break;
                }
            }
        }

        if (singular) {
            return source.metric(metric, rate, tagMap, name);
        }

        // todo: at this point we use start and end to find the time series for this query
        throw "not worked this out yet..";

    };

    source.metric = function (metric, rateString, tagMap, name) {
        var aggregator = "sum";
        var downsampler = "avg";
        var interpolate = false;
        var squashNegatives = false;

        var ret = context.metric(function (start, stop, step, callback) {
            // m=<aggregator>:[rate[{counter[,<counter_max>[,<reset_value>]]]}:][<down_sampler>:]<metric_name>[{<tag_name1>=<grouping filter>[,...<tag_nameN>=<grouping_filter>]}][{<tag_name1>=<non grouping filter>[,...<tag_nameN>=<non_grouping_filter>]}]
            var target = aggregator + (rateString ? ":" + rateString : "");

//            if (step !== 1e4) {
                target += ":" + (!(step % 36e5) ? step / 36e5 + "h" : !(step % 6e4) ? step / 6e4 + "m" : step / 1e3 + "s") + "-" + downsampler;
//            }

            target += ":" + metric;
            if (tagMap) {
                target += "{";
                var sep = "";
                for (var tagk in tagMap) {
                    if (tagMap.hasOwnProperty(tagk)) {
                        target += sep + tagk + "=" + tagMap[tagk];
                        sep = ",";
                    }
                }
                target += "}";
            }

            var url = address + "/api/query"
                + "?m=" + encodeURIComponent(target)
                + "&start=" + cubism_opentsdbFormatDate(start)
                + "&end=" + cubism_opentsdbFormatDate(stop)
                + "&ms=true"
                + "&global_annotations=true"
                + "&arrays=true";

            d3.json(url, function (json) {
                if (!json) {
                    return callback(new Error("unable to load data"));
                }
                var parsed = cubism_opentsdbParse(json, start, step, interpolate, squashNegatives); // array response
                callback(null, parsed[0]);
            });
        }, name);

        ret.aggregate = function (_) {
            aggregator = _;
            return ret;
        };

        ret.downsample = function (_) {
            downsampler = _;
            return ret;
        };

        ret.interpolate = function (_) {
            interpolate = _;
            return ret;
        }

        ret.squashNegatives = function(_) {
            squashNegatives = _;
            return ret;
        }

        return ret;
    }

    source.find = function (pattern, callback) {
        d3.json(address + "/api/suggest?type=metrics&max=1000"
            + "&q=" + encodeURIComponent(pattern), function (result) {
            if (!result) {
                return callback(new Error("unable to find metrics"));
            }
            callback(null, result.metrics.map(function (d) {
                return d;
            }));
        });
    };

    // Returns the tsdb address.
    source.toString = function () {
        return host;
    };

    return source;
};

// Opentsdb understands seconds since UNIX epoch.
function cubism_opentsdbFormatDate(time) {
    return Math.floor(time / 1000);
}

// Helper method for parsing opentsdb's json response
function cubism_opentsdbParse(json, start, step, interpolate, squashNegatives) {
    // no data
    if (json.length == 0) {
        return [[]];
    }
    return json.map(function (ts) {
        if (ts.dps.length == 0) {
            return [];
        }
        if (interpolate) {
            var firstTime = ts.dps[0][0];
            var ret = [];
            for (var v=start; v<firstTime; v+=step) {
                ret.push(null);
            }
            var lastValue;
            var lastValueTime;
            var nextIndex = 0;
            for (var v=firstTime; nextIndex<ts.dps.length; v+=step) {
                if (ts.dps[nextIndex][0] == v) {
                    lastValue = ts.dps[nextIndex][1];
                    lastValueTime = ts.dps[nextIndex][0];
                    ret.push(squashNegatives && lastValue < 0 ? 0 : lastValue);
                    nextIndex++;
                    if (nextIndex>=ts.dps.length) {
                        break;
                    }
                }
                else {
                    // interpolate
                    var nextValue = ts.dps[nextIndex][1];
                    var nextTime = ts.dps[nextIndex][0];
                    var timeDiffLastToNext = nextTime - lastValueTime;
                    var timeDiffLastToNow = v - lastValueTime;
                    var value = nextValue + ((nextValue - lastValue) * (timeDiffLastToNow / timeDiffLastToNext));
                    ret.push(squashNegatives && value < 0 ? 0 : value)
                }
            }
            return ret;
        }
        else {
            if (squashNegatives) {
                return ts.dps.map(function (array) {
                    return array[1] < 0 ? 0 : array[1];
                });
            }
            else {
                return ts.dps.map(function (array) {
                    return array[1];
                });
            }
        }
    });
}
