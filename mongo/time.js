//Should be called like this
// mongo --eval "var input = 'tweets',output = 'time', finestZoom = 19, minZoom = 5, maxZoom = 19, start='Thu Jul 07 22:30:00 +0000 2011', end='Thu Jul 07 22:45:00 +0000 2011'" geo /nfs/src/time.js


if(!input || !output || !finestZoom || !minZoom || !maxZoom || !start || !end) {
    throw "You didn't define the arguments";
}
if (minZoom > maxZoom) {
    var temp = minZoom; minZoom = maxZoom; maxZoom = temp;
}
if (finestZoom < maxZoom) {
    throw "finestZoom must be >= maxZoom";
}

var map = function() {
    function getDataZoom(zoom) { return Math.min(zoom + 4, finestZoom); }
    function pad(n) {return n < 10 ? '0' + n : n} 
    function getDate(d) { 
        d = new Date(d); 
        function getQuarterHours(mins){
            var whichQuarterHour;
            if(mins < 15){
                return 1;
            }
            else if(mins >= 15 && mins < 30){
                return 2;
            }
            else if (mins >= 30 && mins < 45){
                return 3;
            }
            else{
                return 4;
            }
        }
        
        return [pad(d.getUTCMonth()+1), pad(d.getUTCDate()), pad(d.getUTCHours()), pad(getQuarterHours(d.getUTCMinutes()))].join("-"); } 
    function latLongToGoogleTile(a, f, b) {
        var g = Math.PI * 12756274 / 256, c = Math.PI * 6378137;
        a = [f * c / 180, Math.log(Math.tan((90 + a) * Math.PI / 360)) / (Math.PI / 180) * (c / 180)];
        a = (function (a, b, d) {d = g / Math.pow(2, d);a = [(a + c) / d, (b + c) / d];return [Math.ceil(a[0] / 256) - 1, Math.ceil(a[1] / 256) - 1];})(a[0], a[1], b);
        return [a[0], Math.pow(2, b) - 1 - a[1]];
    }
    function getCoordinates() { 
        if (this.geo && this.geo.coordinates) {
            return this.geo.coordinates;
        }
        else if (this && this.place && this.place.bounding_box && this.place.bounding_box.coordinates) {
            var c = this.place.bounding_box.coordinates[0];
            var x = Geo.distance(c[0], c[1]);
            var y = Geo.distance(c[2], c[1])
            if (x * y < .000055) {
                // Find the middle of bounding box.
                return [
                    Math.abs((c[0][1] + c[1][1] + c[2][1] + c[3][1]) / 4), 
                    Math.abs((c[0][0] + c[1][0] + c[2][0] + c[3][0]) / 4),
                ];
            }
        }
        return false;
    }
    var tiles = [];

    var c = getCoordinates.apply(this);
    if (c===false) {
        return;
    }
    // Precompute all the tile values to avoid redundant computation
    for (var zoom = getDataZoom(maxZoom); zoom >= minZoom; --zoom) {
        tiles[zoom] = latLongToGoogleTile(c[0], c[1], zoom);
    }
    for (var zoom = getDataZoom(maxZoom); zoom >= minZoom; --zoom) {
        var dataZoom = getDataZoom(zoom);
        var tile = tiles[zoom];
        var dataTile = tiles[dataZoom];
        var dataTileRowSize = Math.pow(2, dataZoom - zoom);
        var subIndex = (dataTile[0] % dataTileRowSize) + dataTileRowSize * (dataTile[1] % dataTileRowSize);
        var baseKey = "" + zoom + "/" + tile.join(",") + "/";
        counts = {};
        counts[subIndex] = 1;
        var emittedValue = {
            total: 1,
            counts: counts,
            res: (dataTileRowSize * dataTileRowSize),
            zoom: zoom,
        };

        var key = baseKey + getDate(this.created_at);
        emit(key, emittedValue);
    }
};

var reduce = function(key, values) {
    if (values.length == 1) {
        return values[0];
    }
    var result = {
        total: 0,
        counts: {},
        res: values[0].res,
        zoom: values[0].zoom,
    };
    for (var i = 0; i < values.length; ++i) {
        var value = values[i];
        result.total += value.total;
        for (var j in value.counts) {
            result.counts[j] = (result.counts[j] || 0) + value.counts[j];
        }
    }
    return result;
};

if (db[input].count() == 0) {
    print("Input collection (" + db[input] + ") is empty.");
}
else {
    var query = {created_at:{$gt : start, $lt : end}};
    var startTime = new Date();
    print("Input collection (" + db[input] + ") pre-count: " + db[input].find(query).count());
    print("Output collection (" + db[output] + ") pre-count: " + db[output].count());
    db[input].mapReduce(map, reduce, {
        out: output, 
        scope: { 
            minZoom: minZoom,
            maxZoom: maxZoom,
            finestZoom: finestZoom,
        },
        query: query
    });
    print("Run time: " + ((new Date() - startTime)/1000) + " seconds");
    print("Output collection (" + output + ") post-count: " + db[output].count());
    print("Input collection (" + input + ") post-count: " + db[input].find(query).count());
}
