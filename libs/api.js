const Redis = require('ioredis');
const async = require('async')

const stats = require('./stats.js')

module.exports = function(logger, portalConfig, poolConfigs){


    const _this = this

    const portalStats = this.stats = new stats(logger, portalConfig, poolConfigs)

    this.liveStatConnections = {};

    this.handleApiRequest = function(req, res, next){
        switch(req.params.method){
            case 'stats':
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(portalStats.statsString);
                return;
            case 'pool_stats':
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(portalStats.statPoolHistory));
                return;
            case 'live_stats':
                res.writeHead(200, {
                    'Content-Type': 'text/event-stream',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive'
                });
                res.write('\n');
                const uid = Math.random().toString()
                _this.liveStatConnections[uid] = res;
                req.on("close", function() {
                    delete _this.liveStatConnections[uid];
                });

                return;
            default:
                next();
        }
    };


    this.handleAdminApiRequest = function(req, res, next){
        switch(req.params.method){
            case 'pools': {
                res.end(JSON.stringify({result: poolConfigs}));
                return;
            }
            default:
                next();
        }
    };

};
