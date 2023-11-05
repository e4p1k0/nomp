const Redis = require('ioredis');
const async = require('async');
const algos = require('stratum-pool/lib/algoProperties.js');

const zlib = require('zlib');
const os = require('os');

module.exports = function(logger, portalConfig, poolConfigs){
    const _this = this;
    const logSystem = 'Stats';

    let redisClients = [];
    let redisStats;

    this.statHistory = [];
    this.statPoolHistory = [];

    this.stats = {};
    this.statsString = '';

    setupStatsRedis();
    gatherStatHistory();

    const canDoStats = true

    Object.keys(poolConfigs).forEach(function(coin){

        if (!canDoStats) return;

        const poolConfig = poolConfigs[coin]

        const redisConfig = poolConfig.redis;
        const redisDB = (redisConfig.db && redisConfig.db > 0) ? redisConfig.db : 0;

        for (let i = 0; i < redisClients.length; i++){
            const client = redisClients[i]
            if (client.client.port === redisConfig.port && client.client.host === redisConfig.host){
                client.coins.push(coin);
                return;
            }
        }

        redisClients.push({
            coins: [coin],
            client: Redis.createClient(redisConfig.port, redisConfig.host, {
                db: redisDB,
                auth_pass: redisConfig.auth
            })
        });
    });


    function setupStatsRedis(){
        redisStats = Redis.createClient(portalConfig.redis.port, portalConfig.redis.host);
        redisStats.on('error', function(err){
            logger.error(logSystem, 'Historics', 'Redis for stats had an error ' + JSON.stringify(err));
        });
    }

    function gatherStatHistory(){
        const retentionTime = (((Date.now() / 1000) - portalConfig.website.stats.historicalRetention) | 0).toString();

        redisStats.zrangebyscore(['statHistory', retentionTime, '+inf'], function(err, replies){
            if (err) {
                logger.error(logSystem, 'Historics', 'Error when trying to grab historical stats ' + JSON.stringify(err));
                return;
            }

            for (let i = 0; i < replies.length; i++){
                _this.statHistory.push(JSON.parse(replies[i]));
            }

            _this.statHistory = _this.statHistory.sort(function(a, b){
                return a.time - b.time;
            });

            _this.statHistory.forEach(function(stats){
                addStatPoolHistory(stats);
            });
        });
    }

    function addStatPoolHistory(stats){
        const data = {
            time: stats.time,
            pools: {}
        };
        for (const pool in stats.pools){
            data.pools[pool] = {
                hashrate: stats.pools[pool].hashrate,
                workerCount: stats.pools[pool].workerCount,
                blocks: stats.pools[pool].blocks
            }
        }
        _this.statPoolHistory.push(data);
    }




    this.getGlobalStats = function(callback){
        const statGatherTime = Date.now() / 1000 | 0;
        let allCoinStats = {};

        async.each(redisClients, function(client, callback){
            const windowTime = (((Date.now() / 1000) - portalConfig.website.stats.hashrateWindow) | 0).toString();
            let redisCommands = [];
            let redisCommandTemplates = [
                ['zremrangebyscore', ':hashrate', '-inf', '(' + windowTime],
                ['zrangebyscore', ':hashrate', windowTime, '+inf'],
                ['hgetall', ':stats'],
                ['scard', ':blocksPending'],
                ['scard', ':blocksConfirmed'],
                ['scard', ':blocksKicked']
            ];

            const commandsPerCoin = redisCommandTemplates.length;

            client.coins.map(function(coin){
                redisCommandTemplates.map(function(t){
                    const clonedTemplates = t.slice(0);
                    clonedTemplates[1] = coin + clonedTemplates[1];
                    redisCommands.push(clonedTemplates);
                });
            });


            client.client.multi(redisCommands).exec(function(err, replies){
                if (err){
                    logger.error(logSystem, 'Global', 'error with getting global stats ' + JSON.stringify(err));
                    callback(err);
                } else {
                    for (let i = 0; i < replies.length; i += commandsPerCoin){
                        const coinName = client.coins[i / commandsPerCoin | 0];
                        const coinStats = {
                            name: coinName,
                            symbol: poolConfigs[coinName].coin.symbol.toUpperCase(),
                            algorithm: poolConfigs[coinName].coin.algorithm,
                            hashrates: replies[i + 1][1],
                            poolStats: {
                                validShares: replies[i + 2][1] ? (replies[i + 2][1].validShares || 0) : 0,
                                validBlocks: replies[i + 2][1] ? (replies[i + 2][1].validBlocks || 0) : 0,
                                invalidShares: replies[i + 2][1] ? (replies[i + 2][1].invalidShares || 0) : 0,
                                totalPaid: replies[i + 2][1] ? (replies[i + 2][1].totalPaid || 0) : 0
                            },
                            blocks: {
                                pending: replies[i + 3][1],
                                confirmed: replies[i + 4][1],
                                orphaned: replies[i + 5][1]
                            }
                        };
                        allCoinStats[coinStats.name] = (coinStats);
                    }
                    callback();
                }
            });
        }, function(err){
            if (err){
                logger.error(logSystem, 'Global', 'error getting all stats' + JSON.stringify(err));
                callback();
                return;
            }

            let portalStats = {
                time: statGatherTime,
                global:{
                    workers: 0,
                    hashrate: 0
                },
                algos: {},
                pools: allCoinStats
            };

            Object.keys(allCoinStats).forEach(function(coin){
                let coinStats = allCoinStats[coin];
                coinStats.workers = {};
                coinStats.shares = 0;
                coinStats.hashrates.forEach(function(ins){
                    const parts = ins.split(':');
                    const workerShares = parseFloat(parts[0]);
                    const worker = parts[1];
                    if (workerShares > 0) {
                        coinStats.shares += workerShares;
                        if (worker in coinStats.workers){
                            coinStats.workers[worker].shares += workerShares;
                        } else {
                            coinStats.workers[worker] = {
                                shares: workerShares,
                                invalidshares: 0,
                                hashrateString: null
                            };
                        }
                    } else {
                        if (worker in coinStats.workers) {
                            coinStats.workers[worker].invalidshares -= workerShares; // workerShares is negative number!

                        } else {
                            coinStats.workers[worker] = {
                                shares: 0,
                                invalidshares: -workerShares,
                                hashrateString: null
                            };
                        }
                    }
                });

                const shareMultiplier = Math.pow(2, 32) / algos[coinStats.algorithm].multiplier;
                coinStats.hashrate = shareMultiplier * coinStats.shares / portalConfig.website.stats.hashrateWindow;

                coinStats.workerCount = Object.keys(coinStats.workers).length;
                portalStats.global.workers += coinStats.workerCount;

                /* algorithm specific global stats */
                const algo = coinStats.algorithm;
                if (!portalStats.algos.hasOwnProperty(algo)){
                    portalStats.algos[algo] = {
                        workers: 0,
                        hashrate: 0,
                        hashrateString: null
                    };
                }
                portalStats.algos[algo].hashrate += coinStats.hashrate;
                portalStats.algos[algo].workers += Object.keys(coinStats.workers).length;

                for (const worker in coinStats.workers) {
                    coinStats.workers[worker].hashrateString = _this.getReadableHashRateString(shareMultiplier * coinStats.workers[worker].shares / portalConfig.website.stats.hashrateWindow);
                }

                delete coinStats.hashrates;
                delete coinStats.shares;
                coinStats.hashrateString = _this.getReadableHashRateString(coinStats.hashrate);
            });

            Object.keys(portalStats.algos).forEach(function(algo){
                let algoStats = portalStats.algos[algo];
                algoStats.hashrateString = _this.getReadableHashRateString(algoStats.hashrate);
            });

            _this.stats = portalStats;
            _this.statsString = JSON.stringify(portalStats);

            _this.statHistory.push(portalStats);
            addStatPoolHistory(portalStats);

            const retentionTime = (((Date.now() / 1000) - portalConfig.website.stats.historicalRetention) | 0);

            for (let i = 0; i < _this.statHistory.length; i++){
                if (retentionTime < _this.statHistory[i].time){
                    if (i > 0) {
                        _this.statHistory = _this.statHistory.slice(i);
                        _this.statPoolHistory = _this.statPoolHistory.slice(i);
                    }
                    break;
                }
            }

            redisStats.multi([
                ['zadd', 'statHistory', statGatherTime, _this.statsString],
                ['zremrangebyscore', 'statHistory', '-inf', '(' + retentionTime]
            ]).exec(function(err, replies){
                if (err)
                    logger.error(logSystem, 'Historics', 'Error adding stats to historics ' + JSON.stringify(err));
            });
            callback();
        });

    };

    this.getReadableHashRateString = function(hashrate){
        let i = -1;
        const byteUnits = [ ' KH', ' MH', ' GH', ' TH', ' PH' ];
        do {
            hashrate = hashrate / 1000;
			i++;
        } while (hashrate > 1000);
        return hashrate.toFixed(2) + byteUnits[i];
    };
};
