const async = require('async');
const Redis = require('ioredis');
const algos = require('stratum-pool/lib/algoProperties')
const Stratum = require('stratum-pool')

module.exports = function(logger){
    const poolConfigs = JSON.parse(process.env.pools);
    let enabledPools = [];

    Object.keys(poolConfigs).forEach(function(configName) {
        const poolOptions = poolConfigs[configName];
        if (poolOptions.poolApi?.enabled) enabledPools.push(configName);
    });

    async.filter(enabledPools, function(coin, callback){
        SetupRPCAPIForPool(logger, poolConfigs[coin], function(setupResults){
            callback(setupResults);
        });
    }, function(coins, error){
        if (error) {
            console.log('error', error)
        }

        coins.forEach(function(coin){
            const poolOptions = poolConfigs[coin];
            const daemonCfg = poolOptions.daemons[0];
            const logSystem = 'API';

            logger.debug(logSystem, coin, `RPC API setup to run every ${poolOptions.poolApi.rpcInterval} sec`
                + ` with daemon (${daemonCfg.user}@${daemonCfg.host}:${daemonCfg.port})`
                + ` and redis (${poolOptions.redis.host}:${poolOptions.redis.port})`);

        });
    });

    async.filter(enabledPools, function(coin, callback){
        SetupChartsCollectingForPool(logger, poolConfigs[coin], function(setupResults){
            callback(setupResults);
        });
    }, function(coins, error){
        if (error) {
            console.log('error', error)
        }

        coins.forEach(function(coin){
            const poolOptions = poolConfigs[coin];
            const logSystem = 'API';

            logger.debug(logSystem, coin, `Charts collecting setup to run every ${poolOptions.poolApi.graphInterval} sec`
                + ` with redis (${poolOptions.redis.host}:${poolOptions.redis.port})`);
        });
    });
};

function SetupRPCAPIForPool(logger, poolOptions, setupFinished){
    const cfg = poolOptions.poolApi;
    const logSystem = 'API';
    const logComponent = poolOptions.name;

    const daemon = new Stratum.daemon.interface([poolOptions.daemons[0]], function(severity, message){
        logger[severity](logSystem, logComponent, message);
    });

    const redisConfig = poolOptions.redis;
    const baseName = redisConfig.baseName;

    const redisClient = new Redis({
        port: redisConfig.port,
        host: redisConfig.host,
        db: redisConfig.db,
        maxRetriesPerRequest: 1,
    });

    setInterval(function(){
        try {
            ProcessRPCApi();
        } catch(e){
            throw e;
        }
    }, cfg.rpcInterval * 1000);

    function ProcessRPCApi() {
        const startApiProcess = Date.now();
        let startTimeRedis;
        let startTimeRPC;
        let timeSpentRPC = 0;
        let timeSpentRedis = 0;

        const startRedisTimer = function(){ startTimeRedis = Date.now() };
        const endRedisTimer = function(){ timeSpentRedis += Date.now() - startTimeRedis };

        const startRPCTimer = function(){ startTimeRPC = Date.now(); };
        const endRPCTimer = function(){ timeSpentRPC += Date.now() - startTimeRPC };

        let batchRpcCalls = [
            ['getmininginfo', []]
        ];
        // if (poolOptions.coin.hasGetInfo) {
        //     batchRpcCalls.push(['getinfo', []]);
        // } else {
        //     batchRpcCalls.push(['getblockchaininfo', []], ['getnetworkinfo', []]);
        // }

        async.waterfall([
                function(callback){
                    startRPCTimer();
                    daemon.batchCmd(batchRpcCalls, function(error, results){
                        endRPCTimer();
                        // [
                        //     {
                        //         result: {
                        //             blocks: 26175,
                        //             currentblocksize: 3291,
                        //             currentblocktx: 1,
                        //             difficulty: 287.1243004569492,
                        //             errors: '',
                        //             genproclimit: 1,
                        //             networkhashps: 24835664468.55431,
                        //             pooledtx: 1,
                        //             testnet: false,
                        //             chain: 'main',
                        //             generate: false
                        //         },
                        //         error: null,
                        //         id: 1698935008557
                        //     },
                        //
                        // ]

                        if (error && !results[0] && results[0].error){
                            callback('RPC error loop ended - error');
                            return;
                        }

                        const result = results[0].result;

                        const height = result.blocks
                        const rawDiff = result.difficulty
                        const difficulty = rawDiff * algos[poolOptions.coin.algorithm].multiplier
                        callback(null, height, difficulty);
                    });
                },
                function(height, difficulty, callback){
                    let redisCommands = [
                        ['hset', `${baseName}:stats`, 'height', height],
                        ['hset', `${baseName}:stats`, 'difficulty', difficulty],
                    ];

                    startRedisTimer();
                    redisClient.multi(redisCommands).exec(function(error, results){
                        endRedisTimer();
                        if (error) {
                            callback('API loop ended - redis error with multi write charts data');
                            return;
                        }

                        callback();
                    });
                }],
            function () {
                const apiProcessTime = Date.now() - startApiProcess;
                logger.debug(logSystem, logComponent, `RPC data collected - ${apiProcessTime} ms total: `
                    + `${timeSpentRedis} ms redis, ${timeSpentRPC} ms RPC daemon`);
            }
        )

    }

    setupFinished(true);
}

function SetupChartsCollectingForPool(logger, poolOptions, setupFinished){
    const cfg = poolOptions.poolApi;
    const logSystem = 'API';

    const logComponent = poolOptions.name;

    const algo = poolOptions.coin.algorithm;
    const shareMultiplier = algos[algo].multiplier ? Math.pow(2, 32) / algos[algo].multiplier : 1;
    const redisConfig = poolOptions.redis;
    const baseName = redisConfig.baseName;

    const redisClient = new Redis({
        port: redisConfig.port,
        host: redisConfig.host,
        db: redisConfig.db,
        maxRetriesPerRequest: 1,
    })

    setInterval(function(){
        try {
            ProcessApi();
        } catch(e){
            throw e;
        }
    }, cfg.graphInterval * 1000);

    function ProcessApi() {
        const startApiProcess = Date.now();
        let startTimeRedis;

        let timeSpentRedis = 0;

        const now = Date.now();
        const nowMs = Math.round(now / 1000);
        const hrWindow = cfg.hrWindow;
        const largeHrWindow = cfg.largeHrWindow;

        const startRedisTimer = function(){ startTimeRedis = Date.now() };
        const endRedisTimer = function(){ timeSpentRedis += Date.now() - startTimeRedis };

        async.waterfall([
            function(callback){
                startRedisTimer();
                redisClient.multi([
                    ['zremrangebyscore', `${baseName}:hashrate`, '-inf', `(${nowMs - largeHrWindow}`],
                    ['zrangebyscore', `${baseName}:hashrate`, 0, '+inf']
                ]).exec(function(error, results){
                    if (error) {
                        callback('API loop ended - redis error with multi get hashrate data');
                        return;
                    }

                    endRedisTimer();
                    callback(null, results[1][1]);
                });
            },

            //  calculate hashrate
            function(shares, callback){
                let totalHashrate = 0;
                let totalHashrateAvg = 0;
                let miners = {};

                for (const shareString of shares) {
                    // 8
                    // :GJK9gjntGMR3sQENuhNL99t6gkx2ct5xvb
                    // :3070tilaptop
                    // :1698929185794
                    const data = shareString.split(':');
                    const share = +data[0];
                    const login = data[1];
                    const timestamp = Math.round(+data[3]/1000);

                    if (!miners[login]) {
                        miners[login] = {
                            hashrate: 0,
                            hashrateAvg: 0
                        }
                    }

                    miners[login].hashrateAvg += share;
                    if (timestamp > nowMs - hrWindow) {
                        miners[login].hashrate += share;
                    }
                }

                let totalShares = 0;
                for (let login in miners) {
                    totalShares += miners[login].hashrate;
                    miners[login].hashrate *= shareMultiplier/hrWindow
                    miners[login].hashrateAvg *= shareMultiplier/largeHrWindow

                    totalHashrate += miners[login].hashrate
                    totalHashrateAvg += miners[login].hashrateAvg
                }

                callback(null, totalHashrate, totalHashrateAvg, miners)
            },

            function(totalHashrate, totalHashrateAvg, miners, callback){
                let redisCommands = [
                    ['zadd', `${baseName}:charts:pool`, nowMs, [totalHashrate, totalHashrateAvg].join(':')]
                ];

                for (let miner in miners) {
                    redisCommands.push(['zadd', `${baseName}:charts:miners:` + miner, nowMs,
                        [miners[miner].hashrate, miners[miner].hashrateAvg].join(':')]);
                }

                startRedisTimer();
                redisClient.multi(redisCommands).exec(function(error, results){
                    endRedisTimer();
                    if (error) {
                        callback('API loop ended - redis error with multi write charts data');
                        return;
                    }

                    callback();
                });
            }],
            function () {
                const apiProcessTime = Date.now() - startApiProcess;
                logger.debug(logSystem, logComponent, `Charts data collected - ${apiProcessTime} ms total: `
                    + `${timeSpentRedis} ms redis`);
            }
        )

    }

    //  if error

    setupFinished(true);
}
