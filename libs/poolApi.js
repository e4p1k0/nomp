const fs = require('fs');
const async = require('async');
const Redis = require('ioredis');
const algos = require('stratum-pool/lib/algoProperties')

module.exports = function(logger){
    const poolConfigs = JSON.parse(process.env.pools);
    let enabledPools = [];

    Object.keys(poolConfigs).forEach(function(coin) {
        const poolOptions = poolConfigs[coin];


        enabledPools.push(coin);

        //  check poolapi enabled LATER
        // if (poolOptions.paymentProcessing &&
        //     poolOptions.paymentProcessing.enabled)
        //     enabledPools.push(coin);
    });

    async.filter(enabledPools, function(coin, callback){
        SetupForPool(logger, poolConfigs[coin], function(setupResults){
            callback(setupResults);
        });
    }, function(coins, error){
        if (error) {
            console.log('error', error)
        }

        //  in old async ver
        // console.log('coins', coins)
        // coins [ 'goodmorning' ]
        coins.forEach(function(coin){
            const poolOptions = poolConfigs[coin];
            const processingConfig = poolOptions.paymentProcessing;
            const logSystem = 'API';
            logger.debug(logSystem, coin, 'API processing setup to run every '
                + processingConfig.paymentInterval + ' second(s) with daemon ('
                + processingConfig.daemon.user + '@' + processingConfig.daemon.host + ':' + processingConfig.daemon.port
                + ') and redis (' + poolOptions.redis.host + ':' + poolOptions.redis.port + ')');

        });
    });
};

function SetupForPool(logger, poolOptions, setupFinished){
    const coin = poolOptions.coin.name;
    const processingConfig = poolOptions.paymentProcessing;
    const logSystem = 'API';
    const logComponent = coin;

    const algo = poolOptions.coin.algorithm;
    const shareMultiplier = Math.pow(2, 32) / algos[algo].multiplier;
// console.log('kiska!')
//     console.log('shareMultiplier', shareMultiplier)
    // let algorithm: poolConfigs[coinName].coin.algorithm,

    // console.log(shareMultiplier)
    // console.log(algos)
    const redisConfig = poolOptions.redis;
    const redisClient = new Redis({
        port: redisConfig.port,
        host: redisConfig.host,
        db: redisConfig.db,
        maxRetriesPerRequest: 1,
        readTimeout: 5
    })

    processingConfig.apiInterval = 120;   // Setup LATER
    setInterval(function(){
        try {
            ProcessApi();
        } catch(e){
            throw e;
        }
    }, processingConfig.apiInterval * 1000);

    function ProcessApi() {
        const startApiProcess = Date.now();

        let timeSpentRPC = 0;
        let timeSpentRedis = 0;

        const now = Date.now();
        const nowMs = Math.round(now / 1000);
        const hrWindow = 1800;                          // Setup LATER
        const largeHrWindow = 10800;                    // Setup LATER

        const startRedisTimer = function(){ startTimeRedis = Date.now() };
        const endRedisTimer = function(){ timeSpentRedis += Date.now() - startTimeRedis };

        async.waterfall([
            function(callback){
                startRedisTimer();
                redisClient.multi([
                    ['zremrangebyscore', coin + ':hashrate', '-inf', `(${nowMs - largeHrWindow}`],
                    ['zrangebyscore', coin + ':hashrate', 0, '+inf']
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
                    ['zadd', coin + ':charts:pool', nowMs, [totalHashrate, totalHashrateAvg].join(':')]
                ];

                for (let miner in miners) {
                    redisCommands.push(['zadd', coin + ':charts:miners:' + miner, nowMs,
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
                logger.debug(logSystem, logComponent, 'Finished interval - time spent: '
                    + apiProcessTime + 'ms total, ' + timeSpentRedis + 'ms redis, '
                    + timeSpentRPC + 'ms daemon RPC');
            }
        )

    }

    //  if error
    if (false) {
        setupFinished(false);
    }
    setupFinished(true);
}
