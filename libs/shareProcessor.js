const Redis = require('ioredis');
const Stratum = require('stratum-pool');

/*
This module deals with handling shares when in internal payment processing mode. It connects to a redis
database and inserts shares with the database structure of:

key: coin_name + ':' + block_height
value: a hash with..
        key:

 */

module.exports = function(log, poolConfig){
    const coin = poolConfig.coin.name;
    const rewardType = (poolConfig.type && ['pplns', 'solo'].includes(poolConfig.type))
        ? poolConfig.type
        : 'pplns';

    let pplns = 1;
    if (rewardType === 'pplns') {
        pplns = (poolConfig.pplns && poolConfig.pplns > 1) ? poolConfig.pplns : 10000;
    }

    const redisConfig = poolConfig.redis;
    const redisDB = (redisConfig.db && redisConfig.db > 0) ? redisConfig.db : 0;
    const baseName = redisConfig.baseName ? redisConfig.baseName : coin;
    const client = new Redis({
        port: redisConfig.port,
        host: redisConfig.host,
        db: redisDB,
        maxRetriesPerRequest: 1,
        readTimeout: 5
    })

    const forkId = process.env.forkId;
    const logSystem = 'Pool';
    const logComponent = coin;
    const logSubCat = 'Thread ' + (parseInt(forkId) + 1);

    client.on('ready', function(){
        log.debug(logSystem, logComponent, logSubCat, 'Share processing setup with redis (' + redisConfig.host +
            ':' + redisConfig.port  + ')');
    });
    client.on('error', function(err){
        log.error(logSystem, logComponent, logSubCat, 'Redis client had an error: ' + JSON.stringify(err))
    });
    client.on('end', function(){
        log.error(logSystem, logComponent, logSubCat, 'Connection to redis database has been ended');
    });

    client.info(function(error, response){
        if (error){
            log.error(logSystem, logComponent, logSubCat, 'Redis version check failed');
            return;
        }
        const parts = response.split('\r\n');
        let version;
        let versionString;
        for (let i = 0; i < parts.length; i++) {
            if (parts[i].indexOf(':') !== -1){
                const valParts = parts[i].split(':');
                if (valParts[0] === 'redis_version'){
                    versionString = valParts[1];
                    version = parseFloat(versionString);
                    break;
                }
            }
        }
        if (!version) {
            log.error(logSystem, logComponent, logSubCat, 'Could not detect redis version - but be super old or broken');
        } else if (version < 2.6) {
            log.error(logSystem, logComponent, logSubCat, "You're using redis version " + versionString + " the minimum required version is 2.6. Follow the damn usage instructions...");
        }
    });

    this.handleShare = function(isValidShare, isValidBlock, shareData){
        // shareData
        // {
        //     job: '5',
        //     ip: '::ffff:81.177.74.130',
        //     port: 3031,
        //     worker: 'GJK9gjntGMR3sQENuhNL99t6gkx2ct5xvb',
        //     height: 22012,
        //     blockReward: 300000000000,
        //     difficulty: 32,
        //     shareDiff: '1124.84785578',
        //     blockDiff: 125303.470650112,
        //     blockDiffActual: 489.466682227,
        //     blockHash: undefined,
        //     blockHashInvalid: undefined
        // }
        let redisCommands = [];

        if (isValidShare) {
            redisCommands.push(['hincrbyfloat', `${baseName}:stats`, 'roundShares', shareData.difficulty]);

            if (rewardType !== 'solo') {
                const baseShareDifficulty = 25000;
                const times = Math.floor(shareData.difficulty / baseShareDifficulty);

                for (let i = 0; i < times; ++i) {
                    redisCommands.push(['lpush', `${baseName}:lastShares`, shareData.worker]);
                }
                redisCommands.push(['ltrim', `${baseName}:lastShares`, 0, pplns - 1]);
            } else {
                redisCommands.push(['hincrbyfloat', `${baseName}:workers:${shareData.worker}`, 'soloShares', shareData.difficulty]);
            }

            redisCommands.push(['hincrbyfloat', baseName + ':shares:roundCurrent', shareData.worker, shareData.difficulty]);                //  we need it?
            redisCommands.push(['hincrby', baseName + ':stats', 'validShares', 1]);                                                         //  we need it?
        } else {
            redisCommands.push(['hincrby', baseName + ':stats', 'invalidShares', 1]);                                                       //  we need it?
        }
        /* Stores share diff, worker, and unique value with a score that is the timestamp. Unique value ensures it
           doesn't overwrite an existing entry, and timestamp as score lets us query shares from last X minutes to
           generate hashrate for each worker and pool. */
        const dateNow = Date.now();
        const hashrateData = [ isValidShare ? shareData.difficulty : -shareData.difficulty, shareData.worker, dateNow];
        redisCommands.push(['zadd', coin + ':hashrate', dateNow / 1000 | 0, hashrateData.join(':')]);

        if (isValidBlock) {
            redisCommands.push(['rename', coin + ':shares:roundCurrent', coin + ':shares:round' + shareData.height]);                       //  we need it?
            redisCommands.push(['sadd', coin + ':blocksPending', [shareData.blockHash, shareData.txHash, shareData.height].join(':')]);
            redisCommands.push(['hincrby', coin + ':stats', 'validBlocks', 1]);
        } else if (shareData.blockHash) {
            redisCommands.push(['hincrby', coin + ':stats', 'invalidBlocks', 1]);
        }

        //  CHECK!
        if (isValidBlock) {
            redisCommands.push(['hset', `${baseName}:stats`, `lastBlockFound`, Date.now()]);

            if (rewardType === 'pplns') {
                redisCommands.push(['lrange', `${baseName}:lastShares`, 0, pplns]);
                redisCommands.push(['hget', `${baseName}:stats`, 'roundShares']);
            } else if (rewardType === 'solo') {
                redisCommands.push(['hget', `${baseName}:workers:${shareData.worker}`, 'soloShares']);
            }
        }
        //  CHECK!
        // console.log('redisCommands', redisCommands)
        client.multi(redisCommands)
            .exec(function (err, replies) {
                if (err) {
                    log.error(logSystem, logComponent, logSubCat, `Error(1) failed to insert share data into redis:\n${JSON.stringify(redisCommands)}\n${JSON.stringify(err)}`);
                    return;
                }

                if (isValidBlock) {
                    let redisCommands2 = [];
                    const totalShares = replies[replies.length - 1];
                    // let totalScore = 0;
                    // let totalShares = 0;
                    if (rewardType === 'solo') {
                        redisCommands2.push(['hdel', `${coin}:workers:${shareData.worker}`, 'soloShares']);
                        redisCommands2.push(['hincrby', `${coin}:workers:${shareData.worker}`, 'soloBlocksFound', 1]);
                    } else if (rewardType === 'pplns') {
                        const pplnsShares = replies[replies.length - 2];
                        let totalSharesArr = [];

                        for (const miner of pplnsShares) {
                            if (!totalSharesArr[miner]) {
                                totalSharesArr[miner] = 1;
                            } else {
                                ++totalSharesArr[miner];
                            }
                        }

                        for (const miner in totalSharesArr) {
                            redisCommands2.push(['hincrby', `${coin}:shares:round${shareData.height}`, miner, totalSharesArr[miner]]);
                        }

                    }

                    redisCommands2.push(['zadd', `${baseName}:blocks:candidates`, shareData.height,
                        [
                            rewardType,
                            shareData.worker,
                            shareData.blockHash,
                            shareData.txHash,
                            Date.now() / 1000 | 0,
                            shareData.blockDiff,
                            totalShares
                        ].join(':')]
                    );

                    client.multi(redisCommands2)
                        .exec(function (err, replies) {
                            if (err) {
                                log.error(logSystem, logComponent, logSubCat, `Error(2) failed to insert share data into redis:\n${JSON.stringify(redisCommands)}\n${JSON.stringify(err)}`);
                                // return;
                            }
                        })
                }
            });
        //  double check end

        // client.multi(redisCommands).exec(function(err, replies){
        //     if (err)
        //         log.error(logSystem, logComponent, logSubCat, 'Error with share processor multi ' + JSON.stringify(err));
        // })

    };
};
