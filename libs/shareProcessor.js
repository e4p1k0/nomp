const Redis = require('ioredis');

//  This module deals with handling shares. It connects to a redis and inserts shares
module.exports = function(log, poolConfig){
    const name = poolConfig.name;
    const forkId = process.env.forkId;
    const logSystem = 'Pool';
    const logComponent = name;
    const logSubCat = 'Thread ' + (parseInt(forkId) + 1);

    const rewardType = poolConfig.type;
    const pplns = rewardType === 'pplns' ? poolConfig.pplns : 1;

    const defaultBaseShareDifficulty = 2;
    let baseShareDifficulty;
    let varDiffCfgFound = false;
    for (const portCfg of Object.values(poolConfig.ports)) {
        const portMinDiff = portCfg?.varDiff?.minDiff;
        if (portMinDiff) {
            varDiffCfgFound = true;
            if (!baseShareDifficulty || portMinDiff < baseShareDifficulty) {
                baseShareDifficulty = portMinDiff;
            }
        }
    }
    if (rewardType === 'pplns') {
        if (!varDiffCfgFound) {
            baseShareDifficulty = defaultBaseShareDifficulty;
            log.debug(logSystem, logComponent, logSubCat, `Vardiff cfg not found, using default baseShareDifficulty`);
        }
        log.debug(logSystem, logComponent, logSubCat, `Using baseShareDifficulty ${baseShareDifficulty}`);
    }

    const redisConfig = poolConfig.redis;
    const baseName = redisConfig.baseName;
    const client = new Redis({
        port: redisConfig.port,
        host: redisConfig.host,
        db: redisConfig.db,
        maxRetriesPerRequest: 1,
    })

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
        if (!isValidShare) {
            return;
        }
        // shareData
        // {
        //     job: '5',
        //     ip: '::ffff:81.177.74.130',
        //     port: 3031,
        //     height: 22012,
        //     blockReward: 300000000000,
        //     difficulty: 32,
        //     shareDiff: '1124.84785578',
        //     blockDiff: 125303.470650112,
        //     blockDiffActual: 489.466682227,
        //     blockHash: undefined,
        //     blockHashInvalid: undefined,
        //     login: 'GJK9gjntGMR3sQENuhNL99t6gkx2ct5xvb',
        //     worker: 'testtest',
        
        // }
        let redisCommands = [];

        redisCommands.push(['hincrbyfloat', `${baseName}:stats`, 'roundShares', shareData.difficulty]);

        if (rewardType !== 'solo') {
            const times = Math.floor(shareData.difficulty / baseShareDifficulty);

            for (let i = 0; i < times; ++i) {
                redisCommands.push(['lpush', `${baseName}:lastShares`, shareData.login]);
            }
            redisCommands.push(['ltrim', `${baseName}:lastShares`, 0, pplns - 1]);
        } else {
            redisCommands.push(['hincrbyfloat', `${baseName}:miners:${shareData.login}`, 'soloShares', shareData.difficulty]);
        }

        const dateNow = Date.now();
        const dateNowMs = Math.round(dateNow / 1000);
        const hashrateData = [ isValidShare ? shareData.difficulty : -shareData.difficulty, shareData.login, shareData.worker, dateNow];
        redisCommands.push(['zadd', `${baseName}:hashrate`, dateNowMs, hashrateData.join(':')]);
        redisCommands.push(['zadd', `${baseName}:hashrate:miners:` + shareData.login, dateNowMs, hashrateData.join(':')]);
        redisCommands.push(['hset', `${baseName}:miners:${shareData.login}`, 'lastShare', dateNowMs]);

        if (isValidBlock) {
            redisCommands.push(['hset', `${baseName}:stats`, `lastBlockFound`, dateNowMs]);
            redisCommands.push(['hincrby', `${baseName}:miners:${shareData.login}`, 'blocksFound', 1]);

            if (rewardType === 'pplns') {
                redisCommands.push(['lrange', `${baseName}:lastShares`, 0, pplns]);
                redisCommands.push(['hget', `${baseName}:stats`, 'roundShares']);
            } else if (rewardType === 'solo') {
                redisCommands.push(['hget', `${baseName}:miners:${shareData.login}`, 'soloShares']);
            }
        }

        client.multi(redisCommands).exec(function (err, replies) {
            if (err) {
                log.error(logSystem, logComponent, logSubCat, `Error(1) failed to insert share data into redis:\n${JSON.stringify(redisCommands)}\n${JSON.stringify(err)}`);
                return;
            }

            if (isValidBlock) {
                let redisCommands2 = [];
                const totalShares = replies[replies.length - 1][1];
                if (rewardType === 'solo') {
                    redisCommands2.push(['hdel', `${baseName}:miners:${shareData.login}`, 'soloShares']);
                } else if (rewardType === 'pplns') {
                    const pplnsShares = replies[replies.length - 2][1];
                    let totalSharesArr = [];

                    for (const miner of pplnsShares) {
                        if (!totalSharesArr[miner]) {
                            totalSharesArr[miner] = 1;
                        } else {
                            ++totalSharesArr[miner];
                        }
                    }

                    for (const miner in totalSharesArr) {
                        redisCommands2.push(['hincrby', `${baseName}:shares:pplnsRound${shareData.height}`, miner, totalSharesArr[miner]]);
                    }
                }

                redisCommands2.push(['zadd', `${baseName}:blocks:candidates`, shareData.height,
                    [
                        rewardType,
                        shareData.login,
                        shareData.blockHash,
                        shareData.txHash,
                        Date.now() / 1000 | 0,
                        shareData.blockDiff,
                        totalShares,
                    ].join(':')]
                );
                redisCommands2.push(['hdel', `${baseName}:stats`, 'roundShares']);

                client.multi(redisCommands2).exec(function (err, _) {
                    if (err) {
                        log.error(logSystem, logComponent, logSubCat, `Error(2) failed to insert share data into redis:\n${JSON.stringify(redisCommands)}\n${JSON.stringify(err)}`);
                        // return;
                    }
                })
            }
        });
    };
};
