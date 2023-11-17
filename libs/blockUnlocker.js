const async = require('async');
const Redis = require('ioredis');
const Stratum = require('stratum-pool');

module.exports = function(logger){
    const poolConfigs = JSON.parse(process.env.pools);
    let enabledPools = [];

    Object.keys(poolConfigs).forEach(function(configName) {
        const poolOptions = poolConfigs[configName];
        if (poolOptions.blockUnlocker?.enabled) enabledPools.push(configName);
    });

    async.filter(enabledPools, function(configName, callback){
        SetupForPool(logger, poolConfigs[configName], function(setupResults){
            callback(setupResults);
        });
    }, function(coins, error){
        if (error) console.log('error', error)

        coins.forEach(function(configName){
            const poolOptions = poolConfigs[configName];
            const cfg = poolOptions.blockUnlocker;
            const daemonCfg = poolOptions.daemons[0];
            const logSystem = 'Unlocker';
            logger.debug(logSystem, configName, `Block unlocker setup to run every ${cfg.interval} sec`
                + ` with daemon (${daemonCfg.user}@${daemonCfg.host}:${daemonCfg.port})`
                + ` and redis (${poolOptions.redis.host}:${poolOptions.redis.port})`);

        });
    });
};

function SetupForPool(logger, poolOptions, setupFinished){
    const name = poolOptions.name;
    const pplns = poolOptions.pplns;

    const cfg = poolOptions.blockUnlocker;
    const daemonCfg = poolOptions.daemons[0];
    const logSystem = 'Unlocker';
    const logComponent = name;
    const daemon = new Stratum.daemon.interface([daemonCfg], function(severity, message){
        logger[severity](logSystem, logComponent, message);
    });

    const redisConfig = poolOptions.redis;
    const baseName = poolOptions.redis.baseName;
    const redisClient = new Redis({
        port: redisConfig.port,
        host: redisConfig.host,
        db: redisConfig.db,
        maxRetriesPerRequest: 1,
    });

    let magnitude;
    let coinPrecision;
    let blockUnlockingInterval;

    async.parallel([
        function(callback){
            daemon.cmd('validateaddress', [poolOptions.address], function(result) {
                if (result.error) {
                    logger.error(logSystem, logComponent, 'Error with block unlocker daemon ' + JSON.stringify(result.error));
                    callback(true);
                } else if (!result.response || !result.response.ismine) {
                    daemon.cmd('getaddressinfo', [poolOptions.address], function(result) {
                        if (result.error){
                            logger.error(logSystem, logComponent, 'Error with block unlocker daemon, getaddressinfo failed ... ' + JSON.stringify(result.error));
                            callback(true);
                        } else if (!result.response || !result.response.ismine) {
                            logger.error(logSystem, logComponent,
                                    'Daemon does not own pool address - block unlocking can not be done with this daemon, '
                                    + JSON.stringify(result.response));
                            callback(true);
                        } else {
                            callback()
                        }
                    }, true);
                } else {
                    callback()
                }
            }, true);
        },

        function(callback){
            daemon.cmd('getbalance', [], function(result){
                if (result.error){
                    callback(true);
                    return;
                }

                try {
                    const d = result.data.split('result":')[1].split(',')[0].split('.')[1];
                    magnitude = parseInt('10' + new Array(d.length).join('0'));
                    coinPrecision = magnitude.toString().length - 1;
                    callback();
                }
                catch(e){
                    logger.error(logSystem, logComponent, 'Error detecting number of satoshis in a coin, cannot do block unlocking. Tried parsing: ' + result.data);
                    callback(true);
                }

            }, true, true);
        }
    ], function(err){
        if (err){
            setupFinished(false);
            return;
        }

        blockUnlockingInterval = setInterval(function(){
            try {
                processBlockUnlocking();
            } catch(e){
                throw e;
            }
        }, cfg.interval * 1000);
        setTimeout(processBlockUnlocking, 100);
        setupFinished(true);
    });

    /* Deal with numbers in the smallest possible units (satoshis) as much as possible. This greatly helps with accuracy
       when rounding and whatnot. When we are storing numbers for only humans to see, store in whole coin units. */
    const processBlockUnlocking = function(){
        const processStarted = Date.now();

        let timeSpentRPC = 0;
        let timeSpentRedis = 0;

        let startTimeRedis;
        let startTimeRPC;

        const startRedisTimer = function(){ startTimeRedis = Date.now() };
        const endRedisTimer = function(){ timeSpentRedis += Date.now() - startTimeRedis };

        const startRPCTimer = function(){ startTimeRPC = Date.now(); };
        const endRPCTimer = function(){ timeSpentRPC += Date.now() - startTimeRPC };

        async.waterfall([
            /* Call redis to get candidates */
            function(callback){
                startRedisTimer();
                redisClient.multi([
                    ['zrangebyscore', `${baseName}:blocks:candidates`, 0, '+inf', 'WITHSCORES']
                ]).exec(function(error, results){
                    endRedisTimer();

                    if (error) {
                        logger.error(logSystem, logComponent, 'Could not get candidates from redis ' + JSON.stringify(error));
                        callback(true);
                        return;
                    }

                    let rawCandidates = results[0][1];
                    let rounds = [];
                    for (let i = 0; i < rawCandidates.length - 1; i = i + 2) {
                        const rawCandidateStr = rawCandidates[i];
                        const height = +rawCandidates[i + 1];
                        const data = rawCandidateStr.split(':');
                        rounds.push({
                            type:       data[0],
                            finder:     data[1],
                            blockhash:  data[2],
                            txHash:     data[3],
                            serialized: rawCandidateStr,
                            height,
                        })
                    }
                    callback(null,  rounds);
                });
            },

            /* Does a batch rpc call to daemon with all the transaction hashes to see if they are confirmed yet.
               It also adds the block reward amount to the round object - which the daemon also gives us. */
            function(rounds, callback){
                const batchRPCcommand = rounds.map(function(r){
                    return ['gettransaction', [r.txHash]];
                });

                startRPCTimer();
                daemon.batchCmd(batchRPCcommand, function(error, txDetails){
                    endRPCTimer();

                    if (error || !txDetails) {
                        logger.error(logSystem, logComponent, 'Check finished - daemon rpc error with batch gettransactions '
                            + JSON.stringify(error));
                        callback(true);
                        return;
                    }

                    txDetails.forEach(function(tx, i){
                        const round = rounds[i];

                        if (tx.error && tx.error.code === -5) {
                            logger.warning(logSystem, logComponent, 'Daemon reports invalid transaction: ' + round.txHash);
                            round.category = 'kicked';
                            return;
                        } else if (!tx.result.details || (tx.result.details && tx.result.details.length === 0)) {
                            logger.warning(logSystem, logComponent, 'Daemon reports no details for transaction: ' + round.txHash);
                            round.category = 'kicked';
                            return;
                        } else if (tx.error || !tx.result) {
                            logger.error(logSystem, logComponent, 'Odd error with gettransaction ' + round.txHash + ' '
                                + JSON.stringify(tx));
                            return;
                        }

                        let generationTx = tx.result.details.filter(function(tx) {
                            return tx.address === poolOptions.address;
                        })[0];

                        if (!generationTx && tx.result.details.length === 1) {
                            generationTx = tx.result.details[0];
                        }

                        if (!generationTx) {
                            logger.error(logSystem, logComponent, 'Missing output details to pool address for transaction '
                                + round.txHash);
                            return;
                        }

                        round.category = generationTx.category;

                        if (round.category === 'generate') {
                            const reward = generationTx.amount || generationTx.value;
                            round.reward = reward * magnitude;
                        }
                    });

                    let goodRounds = [];
                    let badRounds = [];

                    for (const round of rounds) {
                        if (round.category === 'generate') {
                            goodRounds.push(round);
                        } else if (round.category !== 'immature') {
                            badRounds.push(round);
                        }
                    }
                    callback(null, goodRounds, badRounds);
                });
            },

            /* Does a batch redis call to get shares contributed to each round. Then calculates the reward
               amount owned to each miner for each round. */
            function(rounds, badRounds, callback){
                const redisCommands = rounds.map(function(r){
                    if (r.type === 'pplns') return ['hgetall', `${baseName}:shares:pplnsRound` + r.height];

                    return ['echo', 'solo'];
                });

                startRedisTimer();
                redisClient.multi(redisCommands).exec(function(error, allWorkerShares){
                    endRedisTimer();

                    if (error) {
                        callback('Check finished - redis error with multi get rounds share');
                        return;
                    }

                    rounds.forEach(function(round, i){
                        let workerShares = allWorkerShares[i][1];

                        if (!workerShares) {
                            logger.error(logSystem, logComponent, `No worker shares for round ${round.height}`);
                            return;
                        }


                        if (allWorkerShares[i][0]) {
                            logger.error(logSystem, logComponent, 'No worker shares for round: '
                                + round.height + ' blockHash: ' + round.blockHash);
                            return;
                        }
                        const roundShares = allWorkerShares[i][1];

                        const recipients = [];
                        if (round.type === 'solo') {
                            recipients.push({
                                login: round.finder,
                                share: 1,
                                reward: round.reward
                            })
                        } else if (round.type === 'pplns') {
                            for (let login in roundShares) {
                                const share = roundShares[login] / pplns;
                                const reward = Math.floor(round.reward * share);
                                recipients.push({
                                    login, share, reward
                                })
                            }
                        } else {
                            logger.error(logSystem, logComponent, `Unknown round type: ${round.type}`
                                 + ` blockHash: ${round.blockHash}`);
                            return;
                        }
                        round.recipients = recipients;
                    });

                    callback(null, rounds, badRounds);
                });
            },
            /* create rewards and charge rewards to miner balances */
            function(rounds, badRounds, callback) {
                const now = Math.round(Date.now() / 1000)
                let redisCommands = [];
                for (const round of rounds) {
                    for (const recipient of round.recipients) {
                        redisCommands.push(['zadd', `${baseName}:rewards:${round.type}:${recipient.login}`, now, [
                            recipient.reward,
                            recipient.share,
                            round.blockhash,
                            round.height
                        ].join(':')]);

                        redisCommands.push(['hincrbyfloat', `${baseName}:miners:${recipient.login}`, 'balance', recipient.reward]);
                    }
                }

                startRedisTimer();
                redisClient.multi(redisCommands).exec(function(error, _) {
                    endRedisTimer();
                    if (error) {
                        callback('ERROR occured while trying to update balances and rewards!!!');
                        return;
                    }

                    callback(null, rounds, badRounds)
                })
            },

            /* move candidates to matured */
            function(rounds, badRounds, callback) {
                let redisCommands = [];

                for (const round of [...rounds, ...badRounds]) {
                    let uncleKey;
                    if (round.category === 'generate') {
                        uncleKey = 0;
                    } else if (round.category === 'orphan') {
                        uncleKey = 1;
                    } else {
                        uncleKey = 2;
                    }

                    if (!round.reward) {    //  in non-generate rounds
                        round.reward = 0;
                    }
                    redisCommands.push(['zrem', `${baseName}:blocks:candidates`, round.serialized]);
                    redisCommands.push(['zadd', `${baseName}:blocks:matured`, round.height, `${round.serialized}:${round.reward}:${uncleKey}`]);
                }

                startRedisTimer();
                redisClient.multi(redisCommands).exec(function(error, _) {
                    endRedisTimer();
                    if (error) {
                        callback('ERROR occured while trying to move blocks from candidates to matured!!!');
                    }
                })
                callback(null)
            }
        ], function(){
            const paymentProcessTime = Date.now() - processStarted;
            logger.debug(logSystem, logComponent, `Finished interval - ${paymentProcessTime} ms total: `
                + `${timeSpentRedis} ms redis, ${timeSpentRPC} ms RPC daemon`);
        });
    };
}
