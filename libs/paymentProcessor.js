const fs = require('fs');
const async = require('async');
const Redis = require('ioredis');

const Stratum = require('stratum-pool');
const util = require('stratum-pool/lib/util.js');

module.exports = function(logger){
    const poolConfigs = JSON.parse(process.env.pools);
    let enabledPools = [];

    Object.keys(poolConfigs).forEach(function(configName) {
        const poolOptions = poolConfigs[configName];
        if (poolOptions.paymentProcessing?.enabled) {
            enabledPools.push(configName);
        }
    });

    async.filter(enabledPools, function(configName, callback){
        SetupForPool(logger, poolConfigs[configName], function(setupResults){
            callback(setupResults);
        });
    }, function(coins, error){
        if (error) console.log('error', error)

        coins.forEach(function(configName){
            const poolOptions = poolConfigs[configName];

            const cfg = poolOptions.paymentProcessing;
            const daemonCfg = poolOptions.daemons[0];

            const logSystem = 'Payments';
            logger.debug(logSystem, configName, `Payment processing setup to run every ${cfg.interval}`
                + ` with daemon (${daemonCfg.user}@${daemonCfg.host}:${daemonCfg.port})`
                + ` and redis (${poolOptions.redis.host}:${poolOptions.redis.port})`);

        });
    });
};

function SetupForPool(logger, poolOptions, setupFinished){
    const name = poolOptions.name;
    const cfg = poolOptions.paymentProcessing;
    const daemonCfg = poolOptions.daemons[0];
    const logSystem = 'Payments';
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
    })

    let magnitude;
    let minPaymentSatoshis;
    let coinPrecision;
    let interval;

    async.parallel([
        function(callback){
            daemon.cmd('validateaddress', [poolOptions.address], function(result) {
                if (result.error) {
                    logger.error(logSystem, logComponent, 'Error with payment processing daemon ' + JSON.stringify(result.error));
                    callback(true);
                } else if (!result.response || !result.response.ismine) {
                    daemon.cmd('getaddressinfo', [poolOptions.address], function(result) {
                        if (result.error){
                            logger.error(logSystem, logComponent, 'Error with payment processing daemon, getaddressinfo failed ... ' + JSON.stringify(result.error));
                            callback(true);
                        } else if (!result.response || !result.response.ismine) {
                            logger.error(logSystem, logComponent,
                                    'Daemon does not own pool address - payment processing can not be done with this daemon, '
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
                    minPaymentSatoshis = parseInt(cfg.minimumPayment * magnitude);
                    coinPrecision = magnitude.toString().length - 1;
                    callback();
                }
                catch(e){
                    logger.error(logSystem, logComponent, 'Error detecting number of satoshis in a coin, cannot do payment processing. Tried parsing: ' + result.data);
                    callback(true);
                }

            }, true, true);
        }
    ], function(err){
        if (err){
            setupFinished(false);
            return;
        }

        interval = setInterval(function(){
            try {
                processPayments();
            } catch(e){
                throw e;
            }
        }, cfg.interval * 1000);
        setTimeout(processPayments, 100);
        setupFinished(true);
    });

    const satoshisToCoins = function(satoshis){
        return parseFloat((satoshis / magnitude).toFixed(coinPrecision));
    };

    const coinsToSatoshies = function(coins){
        return coins * magnitude;
    };

    /* Deal with numbers in the smallest possible units (satoshis) as much as possible. This greatly helps with accuracy
       when rounding and whatnot. When we are storing numbers for only humans to see, store in whole coin units. */

    const processPayments = function(){
        const startPaymentProcess = Date.now();

        let timeSpentRPC = 0;
        let timeSpentRedis = 0;

        let startTimeRedis;
        let startTimeRPC;

        const startRedisTimer = function(){ startTimeRedis = Date.now() };
        const endRedisTimer = function(){ timeSpentRedis += Date.now() - startTimeRedis };

        const startRPCTimer = function(){ startTimeRPC = Date.now(); };
        const endRPCTimer = function(){ timeSpentRPC += Date.now() - startTimeRedis };

        async.waterfall([
            /* Call redis to get candidates */
            function(callback){

                startRedisTimer();
                redisClient.multi([ ['keys', `${baseName}:miners:*`] ]).exec(function(error, results){
                    endRedisTimer();

                    if (error || !results[0] || !results[0][1]) {
                        logger.error(logSystem, logComponent, 'Could not get candidates from redis ' + JSON.stringify(error));
                        callback(true);
                        return;
                    }

                    callback(null, results[0][1]);
                });
            },

            function(keys, callback){
                const redisCommands = keys.map(key => ['hget', key, 'balance'])

                startRedisTimer();
                redisClient.multi(redisCommands).exec(function(error, results){
                    endRedisTimer();

                    if (error) {
                        logger.error(logSystem, logComponent, 'Could not get miners from redis ' + JSON.stringify(error));
                        callback(true);
                        return;
                    }

                    let minersToPay = [];
                    for (let i in results) {
                        const balance = parseFloat(results[i][1]);
                        if (!balance || balance < minPaymentSatoshis) continue;

                        const address = keys[i].replace(`${baseName}:miners:`, '');
                        if (!address) continue;

                        minersToPay.push({
                            address, balance
                        });
                    }

                    callback(null, minersToPay);
                });
            },

            /* Calculate if any payments are ready to be sent and trigger them sending
             Get balance different for each address and pass it along as object of latest balances such as
             {worker1: balance1, worker2, balance2}
             when deciding the sent balance, it the difference should be -1*amount they had in db,
             if not sending the balance, the differnce should be +(the amount they earned this round)
             */
            function(minersToPay, callback) {
                const trySend = function (withholdPercent) {
                    let addressAmounts = {};
                    let totalSent = 0;
                    for (const w in minersToPay) {
                        const worker = minersToPay[w];
                        worker.balance = worker.balance || 0;
                        const toSend = worker.balance * (1 - withholdPercent);
                        if (toSend >= minPaymentSatoshis) {
                            totalSent += toSend;
                            const address = worker.address = (worker.address || getProperAddress(w));
                            worker.sent = addressAmounts[address] = satoshisToCoins(toSend);
                            worker.balanceChange = Math.min(worker.balance, toSend) * -1;
                        } else {
                            worker.balanceChange = Math.max(toSend - worker.balance, 0);
                            worker.sent = 0;
                        }
                    }

                    // return
                    if (totalSent === 0) {
                        logger.debug(logSystem, logComponent, 'No miners to credit');
                        callback(true);
                        return;
                    }
                    startRPCTimer();
                    daemon.cmd('sendmany', ['', addressAmounts], function (result) {
                        endRPCTimer();
                        //Check if payments failed because wallet doesn't have enough coins to pay for tx fees
                        if (result.error && result.error.code === -6) {
                            // error: { code: -6, message: 'Account has insufficient funds' },
                            const higherPercent = withholdPercent + 0.01;
                            logger.warning(logSystem, logComponent, 'Not enough funds to cover the tx fees for sending out payments, decreasing rewards by '
                                + (higherPercent * 100) + '% and retrying');
                            trySend(higherPercent);
                        } else if (result.error) {
                            logger.error(logSystem, logComponent, 'Error trying to send payments with RPC sendmany '
                                + JSON.stringify(result.error));
                            callback(true);
                        } else {
                            console.log('result', result)
                            logger.debug(logSystem, logComponent, 'Sent out a total of ' + (totalSent / magnitude)
                                + ' to ' + Object.keys(addressAmounts).length + ' workers');
                            if (withholdPercent > 0) {
                                logger.warning(logSystem, logComponent, 'Had to withhold ' + (withholdPercent * 100)
                                    + '% of reward from miners to cover transaction fees. '
                                    + 'Fund pool wallet with coins to prevent this from happening');
                            }
                            callback(null, addressAmounts, result.response);
                        }

                        // {
                        //     error: null,
                        //         response: '06495b809c37abcdbe724c5d7b8103fae641c3820bae3ea048fd828cb5dae1c2',
                        //     instance: {
                        //     host: '127.0.0.1',
                        //         port: 8887,
                        //         user: 'gmuser',
                        //         password: 'gmpass',
                        //         index: 0
                        // },
                        //     data: '{"result":"06495b809c37abcdbe724c5d7b8103fae641c3820bae3ea048fd828cb5dae1c2","error":null,"id":1699110009611}\n'
                        // }

                    }, true, true);
                };
                trySend(0);

            },
            function(addressAmounts, txId, callback){
                const now = Math.round(Date.now() / 1000);
                let redisCommands = [];
                for (const address of Object.keys(addressAmounts)) {
                    const amount = coinsToSatoshies(parseFloat(addressAmounts[address]));
                    redisCommands.push(['hincrbyfloat', `${baseName}:miners:${address}`, 'balance', -1 * amount]);
                    redisCommands.push(['hincrbyfloat', `${baseName}:miners:${address}`, 'paid', amount]);
                    redisCommands.push(['zadd', `${baseName}:payments:${address}`, now, [txId, amount].join(':')]);
                }

                startRedisTimer();
                redisClient.multi(redisCommands).exec(function(error, _){
                    endRedisTimer();
                    if (error) {
                        clearInterval(interval);
                        logger.error(logSystem, logComponent,
                            'Payments sent but could not update redis. ' + JSON.stringify(error)
                            + ' Disabling payment processing to prevent possible double-payouts. The redis commands in '
                            + name + '_finalRedisCommands.txt must be ran manually');
                        fs.writeFile(name + '_finalRedisCommands.txt', JSON.stringify(redisCommands), function(_){
                            logger.error('Could not write finalRedisCommands.txt, you are fucked.');
                        });
                    }

                    callback(null);
                });
            }

        ], function(){
            const paymentProcessTime = Date.now() - startPaymentProcess;
            logger.debug(logSystem, logComponent, 'Finished interval - time spent: '
                + paymentProcessTime + 'ms total, ' + timeSpentRedis + 'ms redis, '
                + timeSpentRPC + 'ms daemon RPC');
        });
    };

    const getProperAddress = function(address){
        if (address.length === 40){
            return util.addressFromEx(poolOptions.address, address);
        }
        else return address;
    };
}
