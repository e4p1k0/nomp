const fs = require('fs');
const async = require('async');
const Redis = require('ioredis');

const Stratum = require('stratum-pool');
const util = require('stratum-pool/lib/util.js');

module.exports = function(logger){
    const poolConfigs = JSON.parse(process.env.pools);
    let enabledPools = [];

    Object.keys(poolConfigs).forEach(function(coin) {
        const poolOptions = poolConfigs[coin];
        if (poolOptions.paymentProcessing &&
            poolOptions.paymentProcessing.enabled)
            enabledPools.push(coin);
    });

    async.filter(enabledPools, function(coin, callback){
        SetupForPool(logger, poolConfigs[coin], function(setupResults){
            callback(setupResults);
        });
    }, function(coins, error){
        if (error) console.log('error', error)

        coins.forEach(function(coin){
            const poolOptions = poolConfigs[coin];
            const processingConfig = poolOptions.paymentProcessing;
            const logSystem = 'Payments';
            logger.debug(logSystem, coin, 'Payment processing setup to run every '
                + processingConfig.paymentInterval + ' second(s) with daemon ('
                + processingConfig.daemon.user + '@' + processingConfig.daemon.host + ':' + processingConfig.daemon.port
                + ') and redis (' + poolOptions.redis.host + ':' + poolOptions.redis.port + ')');

        });
    });
};

function SetupForPool(logger, poolOptions, setupFinished){
    const coin = poolOptions.coin.name;
    const pplns = poolOptions.pplns;
    const processingConfig = poolOptions.paymentProcessing;
    const logSystem = 'Payments';
    const logComponent = coin;
    const daemon = new Stratum.daemon.interface([processingConfig.daemon], function(severity, message){
        logger[severity](logSystem, logComponent, message);
    });
    const redisConfig = poolOptions.redis;
    const redisClient = new Redis({
        port: redisConfig.port,
        host: redisConfig.host,
        db: redisConfig.db,
        maxRetriesPerRequest: 1,
        readTimeout: 5
    })

    let magnitude;
    let minPaymentSatoshis;
    let coinPrecision;
    let paymentInterval;




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
                    minPaymentSatoshis = parseInt(processingConfig.minimumPayment * magnitude);
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

        paymentInterval = setInterval(function(){
            try {
                processPayments();
            } catch(e){
                throw e;
            }
        }, processingConfig.paymentInterval * 1000);
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
                redisClient.multi([
                    ['keys', coin + ':miners:*']
                ]).exec(function(error, results){
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
                        logger.error(logSystem, logComponent, 'Could not get candidates from redis ' + JSON.stringify(error));
                        callback(true);
                        return;
                    }

                    let minersToPay = [];
                    for (let i in results) {
                        const balance = parseFloat(results[i][1]);
                        if (!balance || balance < minPaymentSatoshis) continue;

                        const address = keys[i].replace(`${coin}:miners:`, '');
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
                        console.log(toSend)
                        console.log(satoshisToCoins(toSend))
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


                    console.log('poolOptions.address', poolOptions.address)
                    console.log('addressAmounts', addressAmounts)
                    console.log('totalSent', totalSent)
                    if (totalSent === 0) {
                        logger.debug(logSystem, logComponent, 'No miners to credit');
                        callback(true);
                        return;
                    }
                    // daemon.cmd('sendmany', [poolOptions.address || '', addressAmounts], function (result) {
                    daemon.cmd('sendmany', ['', addressAmounts], function (result) {
                        //Check if payments failed because wallet doesn't have enough coins to pay for tx fees
                        if (result.error && result.error.code === -6) {
                            console.log(result)
                            const higherPercent = withholdPercent + 0.01;
                            logger.warning(logSystem, logComponent, 'Not enough funds to cover the tx fees for sending out payments, decreasing rewards by '
                                + (higherPercent * 100) + '% and retrying');
                            // trySend(higherPercent);
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
                            callback(null, addressAmounts);
                        }

                        //  ADD PAYMENT TO DB
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
            function(addressAmounts, rounds, callback){
                console.log('addressAmounts', addressAmounts)
                console.log('kiska')

                let redisCommands = [];
                for (const address of Object.keys(addressAmounts)) {
                    redisCommands.push(['hincrby', `${coin}:miners:${address}`, 'balance', -1 * coinsToSatoshies(parseFloat(addressAmounts[address]))]);
                }
                console.log('redisCommands', redisCommands)

                startRedisTimer();
                redisClient.multi(redisCommands).exec(function(error, results){
                    endRedisTimer();
                    console.log('final error', error)
                    console.log('final results', results)
                    if (error) {
                        clearInterval(paymentInterval);
                        logger.error(logSystem, logComponent,
                            'Payments sent but could not update redis. ' + JSON.stringify(error)
                            + ' Disabling payment processing to prevent possible double-payouts. The redis commands in '
                            + coin + '_finalRedisCommands.txt must be ran manually');
                        fs.writeFile(coin + '_finalRedisCommands.txt', JSON.stringify(finalRedisCommands), function(err){
                            logger.error('Could not write finalRedisCommands.txt, you are fucked.');
                        });
                    }

                    callback(true);
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
