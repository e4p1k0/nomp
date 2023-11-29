const Stratum = require('stratum-pool');
// const KawpowStratum = require('kawpow-stratum-pool');

// const redis   = require('ioredis');
// const net     = require('net');
const ShareProcessor = require('./shareProcessor.js');

module.exports = function(logger){
    const _this = this

    const poolConfigs  = JSON.parse(process.env.pools);
    // var portalConfig = JSON.parse(process.env.portalConfig);

    const forkId = process.env.forkId;
    
    let pools = {};

    //Handle messages from master process sent via IPC
    process.on('message', function(message) {
        switch(message.type){

            case 'banIP':
                for (let p in pools){
                    if (pools[p].stratumServer)
                        pools[p].stratumServer.addBannedIP(message.ip);
                }
                break;

            case 'blocknotify':

                const messageCoin = message.coin.toLowerCase()
                const poolTarget = Object.keys(pools).filter(function (p) {
                    return p.toLowerCase() === messageCoin
                })[0]

                if (poolTarget)
                    pools[poolTarget].processBlockNotify(message.hash, 'blocknotify script');

                break;
        }
    });

    Object.keys(poolConfigs).forEach(function(coin) {
        let poolOptions = poolConfigs[coin];
        const logSystem = 'Pool'
        const logComponent = coin
        const logSubCat = 'Thread ' + (parseInt(forkId) + 1)

        const handlers = {
            auth: function () {
            },
            share: function () {
            },
            diff: function () {
            },
        }

        const shareProcessor = new ShareProcessor(logger, poolOptions)

        handlers.auth = function(port, login, password, authCallback){
            if (poolOptions.validateWorkerUsername !== true)
                authCallback(true);
            else {
                if (login.length === 40) {
                    try {
                        new Buffer(login, 'hex');
                        authCallback(true);
                    }
                    catch (e) {
                        authCallback(false);
                    }
                } else {
                    pool.daemon.cmd('validateaddress', [login], function (results) {
                        const isValid = results.filter(function (r) {
                            return r.response.isvalid
                        }).length > 0
                        authCallback(isValid);
                    });
                }

            }
        };

        handlers.share = function(isValidShare, isValidBlock, data){
            shareProcessor.handleShare(isValidShare, isValidBlock, data);
        };

        const authorizeFN = function (ip, port, login, password, callback) {
            handlers.auth(port, login, password, function (authorized) {
                const authString = authorized ? 'Authorized' : 'Unauthorized '

                logger.debug(logSystem, logComponent, logSubCat, authString + ' ' + login + ':' + password + ' [' + ip + ']')
                callback({
                    error: null,
                    authorized: authorized,
                    disconnect: false,
                })
            })
        }

        console.log(Stratum)
        const pool = Stratum.createPool(poolOptions, authorizeFN, logger);
        pool.on('share', function(isValidShare, isValidBlock, data){
            const shareData = JSON.stringify(data)
            // job: '1',
            // ip: '::ffff:81.177.74.130',
            // port: 3031,
            // height: 26114,
            // blockReward: 300080000000,
            // difficulty: 8,
            // shareDiff: '19.13993564',
            // blockDiff: 93126.51531904,
            // blockDiffActual: 363.775450465,
            // blockHash: undefined,
            // blockHashInvalid: undefined,
            // login: 'GJK9gjntGMR3sQENuhNL99t6gkx2ct5xvb',
            // worker: 'Laptop4'

            if (data.blockHash && !isValidBlock)
                logger.debug(logSystem, logComponent, logSubCat, 'We thought a block was found but it was rejected by the daemon, share data: ' + shareData);

            else if (isValidBlock)
                logger.debug(logSystem, logComponent, logSubCat, `Block found: ${data.blockHash} by ${data.login}/${data.worker}`);

            if (isValidShare) {
                if(data.shareDiff > 1000000000)
                    logger.debug(logSystem, logComponent, logSubCat, 'Share was found with diff higher than 1.000.000.000!');
                else if(data.shareDiff > 1000000)
                    logger.debug(logSystem, logComponent, logSubCat, 'Share was found with diff higher than 1.000.000!');
                logger.debug(logSystem, logComponent, logSubCat, `✔️ ${data.difficulty}/${data.shareDiff} by ${data.login}.${data.worker} [${data.ip}]`);

            } else if (!isValidShare) {
                logger.debug(logSystem, logComponent, logSubCat, 'Share rejected: ' + shareData);
                console.log(data)
            }

            handlers.share(isValidShare, isValidBlock, data)

        }).on('difficultyUpdate', function(fullLogin, diff){
            logger.debug(logSystem, logComponent, logSubCat, 'Difficulty update to diff ' + diff + ' ' + JSON.stringify(fullLogin));
            handlers.diff(fullLogin, diff);
        }).on('log', function(severity, text) {
            logger[severity](logSystem, logComponent, logSubCat, text);
        }).on('banIP', function(ip, worker){
            process.send({type: 'banIP', ip: ip});
        })
        // .on('started', function(){
        //     _this.setDifficultyForProxyPort(pool, poolOptions.coin.name, poolOptions.coin.algorithm);
        // });

        pool.start();
        pools[poolOptions.coin.name] = pool;
    });
};
