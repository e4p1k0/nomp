const fs = require('fs');
const path = require('path');
const os = require('os');
const cluster = require('cluster');
const extend = require('extend');

const PoolLogger = require('./libs/logUtil.js');
const CliListener = require('./libs/cliListener.js');
const PoolWorker = require('./libs/poolWorker.js');
const BlockUnlocker = require('./libs/blockUnlocker.js');
const PaymentProcessor = require('./libs/paymentProcessor.js');
const Website = require('./libs/website.js');
const PoolApi = require('./libs/poolApi.js');

const algos = {
    ...require('stratum-pool/lib/algoProperties.js'),
    ...require('kawpow-stratum-pool/lib/algoProperties.js')
};
console.log(algos)

JSON.minify = JSON.minify || require("node-json-minify");

if (!fs.existsSync('config.json')){
    console.log('config.json file does not exist. Read the installation/setup instructions.');
    return;
}

const portalConfig = JSON.parse(JSON.minify(fs.readFileSync("config.json", {encoding: 'utf8'})));

const defaultValues = {
    type:   'pplns',
    pplns:  10000,
    redis: {
        db: 0,
    },
    poolApi: {
        rpcInterval: 5,
        graphInterval: 300,
        hrWindow: 1800,
        largeHrWindow: 10800
    },
};

let poolConfigs;

let logger = new PoolLogger({
    logLevel: portalConfig.logLevel,
    logColors: portalConfig.logColors
});

if (cluster.isWorker){
    switch(process.env.workerType){
        case 'pool':
            new PoolWorker(logger);
            break;
        case 'blockUnlocker':
            new BlockUnlocker(logger);
            break;
        case 'paymentProcessor':
            new PaymentProcessor(logger);
            break;
        case 'website':
            new Website(logger);
            break;
        case 'poolApi':
            new PoolApi(logger);
            break;
    }

    return;
} 

//  Read all pool configs from pool_configs and join them with their coin profile
const buildPoolConfigs = function(){
    const configDir = 'pool_configs/';
    let configs = {};
    let poolConfigFiles = [];

    /* Get filenames of pool config json files that are enabled */
    fs.readdirSync(configDir).forEach(function(file){
        if (!fs.existsSync(configDir + file) || path.extname(configDir + file) !== '.json') return;
        let poolOptions = JSON.parse(JSON.minify(fs.readFileSync(configDir + file, {encoding: 'utf8'})));
        if (!poolOptions.enabled) return;
        poolOptions.fileName = file;
        poolConfigFiles.push(poolOptions);
    });

    /* Ensure no pool uses any of the same ports as another pool */
    for (let i = 0; i < poolConfigFiles.length; i++){
        let ports = Object.keys(poolConfigFiles[i].ports);
        for (let f = 0; f < poolConfigFiles.length; f++){
            if (f === i) continue;
            let portsF = Object.keys(poolConfigFiles[f].ports);
            for (let g = 0; g < portsF.length; g++){
                if (ports.indexOf(portsF[g]) !== -1){
                    logger.error('Master', poolConfigFiles[f].fileName, 'Has same configured port of ' + portsF[g] + ' as ' + poolConfigFiles[i].fileName);
                    process.exit(1);
                    return;
                }
            }

            // if (poolConfigFiles[f].coin === poolConfigFiles[i].coin){
            //     logger.error('Master', poolConfigFiles[f].fileName, 'Pool has same configured coin file coins/' + poolConfigFiles[f].coin + ' as ' + poolConfigFiles[i].fileName + ' pool');
            //     process.exit(1);
            //     return;
            // }

        }
    }

    poolConfigFiles.forEach(function(poolOptions){
        poolOptions.coinFileName = poolOptions.coin;
        let coinFilePath = 'coins/' + poolOptions.coinFileName;
        if (!fs.existsSync(coinFilePath)){
            logger.error('Master', poolOptions.coinFileName, 'could not find file: ' + coinFilePath);
            return;
        }

        let coinProfile = JSON.parse(JSON.minify(fs.readFileSync(coinFilePath, {encoding: 'utf8'})));
        poolOptions.coin = coinProfile;
        poolOptions.coin.name = poolOptions.coin.name.toLowerCase();

        if (! poolOptions.name) {
            poolOptions.name = `${poolOptions.coin.name}-${poolOptions.type}`
        }

        if (poolOptions.name in configs){
            logger.error('Master', poolOptions.fileName, 'coins/' + poolOptions.coinFileName
                + ' has same configured coin name ' + poolOptions.coin.name + ' as coins/'
                + configs[poolOptions.coin.name].coinFileName + ' used by pool config '
                + configs[poolOptions.coin.name].fileName);

            process.exit(1);
            return;
        }

        for (const option in portalConfig.defaultPoolConfigs){
            if (!(option in poolOptions)) {
                let toCloneOption = portalConfig.defaultPoolConfigs[option];
                let clonedOption = {};
                if (toCloneOption.constructor === Object) {
                    extend(true, clonedOption, toCloneOption);
                } else {
                    clonedOption = toCloneOption;
                }

                poolOptions[option] = clonedOption;
            }
        }

        configs[poolOptions.name] = poolOptions;

        if (!(coinProfile.algorithm in algos)) {
            logger.error('Master', coinProfile.name, 'Cannot run a pool for unsupported algorithm "' + coinProfile.algorithm + '"');
            delete configs[poolOptions.coin.name];
        }

        //  set default values
        if (!poolOptions.type || !['pplns', 'solo'].includes(poolOptions.type)) {
            poolOptions.type = defaultValues.type; //  default
            logger.error('Master', poolOptions.name,  `Pool type is not set, using default: ${poolOptions.type}`);
        }
        if (poolOptions.type === 'pplns' && !poolOptions.pplns) {
            poolOptions.pplns = defaultValues.pplns;  //  default
            logger.error('Master', poolOptions.name,  `Pplns is not set, using default: ${defaultValues.pplns}`);
        }

        if (!poolOptions.redis.baseName) {
            poolOptions.redis.baseName = poolOptions.coin.name;
            logger.error('Master', poolOptions.name,  `Redis baseName is not set, using default: ${poolOptions.coin.name}`);
        }

        if (!poolOptions.redis.db) {
            poolOptions.redis.db = defaultValues.redis.db;
            logger.error('Master', poolOptions.name,  `Redis db is not set, using default: ${defaultValues.redis.db}`);
        }

        if (poolOptions.poolApi?.enabled) {
            for (const option of Object.keys(defaultValues.poolApi)) {
                if (!poolOptions.poolApi[option]) {
                    poolOptions.poolApi[option] = defaultValues.poolApi[option];
                    logger.error('Master', poolOptions.name,  `PoolAPI ${option} not set, using default: ${defaultValues.poolApi[option]}`);
                }
            }

        }
        //  set default values end
    });
    return configs;
};

const spawnPoolWorkers = function(){
    Object.keys(poolConfigs).forEach(function(coin){
        let p = poolConfigs[coin];

        if (!Array.isArray(p.daemons) || p.daemons.length < 1){
            logger.error('Master', coin, 'No daemons configured so a pool cannot be started for this coin.');
            delete poolConfigs[coin];
        }
    });

    if (Object.keys(poolConfigs).length === 0){
        logger.warning('Master', 'PoolSpawner', 'No pool configs exists or are enabled in pool_configs folder. No pools spawned.');
        return;
    }

    let serializedConfigs = JSON.stringify(poolConfigs);

    let numForks = (function(){
        if (!portalConfig.clustering || !portalConfig.clustering.enabled) return 1;
        if (portalConfig.clustering.forks === 'auto') return os.cpus().length;
        if (!portalConfig.clustering.forks || isNaN(portalConfig.clustering.forks)) return 1;

        return portalConfig.clustering.forks;
    })();

    let poolWorkers = {};

    let createPoolWorker = function(forkId){
        let worker = cluster.fork({
            workerType: 'pool',
            forkId: forkId,
            pools: serializedConfigs,
            portalConfig: JSON.stringify(portalConfig)
        });
        worker.forkId = forkId;
        worker.type = 'pool';
        poolWorkers[forkId] = worker;
        worker.on('exit', function(code, signal){
            logger.error('Master', 'PoolSpawner', `Fork ${forkId} died (code ${code}, signal ${signal}), spawning replacement...`);
            setTimeout(function(){
                createPoolWorker(forkId);
            }, 2000);
        }).on('message', function(msg){
            switch(msg.type){
                case 'banIP':
                    Object.keys(cluster.workers).forEach(function(id) {
                        if (cluster.workers[id].type === 'pool') {
                            cluster.workers[id].send({type: 'banIP', ip: msg.ip});
                        }
                    });
                    break;
            }
        });
    };

    let i = 0;
    let spawnInterval = setInterval(function(){
        createPoolWorker(i);
        i++;
        if (i === numForks){
            clearInterval(spawnInterval);
            logger.debug('Master', 'PoolSpawner', 'Spawned ' + Object.keys(poolConfigs).length + ' pool(s) on ' + numForks + ' thread(s)');
        }
    }, 250);

};

const startCliListener = function(){
    let cliPort = portalConfig.cliPort;

    const listener = new CliListener(cliPort);
    listener.on('log', function(text){
        logger.debug('Master', 'CLI', text);
    }).on('command', function(command, params, options, reply){
        switch(command){
            case 'blocknotify':
                Object.keys(cluster.workers).forEach(function(id) {
                    cluster.workers[id].send({type: 'blocknotify', coin: params[0], hash: params[1]});
                });
                reply('Pool workers notified');
                break;
            case 'reloadpool':
                Object.keys(cluster.workers).forEach(function(id) {
                    cluster.workers[id].send({type: 'reloadpool', coin: params[0] });
                });
                reply('reloaded pool ' + params[0]);
                break;
            default:
                reply('unrecognized command "' + command + '"');
                break;
        }
    }).start();
};

const startBlockUnlocker = function(){
    let enabledForAny = false;
    for (const pool in poolConfigs){
        const p = poolConfigs[pool];
        const enabled = p.enabled && p.blockUnlocker && p.blockUnlocker.enabled;
        if (enabled) {
            enabledForAny = true;
            break;
        }
    }

    if (!enabledForAny) return;

    const worker = cluster.fork({
        workerType: 'blockUnlocker',
        pools: JSON.stringify(poolConfigs)
    });

    worker.on('exit', function(code, signal){
        logger.error('Master', 'Block Unlocker', `Block unlocker died (code ${code}, signal ${signal}), spawning replacement...`);
        setTimeout(function(){
            startBlockUnlocker(poolConfigs);
        }, 2000);
    });
};

const startPaymentProcessor = function(){
    let enabledForAny = false;
    for (const pool in poolConfigs){
        const p = poolConfigs[pool];
        const enabled = p.enabled && p.paymentProcessing?.enabled;
        if (enabled){
            enabledForAny = true;
            break;
        }
    }

    if (!enabledForAny) return;

    const worker = cluster.fork({
        workerType: 'paymentProcessor',
        pools: JSON.stringify(poolConfigs)
    });

    worker.on('exit', function(code, signal){
        logger.error('Master', 'Payment Processor', `Payment processor died (code ${code}, signal ${signal}), spawning replacement...`);
        setTimeout(function(){
            startBlockUnlocker(poolConfigs);
        }, 2000);
    });
};

const startWebsite = function(){
    if (!portalConfig.website.enabled) return;

    const worker = cluster.fork({
        workerType: 'website',
        pools: JSON.stringify(poolConfigs),
        portalConfig: JSON.stringify(portalConfig)
    });
    worker.on('exit', function(code, signal){
        logger.error('Master', 'Website', `Website process died (code ${code}, signal ${signal}), spawning replacement...`);
        setTimeout(function(){
            startWebsite(portalConfig, poolConfigs);
        }, 2000);
    });
};

const startPoolApi = function(){
    let enabledForAny = false;
    for (const pool in poolConfigs){
        const p = poolConfigs[pool];
        const enabled = p.enabled && p.poolApi?.enabled;
        if (enabled){
            enabledForAny = true;
            break;
        }
    }

    if (!enabledForAny) return;

    const worker = cluster.fork({
        workerType: 'poolApi',
        pools: JSON.stringify(poolConfigs),
        portalConfig: JSON.stringify(portalConfig)
    });
    worker.on('exit', function(code, signal){
        logger.error('Master', 'PoolApi', `Pool API process died (code ${code}, signal ${signal}), spawning replacement...`);
        setTimeout(function(){
            startPoolApi(portalConfig, poolConfigs);
        }, 2000);
    });
};

(function init(){
    poolConfigs = buildPoolConfigs();

    spawnPoolWorkers();
    startBlockUnlocker();
    startPaymentProcessor();
    startWebsite();
    startPoolApi();
    startCliListener();
})();
