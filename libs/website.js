const fs = require('fs');
const path = require('path');
const async = require('async');
const watch = require('node-watch');
const Redis = require('ioredis');

const dot = require('dot');
const express = require('express');
const bodyParser = require('body-parser');
const compress = require('compression');

const Stratum = require('stratum-pool');
const util = require('stratum-pool/lib/util.js');

const api = require('./api.js');

module.exports = function(logger){
    dot.templateSettings.strip = false;

    const portalConfig = JSON.parse(process.env.portalConfig);
    const poolConfigs = JSON.parse(process.env.pools);

    const websiteConfig = portalConfig.website;

    const portalApi = new api(logger, portalConfig, poolConfigs);
    const portalStats = portalApi.stats;

    const logSystem = 'Website';


    const pageFiles = {
        'index.html': 'index',
        'home.html': '',
        'getting_started.html': 'getting_started',
        'stats.html': 'stats',
        'tbs.html': 'tbs',
        'workers.html': 'workers',
        'api.html': 'api',
        'admin.html': 'admin',
        'mining_key.html': 'mining_key'
    };

    let pageTemplates = {};

    let pageProcessed = {};
    let indexesProcessed = {};

    let keyScriptTemplate = '';
    let keyScriptProcessed = '';

    const processTemplates = function(){
        for (const pageName in pageTemplates){
            if (pageName === 'index') continue;
            pageProcessed[pageName] = pageTemplates[pageName]({
                poolsConfigs: poolConfigs,
                stats: portalStats.stats,
                portalConfig: portalConfig
            });
            indexesProcessed[pageName] = pageTemplates.index({
                page: pageProcessed[pageName],
                selected: pageName,
                stats: portalStats.stats,
                poolConfigs: poolConfigs,
                portalConfig: portalConfig
            });
        }

        //logger.debug(logSystem, 'Stats', 'Website updated to latest stats');
    };

    const readPageFiles = function(files){
        async.each(files, function(fileName, callback){
            const filePath = 'website/' + (fileName === 'index.html' ? '' : 'pages/') + fileName;
            fs.readFile(filePath, 'utf8', function(err, data){
                pageTemplates[pageFiles[fileName]] = dot.template(data);
                callback();
            });
        }, function(err){
            if (err){
                console.log('error reading files for creating dot templates: '+ JSON.stringify(err));
                return;
            }
            processTemplates();
        });
    };

    //If html file was changed, reload it
    watch('website', function(evt, filename){
        const basename = path.basename(filename);
        if (basename in pageFiles){
            console.log(filename);
            readPageFiles([basename]);
            logger.debug(logSystem, 'Server', 'Reloaded file ' + basename);
        }
    });

    portalStats.getGlobalStats(function(){
        readPageFiles(Object.keys(pageFiles));
    });

    const buildUpdatedWebsite = function(){
        portalStats.getGlobalStats(function(){
            processTemplates();

            const statData = 'data: ' + JSON.stringify(portalStats.stats) + '\n\n';
            for (const uid in portalApi.liveStatConnections){
                const res = portalApi.liveStatConnections[uid];
                res.write(statData);
            }

        });
    };

    setInterval(buildUpdatedWebsite, websiteConfig.stats.updateInterval * 1000);

    const buildKeyScriptPage = function(){
        async.waterfall([
            function(callback){
                const redisConfig = portalConfig.redis;
                const redisDB = (redisConfig.db && redisConfig.db > 0) ? redisConfig.db : 0;
                const client = Redis.createClient(redisConfig.port, redisConfig.host, {
                    db: redisDB,
                    auth_pass: redisConfig.auth
                });

                client.hgetall('coinVersionBytes', function(err, coinBytes){
                    if (err){
                        client.quit();
                        return callback('Failed grabbing coin version bytes from redis ' + JSON.stringify(err));
                    }
                    callback(null, client, coinBytes || {});
                });
            },
            function (client, coinBytes, callback){
                const enabledCoins = Object.keys(poolConfigs).map(function(c){return c.toLowerCase()});
                let missingCoins = [];
                enabledCoins.forEach(function(c){
                    if (!(c in coinBytes))
                        missingCoins.push(c);
                });
                callback(null, client, coinBytes, missingCoins);
            },
            function(client, coinBytes, missingCoins, callback){
                let coinsForRedis = {};
                async.each(missingCoins, function(c, cback){
                    const coinInfo = (function(){
                        for (const pName in poolConfigs){
                            if (pName.toLowerCase() === c)
                                return {
                                    daemon: poolConfigs[pName].daemons[0],
                                    address: poolConfigs[pName].address
                                }
                        }
                    })();
                    const daemon = new Stratum.daemon.interface([coinInfo.daemon], function(severity, message){
                        logger[severity](logSystem, c, message);
                    });
                    daemon.cmd('dumpprivkey', [coinInfo.address], function(result){
                        if (result[0].error){
                            logger.error(logSystem, c, 'Could not dumpprivkey for ' + c + ' ' + JSON.stringify(result[0].error));
                            cback();
                            return;
                        }

                        const vBytePub = util.getVersionByte(coinInfo.address)[0];
                        const vBytePriv = util.getVersionByte(result[0].response)[0];

                        coinBytes[c] = vBytePub.toString() + ',' + vBytePriv.toString();
                        coinsForRedis[c] = coinBytes[c];
                        cback();
                    });
                }, function(err){
                    callback(null, client, coinBytes, coinsForRedis);
                });
            },
            function(client, coinBytes, coinsForRedis, callback){
                if (Object.keys(coinsForRedis).length > 0){
                    client.hmset('coinVersionBytes', coinsForRedis, function(err){
                        if (err)
                            logger.error(logSystem, 'Init', 'Failed inserting coin byte version into redis ' + JSON.stringify(err));
                        client.quit();
                    });
                } else {
                    client.quit();
                }
                callback(null, coinBytes);
            }
        ], function(err, coinBytes){
            if (err){
                logger.error(logSystem, 'Init', err);
                return;
            }
            try{
                keyScriptTemplate = dot.template(fs.readFileSync('website/key.html', {encoding: 'utf8'}));
                keyScriptProcessed = keyScriptTemplate({coins: coinBytes});
            }
            catch(e){
                logger.error(logSystem, 'Init', 'Failed to read key.html file');
            }
        });

    };
    buildKeyScriptPage();

    const getPage = function(pageId){
        if (pageId in pageProcessed) {
            return pageProcessed[pageId];
        }
    };

    const route = function(req, res, next){
        const pageId = req.params.page || '';
        if (pageId in indexesProcessed){
            res.header('Content-Type', 'text/html');
            res.end(indexesProcessed[pageId]);
        } else {
            next();
        }
    };

    const app = express();

    app.use(bodyParser.json());

    app.get('/get_page', function(req, res, next){
        const requestedPage = getPage(req.query.id);
        if (requestedPage){
            res.end(requestedPage);
            return;
        }
        next();
    });

    app.get('/key.html', function(req, res, next){
        res.end(keyScriptProcessed);
    });

    app.get('/:page', route);
    app.get('/', route);

    app.get('/api/:method', function(req, res, next){
        portalApi.handleApiRequest(req, res, next);
    });

    app.post('/api/admin/:method', function(req, res, next){
        if (portalConfig.website
            && portalConfig.website.adminCenter
            && portalConfig.website.adminCenter.enabled){
            if (portalConfig.website.adminCenter.password === req.body.password) {
                portalApi.handleAdminApiRequest(req, res, next);
            }
            else {
                res.send(401, JSON.stringify({error: 'Incorrect Password'}));
            }
        } else {
            next();
        }
    });

    app.use(compress());
    app.use('/static', express.static('website/static'));

    app.use(function(err, req, res, next){
        console.error(err.stack);
        res.send(500, 'Something broke!');
    });

    try {
        app.listen(portalConfig.website.port, portalConfig.website.host, function () {
            logger.debug(logSystem, 'Server', 'Website started on ' + portalConfig.website.host + ':' + portalConfig.website.port);
        });
    }
    catch(e){
        logger.error(logSystem, 'Server', 'Could not start website on ' + portalConfig.website.host + ':' + portalConfig.website.port
            +  ' - its either in use or you do not have permission');
    }


};
