const colors = require('colors');

const severityToColor = function(severity, text) {
    switch(severity) {
        case 'special':
            return text.cyan.underline;
        case 'debug':
            return text.green;
        case 'warning':
            return text.yellow;
        case 'error':
            return text.red;
        default:
            console.log("Unknown severity " + severity);
            return text.italic;
    }
};

const severityValues = {
    'debug': 1,
    'warning': 2,
    'error': 3,
    'special': 4
};


const PoolLogger = function (configuration) {
    const logLevelInt = severityValues[configuration.logLevel];
    const logColors = configuration.logColors;

    const log = function(severity, system, component, text, subcat) {
        if (severityValues[severity] < logLevelInt) return;

        if (subcat){
            const realText = subcat;
            const realSubCat = text;
            text = realText;
            subcat = realSubCat;
        }

        const prettyDate = new Date().toISOString()
            .replace(/T/, ' ')
            .replace(/\..+/, '')
        let entryDesc = prettyDate + ' [' + system + ']\t';
        let logString;
        if (logColors) {
            entryDesc = severityToColor(severity, entryDesc);

            logString =
                    entryDesc +
                    ('[' + component + '] ').italic;

            if (subcat) logString += ('(' + subcat + ') ').bold.grey;

            logString += text.grey;
        } else {
            logString =
                    entryDesc +
                    '[' + component + '] ';

            if (subcat)
                logString += '(' + subcat + ') ';

            logString += text;
        }

        console.log(logString);
    };

    // public
    const _this = this;
    Object.keys(severityValues).forEach(function(logType){
        _this[logType] = function(){
            const args = Array.prototype.slice.call(arguments, 0);
            args.unshift(logType);
            log.apply(this, args);
        };
    });
};

module.exports = PoolLogger;
