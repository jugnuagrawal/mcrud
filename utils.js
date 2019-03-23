const path = require('path');
const log4js = require('log4js');
const MongoClient = require('mongodb').MongoClient;
const renderId = require('render-id');

const logger = log4js.getLogger('mcrud');

logger.level = process.env.LOG_LEVEL || 'info';

log4js.configure({
    appenders: { mcrud: { type: 'file', filename: path.join(process.cwd(), 'logs/mcrud.log') } },
    categories: { default: { appenders: ['mcrud'], level: logger.level } }
});

String.prototype.prefix = function (length) {
    return this.substr(0, length);
}

const e = {};

/**
 * @param {{url:string,database:string,collection:string,customId:boolean,idPattern:string}} options CRUD options
 * @returns {Promise}
 */
e.getNextId = (options) => {
    return new Promise((resolve, reject) => {
        try {
            MongoClient.connect(options.url, (err1, client) => {
                if (err1) throw err1;
                logger.debug('Connected to :', options.url);
                let pattern = options.collection.prefix(3).toUpperCase();
                if (options.idPattern) {
                    pattern = options.idPattern;
                }
                const collection = client.db(options.database).collection('counter');
                logger.debug('Using db :', options.database);
                collection.findOne({ _id: options.collection }, (err2, doc) => {
                    if (err2) throw err2;
                    if (!doc) {
                        logger.debug('Counter not found for collection :', options.collection);
                        collection.insertOne({ _id: options.collection, next: 2 }, (err3, doc2) => {
                            if (err3) throw err3;
                            logger.debug('Counter created for collection :', options.collection);
                            const id = renderId.render(pattern, 1);
                            client.close();
                            logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                            resolve(id);
                        });
                    } else {
                        const id = renderId.render(pattern, doc.next);
                        const nextVal = parseInt(doc.next + '') + 1;
                        logger.debug('Counter found for collection :', options.collection);
                        collection.findOneAndUpdate({ _id: options.collection }, { $set: { next: nextVal } }, (err3, doc2) => {
                            if (err3) throw err3;
                            logger.debug('Counter updated for collection :', options.collection);
                            client.close();
                            logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                            resolve(id);
                        });
                    }
                });
            });
        } catch (e) {
            logger.error(e);
            reject(e);
        }
    });
};


/**
 * @param {{url:string,database:string,collection:string,customId:boolean,idPattern:string}} options CRUD options
 * @returns {Promise}
 */
e.getNextCounter = (options) => {
    return new Promise((resolve, reject) => {
        try {
            MongoClient.connect(options.url, (err1, client) => {
                if (err1) throw err1;
                logger.debug('Connected to :', options.url);
                const collection = client.db(options.database).collection('counter');
                logger.debug('Using db :', options.database);
                collection.findOne({ _id: options.collection }, (err2, doc) => {
                    if (err2) throw err2;
                    client.close();
                    logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                    if (!doc) {
                        logger.debug('Counter not found for collection :', options.collection);
                        resolve(1);
                    } else {
                        logger.debug('Counter found for collection :', options.collection);
                        resolve(doc.next);
                    }
                });
            });
        } catch (e) {
            logger.error(e);
            reject(e);
        }
    });
};

/**
 * @param {{url:string,database:string,collection:string,customId:boolean,idPattern:string}} options CRUD options
 * @param {number} counter The next counter value to be stored
 * @returns {Promise}
 */
e.setNextCounter = (options, counter) => {
    return new Promise((resolve, reject) => {
        try {
            MongoClient.connect(options.url, (err1, client) => {
                if (err1) throw err1;
                logger.debug('Connected to :', options.url);
                const collection = client.db(options.database).collection('counter');
                logger.debug('Using db :', options.database);
                collection.findOneAndUpdate({ _id: options.collection }, { $set: { next: counter } }, (err3, doc2) => {
                    if (err3) throw err3;
                    logger.debug('Counter updated for collection :', options.collection);
                    client.close();
                    logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                    resolve(doc2);
                });
            });
        } catch (e) {
            logger.error(e);
            reject(e);
        }
    });
};

module.exports = e;