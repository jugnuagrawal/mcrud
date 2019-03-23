const log4js = require('log4js');
const MongoClient = require('mongodb').MongoClient;
const renderId = require('render-id');

const utils = require('./utils');

const logger = log4js.getLogger('mcrud');

logger.level = process.env.LOG_LEVEL || 'info';

log4js.configure({
    appenders: { mcrud: { type: 'file', filename: path.join(process.cwd(), 'logs/mcrud.log') } },
    categories: { default: { appenders: ['mcrud'], level: logger.level } }
});

/**
 * @param {{url:string,database:string,collection:string,customId:boolean,idPattern:string}} options CRUD options
 */
module.exports.getCRUDMethods = (options) => {
    if (options.idPattern && options.idPattern.trim()) {
        options.customId = true;
    }
    const e = {};
    /**
     * @param {{key:value}} filter mongodb filter
     * @returns {Promise} Promise<any>
     */
    e.count = (filter) => {
        return new Promise((resolve, reject) => {
            try {
                if (!filter) {
                    filter = {}
                }
                if (typeof filter === 'string') {
                    filter = JSON.parse(filter);
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    collection.find(filter).count((err2, doc) => {
                        if (err2) throw err2;
                        logger.debug(doc + ' no of documents found in :', options.collection);
                        client.close();
                        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                        resolve(doc);
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    /**
     * @param {{filter:{key:value},sort:string,page:number,count:number,select:string}} params get record options
     * @returns {Promise} Promise<any>
     */
    e.get = (params) => {
        return new Promise((resolve, reject) => {
            try {
                if (!params) {
                    params = {};
                }
                if (!params.filter) {
                    params.filter = {};
                }
                if (typeof params.filter === 'string') {
                    params.filter = JSON.parse(params.filter);
                }
                if (!params.count) {
                    params.count = 30;
                }
                if (!params.page) {
                    params.page = 1;
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    logger.debug(JSON.stringify(params.filter) + ' filter applied in :', options.collection);
                    let query = collection.find(params.filter);
                    logger.debug(params.count + ' count applied in :', options.collection);
                    if (params.select) {
                        query = query.project(getAsObject(params.select));
                        logger.debug(params.select + ' select applied in :', options.collection);
                    }
                    if (params.count != -1) {
                        const skip = (params.page - 1) * params.count;
                        query = query.limit(params.count).skip(skip);
                        logger.debug(params.page + ' page applied in :', options.collection);
                    }
                    if (params.sort) {
                        query = query.sort(getAsObject(params.sort));
                        logger.debug(params.sort + ' sort applied in :', options.collection);
                    }
                    query.toArray((err2, doc) => {
                        if (err2) throw err2;
                        logger.debug(doc.length + ' results found in :', options.collection);
                        client.close();
                        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                        resolve(doc);
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    /**
     * @param {{key:value}} data data to be insterted as new record
     * @returns {Promise} Promise<any>
     */
    e.post = (data) => {
        return new Promise((resolve, reject) => {
            try {
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    generateIdIfRequired(data).then(newData => {
                        let method = 'insertOne';
                        if (Array.isArray(newData)) {
                            method = 'insertMany';
                        }
                        logger.debug('Using method ' + method + ' for :', options.collection);
                        collection[method](newData, (err2, doc) => {
                            if (err2) throw err2;
                            logger.debug(doc.insertedCount + ' document(s) inserted in :', options.collection);
                            client.close();
                            logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                            resolve(doc)
                        });
                    }).catch(err => {
                        reject(err);
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    /**
     * @param {{key:value}} filter mongodb filter
     * @param {{key:value}} data data to be insterted as new record
     * @returns {Promise} Promise<any>
     */
    e.put = (filter, data) => {
        return new Promise((resolve, reject) => {
            try {
                if (!filter) {
                    filter = {};
                }
                if (typeof filter === 'string') {
                    filter = JSON.parse(filter);
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    collection.updateMany(filter, { $set: data }, (err2, doc) => {
                        if (err2) throw err2;
                        logger.debug(doc.modifiedCount + ' document(s) updated in :', options.collection);
                        client.close();
                        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                        resolve(doc)
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    /**
     * @param {{key:value}} filter mongodb filter
     * @returns {Promise} Promise<any>
     */
    e.delete = (filter) => {
        return new Promise((resolve, reject) => {
            try {
                if (!filter) {
                    filter = {};
                }
                if (typeof filter === 'string') {
                    filter = JSON.parse(filter);
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    collection.deleteMany(filter, (err2, doc) => {
                        if (err2) throw err2
                        logger.debug(doc.deletedCount + ' document(s) deleted in :', options.collection);
                        client.close();
                        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                        resolve(doc)
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    /**
     * @param {object[]} query mongodb aggregate query
     * @returns {Promise} Promise<any>
     */
    e.aggregate = (query) => {
        return new Promise((resolve, reject) => {
            try {
                if (!query) {
                    query = [];
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    collection.aggregate(query).toArray((err2, doc) => {
                        if (err2) throw err2
                        logger.debug(doc.length + ' document(s) found in :', options.collection);
                        client.close();
                        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                        resolve(doc)
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    function generateIdIfRequired(data) {
        return new Promise((resolve, reject) => {
            if (Array.isArray(data)) {
                let pattern = options.collection.prefix(3).toUpperCase();
                if (options.idPattern) {
                    pattern = options.idPattern;
                }
                let idCounter = 0;
                utils.getNextCounter(options).then(next => {
                    data.forEach((item, i) => {
                        if (!item._id && options.customId) {
                            idCounter++;
                            item._id = renderId.render(pattern, next + 1);
                        }
                    });
                    utils.setNextCounter(options, next + idCounter).then(doc => {
                        resolve(data);
                    }).catch(err => {
                        reject(err);
                    });
                }).catch(err => {
                    reject(err);
                });
            } else {
                if (data._id || !options.customId) {
                    resolve(data);
                } else {
                    utils.getNextId(options).then(id => {
                        data._id = id;
                        resolve(data);
                    }).catch(err => {
                        reject(err);
                    });
                }
            }
        });
    }

    /**
     * 
     * @param {string} value
     * @returns {{key:number}}
     */
    function getAsObject(value) {
        try {
            let temp = value.split(',');
            temp = temp.map(e => {
                let key = e;
                if (key.startsWith('-')) {
                    key = key.substr(1, key.length);
                }
                return Object.defineProperty({}, key, {
                    value: e.startsWith('-') ? -1 : 1,
                    enumerable: true,
                    writable: true,
                    configurable: true
                });
            });
            temp = Object.assign.apply({}, temp);
            return temp;
        } catch (e) {
            logger.error(e);
            throw e;
        }
    }
    return e;
};
