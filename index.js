const log4js = require('log4js');
const MongoClient = require('mongodb').MongoClient;

const utils = require('./utils');

const logger = log4js.getLogger('mcrud');

logger.level = process.env.LOG_LEVEL || 'info';

module.exports.getCRUDMethods = (options = { url: 'mongodb://localhost:27017', database: 'mcrud', collection: '' }) => {
    const e = {};
    e.count = (filter) => {
        return new Promise((resolve, reject) => {
            try {
                if (!filter) {
                    filter = {}
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    const collection = client.db(options.database).collection(options.collection);
                    collection.find(filter).count((err2, doc) => {
                        if (err2) throw err2;
                        resolve(doc);
                        client.close();
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    e.get = (params = { filter: {}, sort: '', page: 1, count: 30 }) => {
        return new Promise((resolve, reject) => {
            try {
                if (!params) {
                    params = {};
                }
                if (!params.filter) {
                    params.filter = {};
                }
                if (!params.count) {
                    params.count = 30;
                }
                if (!params.page) {
                    params.page = 1;
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    const collection = client.db(options.database).collection(options.collection);
                    let query = collection.find(params.filter);
                    if (params.count != -1) {
                        const skip = (params.page - 1) * params.count;
                        query = query.limit(params.count).skip(skip);
                    }
                    if (params.sort) {
                        query = query.sort(getSortObject(params.sort));
                    }
                    query.toArray((err2, doc) => {
                        if (err2) throw err2;
                        resolve(doc);
                        client.close();
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    e.post = (data) => {
        return new Promise((resolve, reject) => {
            try {
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    const collection = client.db(options.database).collection(options.collection);
                    generateIdIfNot(data).then(newData => {
                        collection.insert(newData, (err2, doc) => {
                            if (err2) throw err2;
                            resolve(doc)
                            client.close();
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
    e.put = (filter, data) => {
        return new Promise((resolve, reject) => {
            try {
                if (!filter) {
                    filter = {};
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    const collection = client.db(options.database).collection(options.collection);
                    collection.update(filter, data, (err2, doc) => {
                        if (err2) throw err2;
                        resolve(doc)
                        client.close();
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    e.delete = (filter) => {
        return new Promise((resolve, reject) => {
            try {
                if (!filter) {
                    filter = {};
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    const collection = client.db(options.database).collection(options.collection);
                    collection.deleteMany(filter, (err2, doc) => {
                        if (err2) throw err2
                        resolve(doc)
                        client.close();
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    function generateIdIfNot(data) {
        return new Promise((resolve, reject) => {
            if (data._id) {
                resolve(data);
            } else {
                utils.getNextId(options).then(id => {
                    data._id = id;
                    resolve(data);
                }).catch(err => {
                    reject(err);
                });
            }
        });
    }
    function getSortObject(sort) {
        try {
            let temp = sort.split(',');
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
