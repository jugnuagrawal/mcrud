const log4js = require('log4js');
const MongoClient = require('mongodb').MongoClient;
const renderId = require('render-id');

const logger = log4js.getLogger('mcrud');

logger.level = process.env.LOG_LEVEL || 'info';

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
                let pattern = options.collection.prefix(3).toUpperCase();
                if (options.idPattern) {
                    pattern = options.idPattern;
                }
                const collection = client.db(options.database).collection('counter');
                collection.findOne({ _id: options.collection }, (err2, doc) => {
                    if (err2) throw err2;
                    if (!doc) {
                        collection.insert({ _id: options.collection, next: 2 }, (err3, doc2) => {
                            if (err3) throw err3;
                            const id = renderId.render(pattern, 1);
                            client.close();
                            resolve(id);
                        });
                    } else {
                        const id = renderId.render(pattern, doc.next);
                        const nextVal = parseInt(doc.next + '') + 1;
                        collection.findOneAndUpdate({ _id: options.collection }, { $set: { next: nextVal } }, (err3, doc2) => {
                            if (err3) throw err3;
                            client.close();
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
                const collection = client.db(options.database).collection('counter');
                collection.findOne({ _id: options.collection }, (err2, doc) => {
                    if (err2) throw err2;
                    client.close();
                    if (!doc) {
                        resolve(1);
                    } else {
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
                let pattern = options.collection.prefix(3).toUpperCase();
                if (options.idPattern) {
                    pattern = options.idPattern;
                }
                const collection = client.db(options.database).collection('counter');
                collection.findOneAndUpdate({ _id: options.collection }, { $set: { next: counter } }, (err3, doc2) => {
                    if (err3) throw err3;
                    client.close();
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