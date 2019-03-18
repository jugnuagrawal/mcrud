const log4js = require('log4js');
const MongoClient = require('mongodb').MongoClient;

const logger = log4js.getLogger('mcrud');

logger.level = process.env.LOG_LEVEL || 'info';

function generateId(name, length, next) {
    let prefix = name.split('').slice(0, 3).join('').toUpperCase();
    let nextLen = 0;
    let tempNext = next;
    while (tempNext > 0) {
        tempNext = Math.floor(tempNext / 10);
        nextLen += 1;
    }
    for (let i = 0; i < length - nextLen; i++) {
        prefix += '0';
    }
    return prefix + next;
}

module.exports.getNextId = (options) => {
    return new Promise((resolve, reject) => {
        try {
            MongoClient.connect(options.url, (err1, client) => {
                if (err1) throw err1;
                const collection = client.db(options.database).collection('counter');
                collection.findOne({ _id: options.collection }, (err2, doc) => {
                    if (err2) throw err2;
                    if (!doc) {
                        collection.insert({ _id: options.collection, next: 2 }, (err3, doc2) => {
                            if (err3) throw err3;
                            const id = generateId(options.collection, 8, 1);
                            resolve(id);
                        });
                    } else {
                        const id = generateId(options.collection, 8, doc.next);
                        const nextVal = parseInt(doc.next + '') + 1;
                        collection.findOneAndUpdate({ _id: options.collection }, { $set: { next: nextVal } }, (err3, doc2) => {
                            if (err3) throw err3;
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