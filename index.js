#!/usr/bin/env node

'use strict';

const util = require('util');

const setImmediatePromise = util.promisify(setImmediate);

const program = require('commander');
// const winston = require('winston');
const moment = require('moment');

const Client = require('bitcoin-core');
const arangojs = require('arangojs');

const config = require('./config');

program
    .version('0.0.1')
    .option('-c, --clean', 'Clean database before import')
    .option('-v, --verbose', 'Increase verbosity', (v, total) => total + 1, 0)
    .option('-d, --debug', 'Increase verbosity of debug messages', (v, total) => total + 1, 0)
    .parse(process.argv);

process.on('exit', exitHandler('exit'));
process.on('SIGINT', exitHandler('int'));
process.on('uncaughtException', exitHandler('uncaughtException'));
process.on('unhandledRejection', exitHandler('unhandledRejection'));
process.on('rejectionHandled', exitHandler('rejectionHandled'));

class MyError extends Error {
    constructor(message, object) {
        super(message);
        this.object = object;

        let lines = this.stack.split('\n');
        lines.splice(0,1);
        this.stack = lines.join('\n');
    }

    toString() {
        return this.message;
    }
}

const arangoErrors = {
    ERROR_ARANGO_CONFLICT: { code: 1200 },
    ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED: { code: 1210 }
}

const logger = {
    debug: (...args) => logMessage('info', ...args),
    info: (...args) => logMessage('info', ...args),
    warning: (...args) => logMessage('warn', ...args),
    error: (...args) => logMessage('error', ...args)
};

const unhandledRejections = new Map();

const startTime = moment();
let numBlocks = 0;
let numTransactions = 0;
let numOutputs = 0;
let numAddresses = 0;

const client = new Client(config.bitcoinRPC);

const db = new arangojs.Database({
  url: `http://${config.database.username}:${config.database.password}@${config.database.host}:${config.database.port}`,
  databaseName: 'bitcoin'
});

(async function run() {
    if (program.clean) {
        logger.info('Cleaning database');
        await db.truncate();
        logger.info('Database cleaned');
    } else {
        await Promise.all([
            getOrCreateCollection('blocks'),
            getOrCreateCollection('transactions'),
            getOrCreateCollection('addresses')
        ]);

        let block;
        let blockHash;

        if ((await countDocuments('blocks')).count > 0) {
            const cursor = await db.query(
                'FOR b IN blocks ' +
                'SORT b.height DESC ' +
                'LIMIT 1 ' +
                'RETURN b'
            );
            block = await cursor.next();
            blockHash = block._key;
        } else {
            block = {height: 1};
            blockHash = await client.getBlockHash(1);
        }

        let bestBlock = await client.getBlock(await client.getBestBlockHash());
        let bestBlockHeight = bestBlock.height;
        let progress;

        logger.info(`Best block: ${bestBlockHeight}`);
        logger.info(`Last known best block: ${block.height}`);

        while (blockHash) {
            block = await client.getBlock(blockHash, 2);
            // if (Math.round(block.height / bestBlockHeight) !== progress) {
            //     progress = Math.round(block.height / bestBlockHeight);
            //     logger.info(`Progress: ${progress * 100}%`);
            // }
            await processBlock(block);
            blockHash = block.nextblockhash ? block.nextblockhash : null;
        }
    }
})();

// Bussiness logic

async function processBlock(block) {
    if (program.verbose <= 0) {
        logger.info(`Processing block #${block.height} containing ${block.tx.length} transactions`);
    }

    await Promise.all(block.tx.map((transaction) => processTransaction(transaction, block)));

    let document = {
        _key: block.hash,
        height: block.height,
        time: block.time,
        tx: block.tx.map((tx) => tx.txid)
    };

    try {
        await saveOrReplaceDocument('blocks', document);
    } catch (error) {
        throw new MyError(`Creating block failed: ${block.hash}: ${error.name}: ${error.messsage}`, document);
    }

    numBlocks++;

    if (program.verbose >= 4) {
        logger.info(`Done processing block #${block.height}`);
    }
}

async function processTransaction(transaction, block) {
    if (program.verbose >= 2) {
        logger.info(`Processing transaction: ${transaction.txid}`);
    }

    if (transaction.vin[0].hasOwnProperty('coinbase')) {
        if (transaction.vin.length !== 1) {
            logger.warning(`Coinbase transaction with multiple inputs: ${transaction}`);
        }
    }

    await Promise.all(transaction.vout.map((output) => processOutput(output, transaction, block)));

    const document = {
        _key: transaction.txid,
        blockhash: block.hash,
    };

    try {
        await saveOrReplaceDocument('transactions', document);
    } catch (error) {
        throw new MyError(`Creating transaction failed: ${transaction.txid}: ${error.name}: ${error.message}`, document);
    }

    numTransactions++;

    if (program.verbose >= 4) {
        logger.info(`Done processing transaction: ${transaction.txid}`);
    }
}

async function processOutput(output, transaction, block) {
    if (program.verbose >= 4) {
        logger.info(`Processing output #${output.n} in transaction ${transaction.txid}`);
    }

    if (!output.hasOwnProperty('scriptPubKey')) {
        throw new MyError(`No scriptPubKey in output #${output.n} in transaction ${transaction}`);
    }

    if (!output.scriptPubKey.hasOwnProperty('addresses')) {
        throw new MyError(`No addresses in output #${output.n} in transaction ${transaction}`);
    }

    if (output.scriptPubKey.addresses > 1) {
        logger.warning(`Unexpected number of addresses in output #${output.n} in transaction ${transaction}`);
    } else if (output.scriptPubKey.addresses < 1) {
        throw new MyError(`No addresses in output #${output.n}`);
    }

    await Promise.all(output.scriptPubKey.addresses.map((address) => processAddress(address, output, transaction, block)));

    numOutputs++;

    if (program.verbose >= 4) {
        logger.info(`Done processing output #${output.n} in transaction ${transaction.txid}`);
    }
}

async function processAddress(address, output, transaction, block) {
    if (program.verbose >= 3) {
        logger.info(`Processing address ${address} in output #${output.n} in transaction ${transaction.txid}`);
    }

    try {
        await db.transaction({read: ['addresses'], write: ['addresses']}, transactionStr, { address, output, transaction, block });
    } catch (error) {
        if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_CONFLICT.code) {
            logger.warning(`Creating address failed: ${address}: ${error.name}: ${error.message}. Retrying...`);
            await setImmediatePromise();
            try {
                await db.transaction({read: ['addresses'], write: ['addresses']}, transactionStr, { address, output, transaction, block });
            } catch (error) {
                throw new MyError(`Creating address failed: ${address}: ${error.name}: ${error.message}`, output);
            }
        } else {
            throw new MyError(`Creating address failed: ${address}: ${error.name}: ${error.message}`, output);
        }
    }

    numAddresses++;

    if (program.verbose >= 4) {
        logger.info(`Done processing address ${address} in output #${output.n} in transaction ${transaction.txid}`);
    }
}

const transactionStr = String(function (params) {
    const arangodb = require('@arangodb');
    const arangoErrors = arangodb.errors;
    const addresses = arangodb.db._collection('addresses');

    function saveAddress(addressesCol, txId, outputId, address, value) {
        return addressesCol.save({
            _key: address,
            [txId]: {
                [outputId]: value
            }
        });
    }

    function updateAddress(addressesCol, txId, outputId, address, value) {
        return addressesCol.update(address, {
            [txId]: {
                [outputId]: value
            }
        });
    }

    function saveOrUpdate(addressesCol, txId, outputId, address, value) {
        try {
            const document = addressesCol.document(address);
            if (!document.hasOwnProperty('transactions')
                    || !document.transactions.hasOwnProperty(txId)
                    || !document.transactions[txId].hasOwnProperty(outputId)
                    || document.transactions[txId][outputId] !== value) {

                    return updateAddress(addressesCol, txId, outputId, address, value);
            }
        } catch (error) {
            if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_DOCUMENT_NOT_FOUND.code) {
                return saveAddress(addressesCol, txId, outputId, address, value);
            } else {
                throw error;
            }
        }
    }

    return saveOrUpdate(addresses, params.transaction.txid, params.output.n, params.address, params.output.value);
});

// Database logic

async function getOrCreateCollection(collection) {
    const col = db.collection(collection);
    try {
        await col.get();
    } catch (error) {
        logger.info(`Creating collection: ${collection}`);
        try {
            col.create();
        } catch (error) {
            throw new MyError(`Creating collection failed: ${collection}: ${error.message}`);
        }
    }
}

function countDocuments(collection) {
    return db.collection(collection).count();
}

function getDocument(collection, docId) {
    return db.collection(collection).document(documentId);
}

async function saveOrReplaceDocument(collection, document) {
    try {
        return await saveDocument(collection, document);
    } catch (error) {
        if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED.code) {
            logger.warning(`Creating document failed: ${document._key}: ${error.name}: ${error.messsage}. Replacing...`, document);
            return await replaceDocument(collection, document._key, document);
        } else {
            throw error;
        }
    }
}

async function saveDocument(collection, document) {
    try {
        return await db.collection(collection).save(document);
    } catch (error) {
        if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_CONFLICT.code) {
            await setImmediatePromise();
            return await db.collection(collection).save(document);
        } else {
            throw error;
        }
    }
}

async function replaceDocument(collection, documentId, document) {
    try {
        return await db.collection(collection).replace(documentId, document);
    } catch (error) {
        if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_CONFLICT.code) {
            await setImmediatePromise();
            return await db.collection(collection).save(document);
        } else {
            throw error;
        }
    }
}

// Error handling



function exitHandler(error) {
    return function (...args) {
        if (program.debug >= 3) {
            logger.debug(`exitHandler(${error}, ${args})`);
        }

        if (error === 'unhandledRejection') {
            if (program.debug >= 2) {
                logger.debug(`unhandledRejection: ${args}`);
            }
            unhandledRejections.set(args[1], args[0]);
        } else if (error === 'rejectionHandled') {
            if (program.debug >= 2 ) {
                logger.debug(`rejectionHandled: ${args}`);
            }
            unhandledRejections.delete(args[0]);
        } else {
            let ret = 0;

            if (unhandledRejections.size > 0) {
                if (program.debug >= 2) {
                    logger.error('unhandledRejections:');
                }
                for (let [key, value] of unhandledRejections) {
                    logger.error(value);
                }
                ret = 1;
            }

            if (error === 'uncaughtException') {
                if (program.debug >= 2) {
                    logger.error('uncaughtException:');
                }
                logger.error(args[0]);
                ret = 1;
            }

            if (numBlocks + numTransactions + numOutputs > 0) {
                const duration = moment.duration(moment().diff(startTime));
                logger.info(`Processed ${numBlocks} blocks, ` +
                    `${numTransactions} transactions, ` +
                    `${numOutputs} outputs and ` +
                    `${numAddresses} addresses in ` +
                    `${moment.utc(duration.asMilliseconds()).format("HH:mm:ss")}`);
            }

            process.exit(ret);
        }
    }
}

// Logging

function logMessage(level, message, object) {
    let stack;

    if (typeof message !== 'string' && typeof message === 'object') {
        if (message instanceof Error) {
            if (object === undefined) {
                object = message.object;
            }

            stack = message.stack;
        }

        message = message.toString();
    }

    if (message) {
        console[level](message);
    }

    if (program.debug >= 1 && object) {
        console[level](JSON.stringify(object, null, 4));
    }

    if (program.debug >= 1 && stack) {
        console[level](stack);
    }
}
