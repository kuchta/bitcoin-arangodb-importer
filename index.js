#!/usr/bin/env node

'use strict';

const util = require('util');

const setImmediatePromise = util.promisify(setImmediate);

const commander = require('commander');
const moment = require('moment');

const Client = require('bitcoin-core');
const arangojs = require('arangojs');

const { MyError, arangoErrors } = require('./errors');
const config = require('./config');

commander
    .version('0.0.1')
    .option('-c, --clean', 'Clean database before import')
    .option('-v, --verbose', 'Increase verbosity', (v, total) => total + 1, 0)
    .option('-d, --debug', 'Increase verbosity of debug messages', (v, total) => total + 1, 3)
    .option('-r, --retry <n>', 'Number of retries in case of conflict', parseInt)
    .parse(process.argv);

const logger = require('./logger')(commander.verbose, commander.debug);

const unhandledRejections = new Map();

process.on('exit', handleExit);
// process.on('SIGINT', exitHandler('int'));
process.on('uncaughtException', handleExceptions('uncaughtException'));
process.on('unhandledRejection', handleExceptions('unhandledRejection'));
process.on('rejectionHandled', handleExceptions('rejectionHandled'));

const startTime = moment();
let numBlocks = 0;
let numTransactions = 0;
let numOutputs = 0;
let numAddresses = 0;

const client = new Client(config.bitcoinRPC);

const db = new arangojs.Database({
    url: `http://${config.database.username}:${config.database.password}@${config.database.host}:${config.database.port}`,
    databaseName: config.database.database
});

const BLOCKS = {
    name: 'blocks',
    handle: db.collection('blocks'),
    entity: 'block',
};

const TRANSACTIONS = {
    name: 'transactions',
    handle: db.collection('transactions'),
    entity: 'transaction',
};

const ADDRESSES = {
    name: 'addresses',
    handle: db.collection('addresses'),
    entity: 'address'
};

(async function run() {
    if (commander.clean) {
        logger.info('Cleaning database');
        await db.truncate();
        logger.info('Database cleaned');
    } else {
        await Promise.all([
            getOrCreateCollection(BLOCKS),
            getOrCreateCollection(TRANSACTIONS),
            getOrCreateCollection(ADDRESSES)
        ]);

        let block;
        let blockHash;

        if ((await countDocuments(BLOCKS)).count > 0) {
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
            if (Math.round(block.height / bestBlockHeight) !== progress) {
                progress = Math.round(block.height / bestBlockHeight);
                logger.info(`Progress: ${progress * 100}%`);
            }
            await processBlock(block);
            blockHash = block.nextblockhash ? block.nextblockhash : null;
        }
    }
})();

// Bussiness logic

async function processBlock(block) {
    logger.info1(`Processing block #${block.height} containing ${block.tx.length} transactions`);

    await Promise.all(block.tx.map((transaction) => processTransaction(transaction, block)));

    let document = {
        _key: block.hash,
        height: block.height,
        time: block.time,
        tx: block.tx.map((tx) => tx.txid)
    };

    await saveOrReplaceDocument(BLOCKS, document);

    numBlocks++;

    logger.info4(`Done processing block #${block.height}`);
}

async function processTransaction(transaction, block) {
    logger.info2(`Processing transaction: ${transaction.txid}`);

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

    await saveOrReplaceDocument(TRANSACTIONS, document);

    numTransactions++;

    logger.info4(`Done processing transaction: ${transaction.txid}`);
}

async function processOutput(output, transaction, block) {
    logger.info4(`Processing output #${output.n} in transaction ${transaction.txid}`);

    if (!output.hasOwnProperty('scriptPubKey')) {
        throw new MyError(`No scriptPubKey in output #${output.n} in transaction ${transaction.txid}`, transaction);
    }

    if (!output.scriptPubKey.hasOwnProperty('addresses')) {
        logger.warning(`No addresses in output #${output.n} in transaction ${transaction.txid}`, transaction);
        return;
    }

    if (output.scriptPubKey.addresses.length > 1) {
        logger.warning(`Unexpected number of addresses in output #${output.n} in transaction ${transaction.txid}`, transaction);
    } else if (output.scriptPubKey.addresses < 1) {
        logger.warning(`No addresses in output #${output.n} in transaction ${transaction.txid}`, transaction);
    }

    await Promise.all(output.scriptPubKey.addresses.map((address) => processAddress(address, output, transaction, block)));

    numOutputs++;

    logger.info4(`Done processing output #${output.n} in transaction ${transaction.txid}`);
}

async function processAddress(address, output, transaction, block) {
    logger.info3(`Processing address ${address} in output #${output.n} in transaction ${transaction.txid}`);

    try {
        let params = { address, value: output.value, output: output.n, transactionId: transaction.txid };
        // console.log(transactionStr);
        // console.log(params);
        await db.transaction({read: ['addresses'], write: ['addresses']}, transactionStr, params);
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

    logger.info4(`Done processing address ${address} in output #${output.n} in transaction ${transaction.txid}`);
}

const transactionStr = `
function run({address, value, output, transactionId }) {
${MyError}
${saveOrUpdateDocumentSync}
${saveDocumentSync}
${updateDocumentSync}
${modifyCollectionSync}
const arangodb = require('@arangodb');
const arangoErrors = arangodb.errors;
const logger = console;
logger.warning = console.warn;
const ADDRESSES = {
    name: 'addresses',
    handle: arangodb.db._collection('addresses'),
    entity: 'address'
}

saveOrUpdateDocumentSync(ADDRESSES, {
    _key: address,
    [transactionId]: {
        [output]: value
    }
});
}`

// Database logic

async function getOrCreateCollection(collection) {
    try {
        await collection.handle.get();
    } catch (error) {
        logger.info(`Creating collection: ${collection.name}`);
        try {
            collection.handle.create();
        } catch (error) {
            throw new MyError(`Creating collection failed: ${collection.name}: ${error.message}`);
        }
    }
}

async function saveOrReplaceDocument(collection, document) {
    try {
        return await saveDocument(collection, document);
    } catch (error) {
        if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED.code) {
            logger.warning(`Saving ${collection.entity} failed: ${document._key}: ${error.name}: ${error.messsage}. Replacing...`, document);
            try {
                return await replaceDocument(collection, document._key, document);
            } catch (error) {
                throw new MyError(`Replacing ${collection.entity} failed: ${document._key}: ${error.name}: ${error.messsage}`, document);
            }
        } else {
            throw new MyError(`Saving ${collection.entity} failed: ${document._key}: ${error.name}: ${error.messsage}`, document);
        }
    }
}

async function saveOrUpdateDocument(collection, document) {
    try {
        return await saveDocument(collection, document);
    } catch (error) {
        if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED.code) {
            logger.warning(`Saving ${collection.entity} failed: ${document._key}: ${error.name}: ${error.messsage}. Updating...`, document);
            try {
                return await updateDocument(collection, document._key, document);
            } catch (error) {
                throw new MyError(`Updating ${collection.entity} failed: ${document._key}: ${error.name}: ${error.messsage}`, document);
            }
        } else {
            throw new MyError(`Saving ${collection.entity} failed: ${document._key}: ${error.name}: ${error.messsage}`, document);
        }
    }
}

function saveOrUpdateDocumentSync(collection, document) {
    try {
        return saveDocumentSync(collection, document);
    } catch (error) {
        if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED.code) {
            try {
                return updateDocumentSync(collection, document._key, document);
            } catch (error) {
                throw new MyError(`Updating ${collection.entity} failed: ${document._key}: ${error.name}: ${error.messsage}`, document);
            }
        } else {
            throw new MyError(`Saving ${collection.entity} failed: ${document._key}: ${error.name}: ${error.messsage}`, document);
        }
    }
}

function countDocuments(collection) {
    return collection.handle.count();
}

function getDocument(collection, docId) {
    return collection.handle.document(documentId);
}

function saveDocument(collection, document) {
    return modifyCollection(collection, 'save', commander.retries, document);
}

function saveDocumentSync(collection, document) {
    return modifyCollectionSync(collection, 'save', 3, document);
}

function replaceDocument(collection, documentId, document) {
    return modifyCollection(collection, 'replace', commander.retries, documentId, document);
}

function replaceDocumentSync(collection, documentId, document) {
    return modifyCollection(collection, 'replace', 3, documentId, document);
}

function updateDocument(collection, documentId, document) {
    return modifyCollection(collection, 'update', commander.retries, documentId, document);
}

function updateDocumentSync(collection, documentId, document) {
    return modifyCollectionSync(collection, 'update', 3, documentId, document);
}

async function modifyCollection(collection, operation, retries, ...args) {
    // console.log(`modifyCollection(collection=${collection}, operation=${operation}, args=${args}`);
    try {
        return await collection.handle[operation](...args);
    } catch (error) {
        if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_CONFLICT.code && retries > 0) {
            await setImmediatePromise();
            return await modifyCollection(collection, operation, retries-1, ...args);
        } else {
            throw error;
        }
    }
}

function modifyCollectionSync(collection, operation, retries=1, ...args) {
    try {
        return collection.handle[operation](...args);
    } catch (error) {
        if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_CONFLICT.code && retries <= 3) {
            setTimeout(() => modifyCollectionSync(collection, operation, retries+1, ...args), 0);
        } else {
            throw error;
        }
    }
}

// Error handling

function handleExceptions(type) {
    if (type === 'uncaughtException') {
        return (error) => {
            logger.error(`uncaughtException: ${error}`, error);
            process.exit(1);
        }
    } else if (type == 'unhandledRejection') {
        return (error, promise) => {
            unhandledRejections.set(promise, error);
        }
    } else if (type == 'rejectionHandled') {
        return (promise) => {
            unhandledRejections.delete(args[0]);
        }
    }
}

function handleExit(code) {
    if (unhandledRejections.size > 0) {
        for (let [key, value] of unhandledRejections) {
            logger.error(`unhandledRejection: ${value}`, value);
        }
        if (code === 0) {
            process.exit(1);
        }
    }

    if (numBlocks + numTransactions + numOutputs > 0) {
        const duration = moment.duration(moment().diff(startTime));
        logger.info(`Processed ${numBlocks} blocks, ` +
            `${numTransactions} transactions, ` +
            `${numOutputs} outputs and ` +
            `${numAddresses} addresses in ` +
            `${moment.utc(duration.asMilliseconds()).format("HH:mm:ss")}`);
    }
}
