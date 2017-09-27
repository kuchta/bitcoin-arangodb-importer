#!/usr/bin/env node

'use strict';

const EventEmitter = require('events');
const util = require('util');

const setImmediatePromise = util.promisify(setImmediate);

const commander = require('commander');
const moment = require('moment');
const _ = require('lodash');

const Client = require('bitcoin-core');
const {Database, aql} = require('arangojs');

const { MyError, arangoErrors } = require('./errors');
const config = require('./config');

commander
	.version('0.0.1')
	.option('-c, --clean', 'Clean database before import')
	.option('-p, --perf', 'Report performance')
	.option('-a, --async', 'Process transactions asynchronously')
	.option('-o, --dont-overwrite', 'Don\'t overwrite existing entries')
	.option('-r, --retries <n>', 'Number of retries in case of conflict', 3)
	.option('-v, --verbose', 'Increase verbosity', (v, total) => total + 1, 0)
	.option('-d, --debug', 'Increase verbosity of debug messages', (v, total) => total + 1, 0)
	.parse(process.argv);

const logger = require('./logger')(commander.verbose, commander.debug);

const unhandledRejections = new Map();

const startTime = moment();
let bestBlockHeight;
let progress = 0;
let blocksCounter = 0;
let blocksLapTime = moment();
let transactionsCounter = 0;
let transactionsLapTime = moment();
let outputsCounter = 0;
let stop = false;

process.on('exit', handleExit);
process.on('SIGINT', handleInt);
process.on('uncaughtException', handleExceptions('uncaughtException'));
process.on('unhandledRejection', handleExceptions('unhandledRejection'));
process.on('rejectionHandled', handleExceptions('rejectionHandled'));

const client = new Client(config.bitcoinRPC);

const db = new Database({
	url: `http://${config.database.username}:${config.database.password}@${config.database.host}:${config.database.port}`,
	databaseName: config.database.database
});

const BLOCKS = {
	name: 'blocks',
	entity: 'block',
	handle: db.collection('blocks'),
	get: 'document'
};

const TRANSACTIONS = {
	name: 'transactions',
	entity: 'transaction',
	handle: db.collection('transactions'),
	get: 'document'
};

const OUTPUTS = {
	name: 'outputs',
	entity: 'output',
	handle: db.collection('outputs'),
	get: 'document'
};

const ADDRESSES = {
	name: 'addresses',
	entity: 'address',
	handle: db.collection('addresses'),
	get: 'document'
};

const OUTPUTS_SPENT_IN	 = {
	name: 'outputsSpentIn',
	entity: 'outputSpentIn',
	handle: db.edgeCollection('outputsSpentIn'),
	get: 'edge'
};

const GRAPH = {
	name: 'graph',
	handle: db.graph('graph'),
	properties: {
		edgeDefinitions: [{
			collection: OUTPUTS_SPENT_IN.name,
			from: [OUTPUTS.name],
			to: [TRANSACTIONS.name]
		}]
	}
};

(async function run () {
	if (commander.clean) {
		logger.info('Cleaning database');
		await db.truncate();
		logger.info('Database cleaned');
	} else {
		await Promise.all([
			getOrCreateCollection(BLOCKS),
			getOrCreateCollection(TRANSACTIONS),
			getOrCreateCollection(OUTPUTS),
			// saveDocument(OUTPUTS, { _key: 'coinbase'} ),
			getOrCreateCollection(ADDRESSES),
			getOrCreateCollection(OUTPUTS_SPENT_IN)
		]);
		await getOrCreateGraph(GRAPH)

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

		const bestBlock = await client.getBlock(await client.getBestBlockHash());
		bestBlockHeight = bestBlock.height;

		logger.info(`Best block: ${bestBlockHeight}`);
		logger.info(`Last known best block: ${block.height}`);

		while (!stop && blockHash) {
			block = await client.getBlock(blockHash, 2);
			await processBlock(block);
			blockHash = block.nextblockhash ? block.nextblockhash : null;
		}
	}
})();

// Bussiness logic

async function processBlock (block) {
	logger.info1(`Processing block #${block.height} containing ${block.tx.length} transactions`);

	await map(processTransaction, block.tx, block);

	await saveBlock(block.hash, block.height, block.time, block.tx.map((tx) => tx.txid));

	logger.info4(`Done processing block #${block.height}`);
}

async function processTransaction (transaction, block, index) {
	logger.info2(`Processing transaction: ${transaction.txid}`);

	const inputsValues = await map(processInput, transaction.vin, transaction, block);
	// const inputsValue = inputsValues.reduce((total, value) => total + Math.round(1e8 * value));
	const outputsValues = await map(processOutput, transaction.vout, transaction, block);
	// const outputsValue = outputsValues.reduce((total, value) => total + Math.round(1e8 * value));

	// logger.debug1(`inputsValue: ${inputsValues}`);
	// logger.debug1(`outputsValue: ${outputsValues}`);

	// if (outputsValue > inputsValue) {
	// 	logger.warning(`Transaction "${transaction.txid} is spending more (${outputsValue}) than is the sum of it's inputs (${inputsValue})`);
	// }

	await saveTransaction(transaction.txid, block.hash);

	logger.info4(`Done processing transaction: ${transaction.txid}`);
}

async function processInput (input, transaction, block, index) {
	logger.info3(`Processing input in transaction ${transaction.txid}`);

	let output;
	let outputId;
	if (input.hasOwnProperty('coinbase')) {
		if (index > 0) {
			logger.warning(`Coinbase transaction in block "${block.hash}" not first transaction`);
		}
		outputId = `${transaction.txid}:coinbase`;
		output = await saveOutput(transaction.txid, 'coinbase', getBlockSubsidy(block.height));
	} else {
		outputId = `${input.txid}:${input.vout}`;
		try {
			output = await getDocument(OUTPUTS, outputId);
		} catch (error) {
			// logger.debug1(`processInput getDocument(${outputId}) failed with error: ${error.code}`, {error});
			if (error.code === arangoErrors.ERROR_ARANGO_DOCUMENT_NOT_FOUND.code ||
					error.code === arangoErrors.ERROR_HTTP_NOT_FOUND.code) {
				logger.warning(`Can't find output "${outputId}" referenced in input #${index} of transaction "${transaction.txid}"`);
				return 0;
			} else {
				throw error;
			}
		}
	}

	await saveOutputSpentIn(outputId, transaction.txid);

	return output.value;

	logger.info4(`Done processing input in transaction ${transaction.txid}`);
}

async function processOutput (output, transaction, block, index) {
	logger.info3(`Processing output #${output.n} in transaction ${transaction.txid}`);

	if (!output.hasOwnProperty('scriptPubKey')) {
		throw new MyError(`No scriptPubKey in output #${output.n} in transaction "${transaction.txid}"`, {object: output});
	}

	if (output.scriptPubKey.hasOwnProperty('addresses') && output.scriptPubKey.addresses.length >= 1) {
		map(saveAddress, output.scriptPubKey.addresses, transaction.txid, output.n);
	} else {
		logger.warning(`No addresses in output #${output.n} in transaction "${transaction.txid}"`, {object: output});
	}

	await saveOutput(transaction.txid, output.n, output.value);

	return output.value;

	logger.info4(`Done processing output #${output.n} in transaction ${transaction.txid}`);
}

// Utility functions

function getBlockSubsidy(height, subsidy=50) {
	if (height > 210000) {
		return getBlockSubsidy(height - 210000, subsidy/2);
	} else {
		return subsidy;
	}
}

async function map (func, list, ...args) {
	let ret;
	if (commander.async) {
		ret = await Promise.all(list.map((value, index) => func(value, ...args, index)));
	} else {
		ret = []
		for (let index = 0; index < list.length; index++) {
			ret.push(await func(list[index], ...args), index);
		}
	}
	return ret;
}

async function saveBlock(id, height, time, tx) {
	const document = {
		_key: id,
		height: height,
		time: time,
		tx: tx
	};

	await saveDocument(BLOCKS, document);

	blocksCounter++;

	const newProgress = Math.round((height / bestBlockHeight) * 100)
	if (newProgress > progress) {
		logger.info(`Progress: ${height}/${bestBlockHeight} (${Math.round((height / bestBlockHeight) * 100)}%)`);
		if (unhandledRejections.size > 0) {
			logger.info(`Number of unhandled rejections: ${unhandledRejections.size}`);
		}
		progress = newProgress;
	}

	return document;
}

async function saveTransaction(id, blockHash) {
	const document = {
		_key: id,
		blockhash: blockHash
	};

	const ret = await saveDocument(TRANSACTIONS, document);

	transactionsCounter++;

	if (commander.perf && transactionsCounter % 1000 === 0) {
		logger.info(`1000 transactions in ${moment.duration(moment().diff(transactionsLapTime)).asMilliseconds() / 1000} seconds`);
		transactionsLapTime = moment();
	}

	return document;
}

async function saveOutputSpentIn(outputId, transactionId) {
	const document = {
		_key: outputId,
		_from: `outputs/${outputId}`,
		_to: `transactions/${transactionId}`
	};

	await saveDocument(OUTPUTS_SPENT_IN, document);

	return document;
}

async function saveOutput(transactionId, outputId, value) {
	const document = {
		_key: `${transactionId}:${outputId}`,
		value: value
	};

	const ret = await saveDocument(OUTPUTS, document);

	return document;
}

async function saveAddress(address, transactionId, outputId) {
	const document = {
		_key: address,
		outputs: [`${transactionId}:${outputId}`]
	};

	await saveOrUpdateDocument(ADDRESSES, document);

	return document;
}


// Frontend Database Logic

async function getOrCreateCollection (collection) {
	try {
		return await collection.handle.get();
	} catch (error) {
		logger.info(`Creating collection "${collection.name}"`);
		try {
			return await collection.handle.create();
		} catch (error) {
			throw new MyError(`Creating collection "${collection.name}" failed`, {error});
		}
	}
}

async function getOrCreateGraph (graph) {
	try {
		return await graph.handle.get();
	} catch (error) {
		if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_GRAPH_NOT_FOUND.code) {
			logger.info(`Creating graph "${graph.name}"`);
			try {
				return await graph.handle.create(graph.properties);
			} catch (error) {
				throw new MyError(`Creating graph "${graph.name}" failed`, {error});
			}
		} else {
			throw new MyError(`Getting graph ${graph.name} failed`, {error});
		}
	}
}

async function countDocuments (collection) {
	try {
		return await collection.handle.count();
	} catch (error) {
		throw new MyError(`Counting documents in collection "${collection.name}" failed`, {error});
	}
}

async function getDocument (collection, documentId) {
	try {
		return await collection.handle[collection.get](documentId)
	} catch (error) {
		throw new MyError(`Retrieving ${collection.entity} "${documentId}" failed`, {error});
	}
}

async function saveDocument (collection, document) {
	try {
		return await _modifyCollection(commander.retries, collection, 'save', document);
	} catch (error) {
		if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED.code && !commander.dontOverwrite) {
			logger.warning(`Saving ${collection.entity} "${document._key}" failed because it exists. Overwriting...`, {object: document});
			try {
				return await _replaceDocument(collection, document._key, document);
			} catch (error) {
				throw new MyError(`Overwriting ${collection.entity} "${document._key}" failed`, {error, object: document});
			}
		} else {
			throw new MyError(`Saving ${collection.entity} "${document._key}" failed`, {error, object: document});
		}
	}
}

async function saveOrUpdateDocument (collection, document) {
	let query = `
    UPSERT { _key: "${document._key}" }
    INSERT ${JSON.stringify(document)}
    UPDATE ${JSON.stringify(document)}
    IN ${collection.name}
    RETURN { NEW: NEW, OLD: OLD, type: OLD ? 'update' : 'insert' }
    `;

	try {
		return await _queryDatabase(query);
	} catch (error) {
		throw new MyError(`Creating ${collection.entity} "${document._key}" failed`, {error, object: document});
	}
}

async function _replaceDocument (collection, documentId, document, ...args) {
	return await _modifyCollection(commander.retries, collection, 'replace', documentId, document, ...args);
}

// function updateDocument (collection, documentId, document) {
//   return modifyCollection(commander.retries, collection, 'update', documentId, document)
// }

async function _queryDatabase (query, retries=commander.retries) {
    // console.log(`modifyCollection(collection=${collection}, operation=${operation}, args=${args}`);
	try {
		return await db.query(query);
	} catch (error) {
		if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_CONFLICT.code && retries > 0) {
			await setImmediatePromise();
			return await _queryDatabase(query, retries - 1);
		} else {
			throw error;
		}
	}
}

async function _modifyCollection (retries, collection, operation, ...args) {
	// logger.debug1('modifyCollection', {object: {args: args}});
	try {
		return await collection.handle[operation](...args);
	} catch (error) {
		if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_CONFLICT.code && retries > 0) {
			await setImmediatePromise();
			return await modifyCollection(retries - 1, collection, operation, ...args);
		} else {
			throw error;
		}
	}
}

// const transactionStr = `
// function run({address, value, output, transactionId }) {
// ${MyError}
// ${saveOrUpdateDocumentSync}
// ${saveDocumentSync}
// ${updateDocumentSync}
// ${modifyCollectionSync}
// const arangodb = require('@arangodb');
// const arangoErrors = arangodb.errors;
// const logger = console;
// logger.warning = console.warn;
// const ADDRESSES = {
//     name: 'addresses',
//     handle: arangodb.db._collection('addresses'),
//     entity: 'address'
// }
//
// saveOrUpdateDocumentSync(ADDRESSES, {
//     _key: address,
//     [transactionId]: {
//         [output]: value
//     }
// });
// }`
// await db.transaction({read: ['addresses'], write: ['addresses']}, transactionStr, { address, output, transaction, block })


// Error handling

function handleExceptions (type) {
	if (type === 'uncaughtException') {
		return (error) => {
			if (commander.debug >= 1) {
				logger.error(`uncaughtException`, {error});
			} else {
				logger.error(null, {error});
			}
			process.exit(1);
		};
	} else if (type === 'unhandledRejection') {
		return (error, promise) => {
			//   logger.error(`unhandledRejection`, {error})
			unhandledRejections.set(promise, error);
		};
	} else if (type === 'rejectionHandled') {
		return (promise) => {
			unhandledRejections.delete(promise);
		};
	}
}

function handleInt () {
	process.stdout.clearLine();
	process.stdout.cursorTo(0);
	if (stop) {
		process.exit(0);
	} else {
		stop = true;
	}
}

function handleExit (code) {
	if (unhandledRejections.size > 0) {
		unhandledRejections.forEach((error) => {
			if (commander.debug >= 1) {
				logger.error(`unhandledRejection`, {error});
			} else {
				logger.error(null, {error});
			}
		});
		if (code === 0) {
			process.exit(1);
		}
	}

	if (blocksCounter + transactionsCounter > 0) {
		const duration = moment.duration(moment().diff(startTime));
		logger.info(`Processed ${blocksCounter} blocks and ` +
			`${transactionsCounter} transactions in` +
			`${moment.utc(duration.asMilliseconds()).format('HH:mm:ss')}`);
	}
}
