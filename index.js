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
let progress;
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

	saveBlock(block.hash, block.height, block.time, block.tx.map((tx) => tx.txid));

	logger.info4(`Done processing block #${block.height}`);
}

async function processTransaction (transaction, block) {
	logger.info2(`Processing transaction: ${transaction.txid}`);

	const inputsRet = await map(processInput, transaction.vin, transaction, block);
	const outputsRet = await map(processOutput, transaction.vout, transaction, block);

	// logger.debug1('inputsRet', {object: inputsRet});
	// logger.debug1('outputsRet', {object: outputsRet});

	saveTransaction(transaction.txid, block.hash);

	logger.info4(`Done processing transaction: ${transaction.txid}`);
}

async function processInput (input, transaction, block) {
	logger.info3(`Processing input in transaction ${transaction.txid}`);

	let output;
	let outputId;
	if (input.hasOwnProperty('coinbase')) {
		outputId = `${transaction.txid}:coinbase`;
		output = await saveOutput(transaction.txid, 'coinbase', getBlockSubsidy(block.height));
	} else {
		outputId = `${input.txid}:${input.vout}`;
		output = await getDocument(OUTPUTS, outputId);
	}

	if (!output) {
		logger.warning(`Can't find matching output for input ${outputId}`);
	} else {
		saveOutputSpentIn(outputId, transaction.txid);
	}

	return output.value;

	logger.info4(`Done processing input in transaction ${transaction.txid}`);
}

async function processOutput (output, transaction, block) {
	logger.info3(`Processing output #${output.n} in transaction ${transaction.txid}`);

	if (!output.hasOwnProperty('scriptPubKey')) {
		throw new MyError(`No scriptPubKey in output #${output.n} in transaction "${transaction.txid}"`, {object: output});
	}

	if (!output.scriptPubKey.hasOwnProperty('addresses')) {
		logger.warning(`No addresses in output #${output.n} in transaction "${transaction.txid}"`, {object: output});
	} else {
		if (output.scriptPubKey.addresses < 1) {
			logger.warning(`No addresses in output #${output.n} in transaction "${transaction.txid}"`, {object: output});
		} else {
			map(saveAddress, output.scriptPubKey.addresses, transaction.txid, output.n);
		}
	}

	saveOutput(transaction.txid, output.n, output.value);

	return output.value;

	logger.info4(`Done processing output #${output.n} in transaction ${transaction.txid}`);
}

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
		ret = await Promise.all(list.map((value) => func(value, ...args)));
	} else {
		ret = [];
		for (let value of list) {
			ret.push(await func(value, ...args));
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
	if (blocksCounter % 1000 === 0) {
		logger.info(`1000 blocks in ${moment.duration(moment().diff(blocksLapTime)).asMilliseconds() / 1000} seconds`);
		blocksLapTime = moment();
		const newProgress = Math.round((height / bestBlockHeight) * 100)
		if (newProgress > progress) {
			logger.info(`Progress: ${height} (${Math.round((height / bestBlockHeight) * 100)}%)`);
		}
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

async function saveOutput(transactionId, outputId, value) {
	const document = {
		_key: `${transactionId}:${outputId}`,
		value: value
	};

	const ret = await saveDocument(OUTPUTS, document);

	outputsCounter++;

	return document;
}


async function saveAddress(address, transactionId, outputId) {
	const document = {
		_key: address,
		outputs: [`${transactionId}:${outputId}`]
	};

	await saveOrUpdateDocument(ADDRESSES, document);
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

// Database logic

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
	return await collection.handle.count();
}

async function getDocument (collection, documentId) {
	try {
		return await collection.handle[collection.get](documentId)
	} catch (error) {
		throw new MyError(`Getting ${collection.entity} "${documentId}" failed`, {error, object: documentId});
	}
}

async function saveDocument (collection, document) {
	try {
		return await _modifyCollection(commander.retries, collection, 'save', document);
	} catch (error) {
		if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED.code && !commander.dontOverwrite) {
			let ret = await getDocument(collection, document._key);
			delete ret._id;
			delete ret._rev;
			let object;
			if (!_.isEqual(document, ret)) {
				object = {object: {old: ret, new: document}}
			}
			logger.warning(`Saving ${collection.entity} "${document._key}" failed because it exists. Overwriting...`, object);
			try {
				return await replaceDocument(collection, document._key, document);
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
		const ret = await queryDatabase(query);
		// const result = await cursor.all()
		// if (result[0].type === 'update') {
		//   logger.warning(`${collection.entity} "${document._key}" updated`, {OLD: result[0].OLD, NEW: result[0].NEW})
		// }
		return ret;
	} catch (error) {
		// logger.warning(`query: ${query}`);
		// logger.warning(`error code: ${error.code}, error: ${error}`);
		throw new MyError(`Creating ${collection.entity} "${document._key}" failed`, {error, object: document});
	}
}

// async function saveOrReplaceDocument (collection, document) {
  // let query = `
  //     UPSERT { _key: "${document._key}" }
  //     INSERT ${JSON.stringify(document)}
  //     REPLACE ${JSON.stringify(document)}
  //     IN ${collection.name}
  //     RETURN { type: OLD ? 'update' : 'insert' }
  //     `
  //
  // try {
  //   let ret = await queryDatabase(commander.retries, query)
  // 	const result = await cursor.all()
  //   if (result[0].type === 'update') {
  //     logger.warning(`${collection.entity} "${document._key}" updated`)
  //   }
  //     // logger.debug1('ret', result)
  //     // return await saveDocument(collection, document)
  // } catch (error) {
  //   throw new MyError(`Creating ${collection.entity} "${document._key}" failed`, {error, document})
  // }
// }

async function replaceDocument (collection, documentId, document, ...args) {
	return await _modifyCollection(commander.retries, collection, 'replace', documentId, document, ...args);
}

// function updateDocument (collection, documentId, document) {
//   return modifyCollection(commander.retries, collection, 'update', documentId, document)
// }

async function queryDatabase (query, retries=commander.retries) {
    // console.log(`modifyCollection(collection=${collection}, operation=${operation}, args=${args}`);
	try {
		return await db.query(query);
	} catch (error) {
		if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_CONFLICT.code && retries > 0) {
			await setImmediatePromise();
			return await queryDatabase(query, retries - 1);
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

// Error handling

function handleExceptions (type) {
	if (type === 'uncaughtException') {
		return (error) => {
			logger.error(`uncaughtException`, {error});
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
			if (error instanceof MyError) {
				logger.error(null, {error});
			} else {
				logger.error(`unhandledRejection`, {error});
			}
		});
		if (code === 0) {
			process.exit(1);
		}
	}

	if (blocksCounter + transactionsCounter + outputsCounter > 0) {
		const duration = moment.duration(moment().diff(startTime));
		logger.info(`Processed ${blocksCounter} blocks, ` +
			`${transactionsCounter} transactions, ` +
			`${outputsCounter} outputs in ` +
			`${moment.utc(duration.asMilliseconds()).format('HH:mm:ss')}`);
	}
}
