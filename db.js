'use strict';

const util = require('util');

const setImmediatePromise = util.promisify(setImmediate);

const moment = require('moment');

const { Database, aql } = require('arangojs');

const { MyError, arangoErrors } = require('./errors');

const logger = require('./logger');

let DB;

let BLOCKS = {
	name: 'blocks',
	entity: 'block',
	get: 'document',
	entities: []
};

let TRANSACTIONS = {
	name: 'transactions',
	entity: 'transaction',
	get: 'document',
	entities: [],
};

let OUTPUTS = {
	name: 'outputs',
	entity: 'output',
	get: 'document',
	entities: []
};

let ADDRESSES = {
	name: 'addresses',
	entity: 'address',
	get: 'document',
	entities: []
};

let ADDRESSES_TO_OUTPUTS = {
	name: 'addresses_to_outputs',
	entity: 'addresses_to_outputs',
	get: 'edge',
	entities: []
};

let OUTPUTS_TO_TRANSACTIONS = {
	name: 'outputs_to_transactions',
	entity: 'output_to_transaction',
	get: 'edge',
	entities: []
};

let TRANSACTIONS_TO_OUTPUTS = {
	name: 'transactions_to_outputs',
	entity: 'transaction_to_output',
	get: 'edge',
	entities: []
};

let GRAPH = {
	name: 'graph',
	properties: {
		edgeDefinitions: [{
			collection: ADDRESSES_TO_OUTPUTS.name,
			from: [ADDRESSES.name],
			to: [OUTPUTS.name]
		}, {
			collection: OUTPUTS_TO_TRANSACTIONS.name,
			from: [OUTPUTS.name],
			to: [TRANSACTIONS.name]
		}, {
			collection: TRANSACTIONS_TO_OUTPUTS.name,
			from: [TRANSACTIONS.name],
			to: [OUTPUTS.name]
		}]
	}
};

let RETRIES;
let DONT_OVERWRITE;
let PERF;
let NUM_BUFFERED_DOCUMENTS = 1000;

let saveDocumentCounter = 0;
let saveDocumentLapTime = 0;

function init(username, password, host, port, database, retries, dontOverwrite, perf) {
	RETRIES = retries;
	DONT_OVERWRITE = dontOverwrite;
	PERF = perf;

	DB = new Database({
		url: `http://${username}:${password}@${host}:${port}`,
		databaseName: database
	});

	BLOCKS.handle = DB.collection('blocks');
	TRANSACTIONS.handle = DB.collection('transactions');
	OUTPUTS.handle = DB.collection('outputs');
	ADDRESSES.handle = DB.collection('addresses');
	ADDRESSES_TO_OUTPUTS.handle = DB.edgeCollection('addresses_to_outputs');
	OUTPUTS_TO_TRANSACTIONS.handle = DB.edgeCollection('outputs_to_transactions');
	TRANSACTIONS_TO_OUTPUTS.handle = DB.edgeCollection('transactions_to_outputs');
	GRAPH.handle = DB.graph('graph');
}

// Database Logic

async function initializeDatabase() {
    await Promise.all([
		getOrCreateCollection(BLOCKS),
		getOrCreateCollection(TRANSACTIONS),
		getOrCreateCollection(OUTPUTS),
		getOrCreateCollection(ADDRESSES),
		getOrCreateCollection(ADDRESSES_TO_OUTPUTS),
		getOrCreateCollection(OUTPUTS_TO_TRANSACTIONS),
		getOrCreateCollection(TRANSACTIONS_TO_OUTPUTS)
	]);
	// await getOrCreateGraph(GRAPH)
}

async function getLastBlockHeight () {
    let lastBlockHeight;

	if ((await countDocuments(BLOCKS)).count > 0) {
		const cursor = await DB.query(
			'FOR b IN blocks ' +
			'SORT b.height DESC ' +
			'LIMIT 1 ' +
			'RETURN b'
		);
		lastBlockHeight = (await cursor.next()).height;
	} else {
		lastBlockHeight = 1;
    }
    
    return lastBlockHeight;
}

async function saveBlock(id, height, time, tx) {
	BLOCKS.entities.push({
		_key: id,
		height: height,
		time: time,
		tx: tx
	});

	if (BLOCKS.entities.length >= NUM_BUFFERED_DOCUMENTS) {
		importDocuments(BLOCKS);
	}
}

async function saveTransaction(transactionId) {
	TRANSACTIONS.entities.push({
		_key: transactionId,
	});

	if (TRANSACTIONS.entities.length >= NUM_BUFFERED_DOCUMENTS) {
		importDocuments(TRANSACTIONS);
	}
}

async function saveOutput(outputId, value) {
	OUTPUTS.entities.push({
		_key: outputId,
		value: value
	});

	if (OUTPUTS.entities.length >= NUM_BUFFERED_DOCUMENTS) {
		importDocuments(OUTPUTS);
	}
}

async function saveAddress(address, transactionId, outputId) {
	ADDRESSES.entities.push({
		_key: address
	});

	if (ADDRESSES.entities.length >= NUM_BUFFERED_DOCUMENTS) {
		importDocuments(ADDRESSES);
	}

	// try {
	// 	await saveDocument(ADDRESSES, document, true);
	// } catch (error) {
	// 	if (error.code !== arangoErrors.ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED.code) {
	// 		throw error;
	// 	}
	// }

	saveAddressToOutput(address, `${transactionId}:${outputId}`);
}

async function saveAddressToOutput(address, outputId) {
	ADDRESSES_TO_OUTPUTS.entities.push({
		_from: `addresses/${address}`,
		_to: `outputs/${outputId}`
	});

	if (ADDRESSES_TO_OUTPUTS.entities.length >= NUM_BUFFERED_DOCUMENTS) {
		importDocuments(ADDRESSES_TO_OUTPUTS);
	}
}

async function saveOutputToTransaction(outputId, transactionId) {
	OUTPUTS_TO_TRANSACTIONS.entities.push({
		_from: `outputs/${outputId}`,
		_to: `transactions/${transactionId}`
	});

	if (OUTPUTS_TO_TRANSACTIONS.entities.length >= NUM_BUFFERED_DOCUMENTS) {
		importDocuments(OUTPUTS_TO_TRANSACTIONS);
	}
}

async function saveTransactionToOutput(transactionId, outputId) {
	TRANSACTIONS_TO_OUTPUTS.entities.push({
		_from: `transactions/${transactionId}`,
		_to: `outputs/${outputId}`
	});

	if (TRANSACTIONS_TO_OUTPUTS.entities.length >= NUM_BUFFERED_DOCUMENTS) {
		importDocuments(TRANSACTIONS_TO_OUTPUTS);
	}
}

async function commit() {
	for (let entity of [
		BLOCKS, 
		TRANSACTIONS, 
		OUTPUTS, 
		ADDRESSES, 
		ADDRESSES_TO_OUTPUTS, 
		OUTPUTS_TO_TRANSACTIONS,
		TRANSACTIONS_TO_OUTPUTS
	]) {
		if (entity.entities.length > 0) {
			importDocuments(entity);
		}
	}
}

async function getOrCreateCollection (collection) {
	try {
		return await collection.handle.get();
	} catch (error) {
		if (error.isArangoError && (error.errorNum === arangoErrors.ERROR_HTTP_NOT_FOUND.code ||
				error.errorNum === arangoErrors.ERROR_ARANGO_COLLECTION_NOT_FOUND.code)) {
			// logger.error(error.toString(), {object: error.code});
			logger.info(`Creating collection "${collection.name}"`);
			try {
				return await collection.handle.create();
			} catch (error) {
				throw new MyError(`Creating collection "${collection.name}" failed`, {error});
			}
		} else {
			throw new MyError(`Getting collection "${collection.name}" failed`, {error});
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
			throw new MyError(`Getting graph "${graph.name}" failed`, {error});
		}
	}
}

async function countDocuments(collection) {
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

async function saveDocument (collection, document, dontOverwrite=false) {
	let time = moment();
	let ret;

	try {
		ret = await _modifyCollection(RETRIES, collection, 'save', document);
	} catch (error) {
		let id = collection.entity;
		if (document.hasOwnProperty('_key')) {
			id += ` (${document._key})`;
		}
		if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED.code) {
			if (!dontOverwrite && !DONT_OVERWRITE) {
				logger.warning(`Saving ${id} failed because it exists. Overwriting...`, {object: document});
				try {
					ret = await _replaceDocument(collection, document._key, document);
				} catch (error) {
					throw new MyError(`Overwriting ${id} failed`, {error, object: document});
				}
			} else {
				throw new MyError(`Saving ${id} failed`, {error, object: document});
			}
		} else {
			throw new MyError(`Saving ${id} failed`, {error, object: document});
		}
	}

	saveDocumentLapTime += moment.duration(moment().diff(time)).asMilliseconds();

	saveDocumentCounter++;

	if (PERF >= 2 && saveDocumentCounter % 1000 === 0) {
		logger.info(`1k saveDocument in ${saveDocumentLapTime / 1000} seconds`);
		saveDocumentLapTime = 0;
	}
}

async function importDocuments(entity) {
	try {
		entity.handle.import(entity.entities);
		entity.entities = [];
	} catch (error) {
		throw new MyError(`Importing ${id} failed`, {error, object: document});
	}
}

// async function saveOrUpdateDocument (collection, document) {
// 	let query = `
//     UPSERT { _key: "${document._key}" }
//     INSERT ${JSON.stringify(document)}
//     UPDATE ${JSON.stringify(document)}
//     IN ${collection.name}
//     RETURN { NEW: NEW, OLD: OLD, type: OLD ? 'update' : 'insert' }
//     `;

// 	try {
// 		return await _queryDatabase(query);
// 	} catch (error) {
// 		throw new MyError(`Creating ${collection.entity} "${document._key}" failed`, {error, object: document});
// 	}
// }



// function updateDocument (collection, documentId, document) {
//   return modifyCollection(RETRIES, collection, 'update', documentId, document)
// }

// async function _queryDatabase (query, retries=RETRIES) {
//     // console.log(`modifyCollection(collection=${collection}, operation=${operation}, args=${args}`);
// 	try {
// 		return await DB.query(query);
// 	} catch (error) {
// 		if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_CONFLICT.code && retries > 0) {
// 			await setImmediatePromise();
// 			return await _queryDatabase(query, retries - 1);
// 		} else {
// 			throw error;
// 		}
// 	}
// }

async function _replaceDocument (collection, documentId, document, ...args) {
	return await _modifyCollection(RETRIES, collection, 'replace', documentId, document, ...args);
}

async function _modifyCollection (retries, collection, operation, ...args) {
	// logger.debug1('modifyCollection', {object: {args: args}});
	try {
		return await collection.handle[operation](...args);
	} catch (error) {
		if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_CONFLICT.code && retries > 0) {
			await setImmediatePromise();
			return await _modifyCollection(retries - 1, collection, operation, ...args);
		} else {
			throw error;
		}
	}
}

module.exports = {
	init,
    cleanDatabase: () => DB.truncate(),
    initializeDatabase,
    getLastBlockHeight,
	saveBlock,
	saveTransaction,
	saveOutput,
	saveAddress,
	saveAddressToOutput,
	saveOutputToTransaction,
	saveTransactionToOutput
}