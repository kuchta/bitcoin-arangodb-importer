'use strict';

const { MyError, arangoErrors } = require('./errors');

const logger = require('./logger');
const bitcoin = require('./bitcoin');
const db = require('./db');

let VERBOSE;
let DEBUG;
let ASYNC;

function init(verbose, debug, async) {
	VERBOSE = verbose;
	DEBUG = debug;
	ASYNC = async;
}

// Bussiness logic

async function processBlock (blockHash) {
	let block = await bitcoin.getBlock(blockHash);

	logger.info1(`Processing block #${block.height} containing ${block.tx.length} transactions`);

	let stats = await map(processTransaction, block.tx, block);

	await db.saveBlock(block.hash, block.height, block.time, block.tx.map((tx) => tx.txid));

	logger.info4(`Done processing block #${block.height}`);

	stats = stats.reduce((accumulator, value) => {
		accumulator.numInputs += value.numInputs;
		accumulator.numOutputs += value.numOutputs;
		accumulator.numAddresses += value.numAddresses;
		return accumulator;
	}, {numInputs: 0, numOutputs: 0, numAddresses: 0});

	return {
		nextBlockHash: block.nextblockhash,
		numTransactions: block.tx.length,
		numInputs: stats.numInputs,
		numOutputs: stats.numOutputs,
		numAddresses: stats.numAddresses
	}
}

async function processTransaction (transaction, block, index) {
	logger.info2(`Processing transaction: ${transaction.txid} containing ${transaction.vin.length} inputs and ${transaction.vout.length} outputs`);

	await map(processInput, transaction.vin, transaction, block);
	// const inputsValue = inputsValues.reduce((total, value) => total + Math.round(1e8 * value));
	const numAddresses = await map(processOutput, transaction.vout, transaction, block);
	// const outputsValue = outputsValues.reduce((total, value) => total + Math.round(1e8 * value));

	// logger.debug1(`inputsValue: ${inputsValues}`);
	// logger.debug1(`outputsValue: ${outputsValues}`);

	// if (outputsValue > inputsValue) {
	// 	logger.warning(`Transaction "${transaction.txid} is spending more (${outputsValue}) than is the sum of it's inputs (${inputsValue})`);
	// }

	db.saveTransaction(transaction.txid, block.hash);

	logger.info4(`Done processing transaction: ${transaction.txid}`);

	return {
		numInputs: transaction.vin.length,
		numOutputs: transaction.vout.length,
		numAddresses: numAddresses.reduce((accumulator, value) => accumulator + value)
	}
}

async function processInput (input, transaction, block, index) {
	logger.info3(`Processing input in transaction ${transaction.txid}`);

	let outputId;
	let value;
	if (input.hasOwnProperty('coinbase')) {
		if (index > 0) {
			logger.warning(`Coinbase transaction in block "${block.hash}" is not the first transaction`);
		}
		outputId = `${transaction.txid}:coinbase`;
		value = getBlockSubsidy(block.height);
		await db.saveOutput(outputId, value);
	} else {
		outputId = `${input.txid}:${input.vout}`;
		// try {
		// 	const output = await getDocument(OUTPUTS, outputId);
		// 	value = output.value
		// } catch (error) {
		// 	if (error.code !== arangoErrors.ERROR_ARANGO_DOCUMENT_NOT_FOUND.code &&
		// 			error.code !== arangoErrors.ERROR_HTTP_NOT_FOUND.code) {
		// 		throw error;
		// 	}
		// 	logger.warning(`Can't find output "${outputId}" referenced in input #${index} of transaction "${transaction.txid}"`);
		// 	value = 0;
		// }
	}

	if (outputId) {
		db.saveOutputToTransaction(outputId, transaction.txid);
	}

	logger.info4(`Done processing input in transaction ${transaction.txid}`);

	return value;
}

async function processOutput (output, transaction, block, index) {
	logger.info3(`Processing output #${output.n} in transaction ${transaction.txid}`);

	if (!output.hasOwnProperty('scriptPubKey')) {
		throw new MyError(`No scriptPubKey in output #${output.n} in transaction "${transaction.txid}"`, {object: output});
	}

	await db.saveOutput(`${transaction.txid}:${output.n}`, output.value);
	await db.saveTransactionToOutput(transaction.txid, `${transaction.txid}:${output.n}`);

	let numAddresses = 0;

	if (output.scriptPubKey.hasOwnProperty('addresses') && output.scriptPubKey.addresses.length >= 1) {
		await map(db.saveAddress, output.scriptPubKey.addresses, transaction.txid, output.n);
		numAddresses = output.scriptPubKey.addresses.length;
	} else if (output.scriptPubKey.type !== 'nonstandard') {
		logger.warning(`No addresses in output #${output.n} in transaction "${transaction.txid}"`, {object: output});
	}

	logger.info4(`Done processing output #${output.n} in transaction ${transaction.txid}`);

	return numAddresses;
}

// Utility functions

function getBlockSubsidy(height, subsidy=50) {
	if (height >= 210000) {
		return getBlockSubsidy(height - 210000, subsidy/2);
	} else {
		return subsidy;
	}
}

async function map (func, list, ...args) {
	let ret;
	if (ASYNC) {
		ret = await Promise.all(list.map((value, index) => func(value, ...args, index)));
	} else {
		ret = []
		for (let index = 0; index < list.length; index++) {
			ret.push(await func(list[index], ...args, index));
		}
	}
	return ret;
}

module.exports = {
	init,
	processBlock
};