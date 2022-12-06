'use strict';

const moment = require('moment');

const Client = require('bitcoin-core');

const logger = require('./logger');

let PERF;
let client;
let getBlockLapTime = 0;
let getBlockCounter = 0;

function init(config, perf) {
	client = new Client(config);
	PERF = perf;
}

async function getBlock(hash) {
	let time = moment();

	let block = await client.getBlock(hash, 2);

	getBlockLapTime += moment.duration(moment().diff(time)).asMilliseconds();

	getBlockCounter++;

	if (PERF >= 2 && getBlockCounter % 1000 === 0) {
		logger.info(`1k getBlock in ${getBlockLapTime / 1000} seconds`);
		getBlockLapTime = 0;
	}

	return block;
}

module.exports = {
	getBestBlockHash: () => client.getBestBlockHash(),
    getBlockHash: (hash) => client.getBlockHash(hash),
	init,
    getBlock 
};