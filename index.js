#!/usr/bin/env node

'use strict';

const os = require('os');
const cluster = require('cluster');

const commander = require('commander');
const moment = require('moment');

const logger = require('./logger');
const bitcoin = require('./bitcoin');
const db = require('./db');
const worker = require('./worker.js');

const config = require('./config');

const numCPUs = os.cpus().length;

commander
    .version('0.0.1')
    .option('-v, --verbose', 'Increase verbosity', (v, total) => total + 1, 0)
    .option('-d, --debug', 'Increase verbosity of debug messages', (v, total) => total + 1, 0)
    .option('-a, --async', 'Process transactions asynchronously')
    .option('-r, --retries <n>', 'Number of retries in case of conflict', 3)
    .option('-w, --max-workers <n>', 'Maximal number of workers', 1)
    .option('-o, --dont-overwrite', 'Don\'t overwrite existing entries')
    .option('-p, --perf', 'Increase performace report verbosity', (v, total) => total + 1, 0)
    .option('-c, --clean', 'Clean database before import')
	.parse(process.argv);
	
logger.init(commander.verbose, commander.debug);
db.init(
	config.database.username, 
	config.database.password, 
	config.database.host, 
	config.database.port, 
	config.database.database, 
	commander.retries, 
	commander.dontOverwrite, 
	commander.perf
);
bitcoin.init(config.bitcoinRPC, commander.perf);
worker.init(commander.verbose, commander.debug, commander.async);

const unhandledRejections = new Map();

process.on('exit', handleExit);
process.on('SIGINT', handleInt);
process.on('uncaughtException', handleExceptions('uncaughtException'));
process.on('unhandledRejection', handleExceptions('unhandledRejection'));
process.on('rejectionHandled', handleExceptions('rejectionHandled'));

const maxNumCPUs = 10;
const numWorkers = commander.maxWorkers < maxNumCPUs ? commander.maxWorkers : maxNumCPUs;
const startTime = moment();
let bestBlock;
let lastBlockHeight;
let blocksCounter = -numWorkers;
let transactionsCounter = 0;
let inputsOutputsCounter = 0;
let inputsOutputsLapTime = moment();
let progress = 0;

(async function run() {
	if (cluster.isMaster) {
		// logger.debug1('Master started');

		if (commander.clean) {
			logger.info('Cleaning database');
			await db.cleanDatabase();
			logger.info('Database cleaned');
			process.exit(0);
		}
		
		await db.initializeDatabase();
	
		bestBlock = await bitcoin.getBlock(await bitcoin.getBestBlockHash());
		lastBlockHeight = await db.getLastBlockHeight();
		progress = Math.round((lastBlockHeight / bestBlock.height) * 100);
	
		logger.info(`Best block: ${bestBlock.height}`);
		logger.info(`Last block: ${lastBlockHeight}`);
		logger.info(`Progress: ${lastBlockHeight}/${bestBlock.height} (${progress}%)`);
		
		lastBlockHeight = lastBlockHeight - maxNumCPUs;
		if (lastBlockHeight <= 0) {
			lastBlockHeight = -blocksCounter;
		}		

		if (numWorkers > 1) {
			let lastBlockHashPromise;
			function getLastBlockHash() {
				let ret;
				if (!lastBlockHashPromise) {
					lastBlockHashPromise = bitcoin.getBlockHash(lastBlockHeight + blocksCounter);
				}
				ret = lastBlockHashPromise;
				lastBlockHashPromise = bitcoin.getBlockHash(lastBlockHeight + blocksCounter);

				return ret;
			}

			cluster.on('message', async (worker, message) => {
				if (typeof message === 'object') {
					processStats(message);
				}
				blocksCounter++;

				worker.send(await getLastBlockHash());
			});

			cluster.on('exit', (worker, code, signal) => {
				logger.debug1(`Worker #${worker.id} died`);
			});

			for (let i = 0; i < numWorkers; i++) {
				cluster.fork();
			}
		} else {
			let nextBlockHash = await bitcoin.getBlockHash(lastBlockHeight);
			while (nextBlockHash) {
				let stats = await worker.processBlock(nextBlockHash);
				processStats(stats);
				nextBlockHash = stats.nextBlockHash;
			}
			db.commit();
		}
	} else if (cluster.isWorker) {
		logger.debug1(`Worker #${cluster.worker.id} started`);

		process.on('message', async (message) => {
			// logger.debug2(`Worker #${cluster.worker.id} received message from master`, {object: message});
			try {
				const ret = await worker.processBlock(message);
				process.send(ret);
			} catch (error) {
				logger.error(error.message, {error});
			}
		});

		process.send('ready');
	}
})();

function processStats(stats) {
	// logger.debug1(`Master received message from worker #${worker.id}`, {object: message});
	transactionsCounter += stats.numTransactions;
	inputsOutputsCounter += stats.numInputs;
	inputsOutputsCounter += stats.numOutputs;

	const height = lastBlockHeight + blocksCounter;
	const newProgress = Math.round((height / bestBlock.height) * 100)
	if (newProgress > progress) {
		logger.info(`Progress: ${height}/${bestBlock.height} (${Math.round((height / bestBlock.height) * 100)}%)`);
		progress = newProgress;
	}

	if (commander.perf >= 1 && inputsOutputsCounter % 1000 === 0) {
		logger.info(`1k inputs and outputs in ${moment.duration(moment().diff(inputsOutputsLapTime)).asMilliseconds() / 1000} seconds`);
		inputsOutputsLapTime = moment();
	}
}

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
			// logger.error(`unhandledRejection`, {error})
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
	// if (stop) {
		process.exit(0);
	// } else {
		// stop = true;
	// }
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

	if (blocksCounter + transactionsCounter + inputsOutputsCounter > 0) {
		logger.info(`Processed ${blocksCounter} blocks, ` +
			`${transactionsCounter} transactions and ` +
			`${inputsOutputsCounter} inputs + outputs ` +
			`in ${moment.utc(moment.duration(moment().diff(startTime)).asMilliseconds()).format('HH:mm:ss')}`);
	}
}
