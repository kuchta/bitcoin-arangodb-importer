const winston = require('winston');
const moment = require('moment');

const { MyError } = require('./errors');

const config = {
	levels: {
		error: 0,
		warning: 1,
		info: 2,
		info1: 3,
		info2: 4,
		info3: 5,
		info4: 6,
		info5: 7,
		debug1: 8,
		debug2: 9
	},
	colors: {
		error: 'red',
		warning: 'yellow',
		info: 'blue',
		info1: 'blue',
		info2: 'blue',
		info3: 'blue',
		info4: 'blue',
		info5: 'blue',
		debug1: 'green',
		debug2: 'green'
	},
	// padLevels: true,
	transports: [
		new winston.transports.Console({
			level: 'debug2',
			timestamp: () => moment().format('HH:mm:ss'),
			colorize: true,
			prettyPrint: true,
			depth: 5,
			stderrLevels: ['error', 'warning'],
			formatter: null
		// align: true
		})
	]
};

function removeFirstLine (string) {
	let lines = string.split('\n');
	lines.splice(0, 1);
	return lines.join('\n');
}

function logMessage (verbose, debug, logger, level) {
	return (message, { error = null , object = null } = {}) => {
		// console.log(`message=${message}, error=${error}, object=${object}`)
		let stack;
		if (error instanceof Error) {
			stack = error.stack;
			if (error instanceof MyError) {
				if (message) {
					message = `${message}: ${error.toString()}`;
				} else {
					message = error.toString();
				}
				if (error.error) {
					// stack = error.error.stack;
					message = `${message}: ${error.error.toString()}`;
				}
				if (!object && error.object) {
					object = error.object;
				}
			} else {
				if (message) {
					message = `${message}: ${error.toString()}`;
				} else {
					message = error.toString();
				}
			}

			if (debug >= 2 && stack) {
				message = `${message}\n${removeFirstLine(stack)}`;
			}
		}

		if (debug >= 1 && object) {
			logger.log(level, message, object);
		} else {
			logger.log(level, message);
		}
	};
}

function getLogger (verbose, debug) {
	const logger = new (winston.Logger)(config);
	let log = {};

	for (let level in config.levels) {
		let match = level.match(/(info|debug)(\d)/);
		if (match) {
			if ((match[1] === 'info' && verbose >= match[2]) || (match[1] === 'debug' && debug >= match[2])) {
				log[level] = logMessage(verbose, debug, logger, level);
			} else {
				log[level] = () => {
				};
			}
		} else {
			log[level] = logMessage(verbose, debug, logger, level);
		}
	}
	return log;
}

module.exports = getLogger;
