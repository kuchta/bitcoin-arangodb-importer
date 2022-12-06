let array = {one: 1, two: 2, three: 3};

array.foo = 'ahojda';

console.log(Object.values(array));

for (const value of Object.values(array)) {
	console.log(value);
}

// import cac from 'cac';

// const cli = cac()
// .option('verbose', { alias: 'v', desc: 'increase verbosity', type: 'boolean' })
// .option('debug', { alias: 'd', desc: 'enable debug messages', type: 'boolean' });
// // const defaultCommand = cli.command('*', '', () => cli.showHelp());

// Object.keys(config).forEach((key) => {
// 	cli.option(key.toLowerCase().replace(/\_/g, '-'), {
// 		desc: `set config ${key}`,
// 		default: config[key],
// 		type: typeof config[key]
// 	});
// });

// // Add a sub command
// let command = cli.command('bob', 'Bob desc')
// .option('opt', {
// 	required: true,
// 	desc: 'description'

// })

// .handler = ((input, flags) => logger.info('bob', { input, flags} ));

// // cli.use((options) => logger.info('options', options.options));

// cli.on('parsed', (command, input, flags) => {
// 	// command might be undefined
// });

// cli.parse();

const prog = require('caporal');
prog
	.version('1.0.0')
	.command('deploy', 'Our deploy command')
	.argument('<app>', 'App to deploy')
	.argument('<env>', 'Environment')
	.option('--how-much', 'How much app to deploy', prog.INT, 1)
	.action(function (args, options, logger) {
		logger.info(args);
		logger.info(options);
		// {
		//   "app": "myapp",
		//   "env": "production"
		// }
		// {
		//   "howMuch": 2
		// }
	});
prog.parse(process.argv);