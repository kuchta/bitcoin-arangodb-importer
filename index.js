#!/usr/bin/env node

'use strict'

const util = require('util')

const setImmediatePromise = util.promisify(setImmediate)

const commander = require('commander')
const moment = require('moment')

const Client = require('bitcoin-core')
const {Database, aql} = require('arangojs')

const { MyError, arangoErrors } = require('./errors')
const config = require('./config')

commander
    .version('0.0.1')
    .option('-c, --clean', 'Clean database before import')
    .option('-a, --async', 'Process transactions asynchronously')
    .option('-o, --overwrite', 'Overwrite existing entries')
    .option('-r, --retries <n>', 'Number of retries in case of conflict', 3)
    .option('-v, --verbose', 'Increase verbosity', (v, total) => total + 1, 0)
    .option('-d, --debug', 'Increase verbosity of debug messages', (v, total) => total + 1, 0)
    .parse(process.argv)

const logger = require('./logger')(commander.verbose, commander.debug)

const unhandledRejections = new Map()

process.on('exit', handleExit)
process.on('SIGINT', handleInt)
process.on('uncaughtException', handleExceptions('uncaughtException'))
process.on('unhandledRejection', handleExceptions('unhandledRejection'))
process.on('rejectionHandled', handleExceptions('rejectionHandled'))

const startTime = moment()
let numBlocks = 0
let numTransactions = 0
let numOutputs = 0
let numAddresses = 0
let stop = false

const client = new Client(config.bitcoinRPC)

const db = new Database({
  url: `http://${config.database.username}:${config.database.password}@${config.database.host}:${config.database.port}`,
  databaseName: config.database.database
})

const BLOCKS = {
  name: 'blocks',
  entity: 'block',
  handle: db.collection('blocks')
}

const TRANSACTIONS = {
  name: 'transactions',
  entity: 'transaction',
  handle: db.collection('transactions')
}

const ADDRESSES = {
  name: 'addresses',
  entity: 'address',
  handle: db.collection('addresses')
};

(async function run () {
  if (commander.clean) {
    logger.info('Cleaning database')
    await db.truncate()
    logger.info('Database cleaned')
  } else {
    await Promise.all([
      getOrCreateCollection(BLOCKS),
      getOrCreateCollection(TRANSACTIONS),
      getOrCreateCollection(ADDRESSES)
    ])

    let block
    let blockHash

    if ((await countDocuments(BLOCKS)).count > 0) {
      const cursor = await db.query(
                'FOR b IN blocks ' +
                'SORT b.height DESC ' +
                'LIMIT 1 ' +
                'RETURN b'
            )
      block = await cursor.next()
      blockHash = block._key
    } else {
      block = {height: 1}
      blockHash = await client.getBlockHash(1)
    }

    let bestBlock = await client.getBlock(await client.getBestBlockHash())
    let bestBlockHeight = bestBlock.height
    let progress
    let lapTime = moment()
    let lapTransactions = 0

    logger.info(`Best block: ${bestBlockHeight}`)
    logger.info(`Last known best block: ${block.height}`)

    while (!stop && blockHash) {
      block = await client.getBlock(blockHash, 2)
      if (block.height % 1000 === 0) {
        progress = block.height / bestBlockHeight
        lapTime = moment.duration(moment().diff(lapTime)).asMilliseconds() / 1000
        logger.info(`Progress: ${block.height}/${bestBlockHeight} (${Math.round(progress * 100)}%) containing ${lapTransactions} transactions in ${Math.round(lapTime)} sec`)
        lapTime = moment()
        lapTransactions = 0
      }
      lapTransactions += await processBlock(block)
      blockHash = block.nextblockhash ? block.nextblockhash : null
    }
  }
})()

// Bussiness logic

async function processBlock (block) {
  logger.info1(`Processing block #${block.height} containing ${block.tx.length} transactions`)

  if (commander.async) {
    await Promise.all(block.tx.map((transaction) => processTransaction(transaction, block)))
  } else {
    for (let transaction of block.tx) {
      await processTransaction(transaction, block)
    }
  }

  let document = {
    _key: block.hash,
    height: block.height,
    time: block.time,
    tx: block.tx.map((tx) => tx.txid)
  }

  await saveDocument(BLOCKS, document)

  numBlocks++

  logger.info4(`Done processing block #${block.height}`)

  return block.tx.length
}

async function processTransaction (transaction, block) {
  logger.info2(`Processing transaction: ${transaction.txid}`)

  if (transaction.vin[0].hasOwnProperty('coinbase')) {
    if (transaction.vin.length !== 1) {
      logger.warning(`Coinbase transaction with multiple inputs: ${transaction}`)
    }
  }

  await Promise.all(transaction.vout.map((output) => processOutput(output, transaction, block)))

  const document = {
    _key: transaction.txid,
    blockhash: block.hash
  }

  await saveDocument(TRANSACTIONS, document)

  numTransactions++

  logger.info4(`Done processing transaction: ${transaction.txid}`)

  return transaction.vout.length
}

async function processOutput (output, transaction, block) {
  logger.info4(`Processing output #${output.n} in transaction ${transaction.txid}`)

  if (!output.hasOwnProperty('scriptPubKey')) {
    throw new MyError(`No scriptPubKey in output #${output.n} in transaction "${transaction.txid}"`, {object: output})
  }

  if (!output.scriptPubKey.hasOwnProperty('addresses')) {
    logger.warning(`No addresses in output #${output.n} in transaction "${transaction.txid}"`, {object: output})
    return
  }

  if (output.scriptPubKey.addresses.length > 1) {
    logger.warning(`Unexpected number of addresses in output #${output.n} in transaction "${transaction.txid}"`, {object: output})
  } else if (output.scriptPubKey.addresses < 1) {
    logger.warning(`No addresses in output #${output.n} in transaction "${transaction.txid}"`, {object: output})
  }

  await Promise.all(output.scriptPubKey.addresses.map((address) => processAddress(address, output, transaction, block)))

  numOutputs++

  logger.info4(`Done processing output #${output.n} in transaction ${transaction.txid}`)

  return output.scriptPubKey.addresses
}

async function processAddress (address, output, transaction, block) {
  logger.info3(`Processing address ${address} in output #${output.n} in transaction ${transaction.txid}`)

  const document = {
    _key: address,
    [transaction.txid]: {
      [output.n]: output.value
    }
  }

  await saveOrUpdateDocument(ADDRESSES, document)

  numAddresses++

  logger.info4(`Done processing address ${address} in output #${output.n} in transaction ${transaction.txid}`)
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
    await collection.handle.get()
  } catch (error) {
    logger.info(`Creating collection "${collection.name}"`)
    try {
      collection.handle.create()
    } catch (error) {
      throw new MyError(`Creating collection "${collection.name}" failed`, {error})
    }
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
  //   // logger.debug1('query', query)
  //
  // try {
  //   let ret = await queryDatabase(5, query)
  //   if (ret._result[0].type === 'update') {
  //     logger.warning(`${collection.entity} "${document._key}" updated`)
  //   }
  //     // logger.debug1('ret', ret)
  //     // return await saveDocument(collection, document)
  // } catch (error) {
  //   throw new MyError(`Creating ${collection.entity} "${document._key}" failed`, {error, document})
  // }
// }

async function saveOrUpdateDocument (collection, document) {
  let query = `
    UPSERT { _key: "${document._key}" }
    INSERT ${JSON.stringify(document)}
    UPDATE ${JSON.stringify(document)}
    IN ${collection.name}
    RETURN { NEW: NEW, OLD: OLD, type: OLD ? 'update' : 'insert' }
    `

  try {
    let ret = await queryDatabase(commander.retries, query)
        // if (ret._result[0].type === 'update') {
        //   logger.warning(`${collection.entity} "${document._key}" updated`, {OLD: ret._result[0].OLD, NEW: ret._result[0].NEW})
        // }
  } catch (error) {
    logger.warning(`query: ${query}`)
    logger.warning(`error code: ${error.code}, error: ${error}`)
    throw new MyError(`Creating ${collection.entity} "${document._key}" failed`, {error, object: document})
  }
}

function countDocuments (collection) {
  return collection.handle.count()
}

// function getDocument (collection, documentId) {
//   return collection.handle.document(documentId)
// }

async function saveDocument (collection, document) {
  try {
    return await modifyCollection(commander.retries, collection, 'save', document)
  } catch (error) {
    if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED.code && commander.overwrite) {
      logger.warning(`Saving ${collection.entity} "${document._key}" failed because it exists. Overwriting...`, {object: document})
      try {
        return await replaceDocument(collection, document._key, document)
      } catch (error) {
        throw new MyError(`Overwriting ${collection.entity} "${document._key}" failed`, {error, object: document})
      }
    } else {
      throw new MyError(`Saving ${collection.entity} "${document._key}" failed`, {error, object: document})
    }
  }
}

async function replaceDocument (collection, documentId, document) {
  return modifyCollection(commander.retries, collection, 'replace', documentId, document)
}

// function updateDocument (collection, documentId, document) {
//   return modifyCollection(commander.retries, collection, 'update', documentId, document)
// }

async function queryDatabase (retries, query) {
    // console.log(`modifyCollection(collection=${collection}, operation=${operation}, args=${args}`);
  try {
    return await db.query(query)
  } catch (error) {
    if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_CONFLICT.code && retries > 0) {
      await setImmediatePromise()
      return queryDatabase(retries - 1, query)
    } else {
      throw error
    }
  }
}

async function modifyCollection (retries, collection, operation, ...args) {
    // console.log(`modifyCollection(collection=${collection}, operation=${operation}, args=${args}`);
  try {
    return await collection.handle[operation](...args)
  } catch (error) {
    if (error.isArangoError && error.errorNum === arangoErrors.ERROR_ARANGO_CONFLICT.code && retries > 0) {
      await setImmediatePromise()
      return modifyCollection(collection, operation, retries - 1, ...args)
    } else {
      throw error
    }
  }
}

// Error handling

function handleExceptions (type) {
  if (type === 'uncaughtException') {
    return (error) => {
      logger.error(`uncaughtException`, {error})
      process.exit(1)
    }
  } else if (type === 'unhandledRejection') {
    return (error, promise) => {
    //   logger.error(`unhandledRejection`, {error})
      unhandledRejections.set(promise, error)
    }
  } else if (type === 'rejectionHandled') {
    return (promise) => {
      unhandledRejections.delete(promise)
    }
  }
}

function handleInt () {
  process.stdout.clearLine()
  process.stdout.cursorTo(0)
  if (stop) {
    process.exit(0)
  } else {
    stop = true
  }
}

function handleExit (code) {
  if (unhandledRejections.size > 0) {
    unhandledRejections.forEach((error) => {
      if (error instanceof MyError) {
        logger.error(null, {error})
      } else {
        logger.error(`unhandledRejection`, {error})
      }
    })
    if (code === 0) {
      process.exit(1)
    }
  }

  if (numBlocks + numTransactions + numOutputs > 0) {
    const duration = moment.duration(moment().diff(startTime))
    logger.info(`Processed ${numBlocks} blocks, ` +
            `${numTransactions} transactions, ` +
            `${numOutputs} outputs and ` +
            `${numAddresses} addresses in ` +
            `${moment.utc(duration.asMilliseconds()).format('HH:mm:ss')}`)
  }
}
