import debug from 'debug'
import http from 'http'
import { createWriteStream, createReadStream, promises as fsProm } from 'fs'

import concat from 'concat'
import { parse } from 'it-ndjson'
import { pipe } from 'it-pipe'
import ndjson from 'ndjson'
import { Web3Storage } from 'web3.storage'

import { getDestinationKey } from './utils.js'
import { getCreateListBuckets, getUpdateListBuckets } from './buckets.js'

const log = debug(`carpark-bucket-diff`)

/**
 * @param {import('./types').BucketDiffCreateListProps} props
 */
export async function startCreateList (props) {
  log('creating HTTP server...')
  const server = http.createServer(async (req, res) => {
    // @ts-expect-error request url
    const url = new URL(req.url, `http://${req.headers.host}`)
    if (url.pathname === '/metrics') {
      res.write({})
    } else {
      res.statusCode = 404
      res.write('not found')
    }
    res.end()
  })
  server.listen(
    8000,
    () => log(`server listening on: http://localhost:${8000}`)
  )

  log('starting create list...')
  await pipe(
    getList(props),
    logListResult(props)
  )
  log('ending create list...')

  log('merge list...')

  // merge list
  const tmpFiles = await fsProm.readdir(`${props.originBucket.name}-${props.prefix}`)
  const outputFile = `${props.originBucket.name}-${props.prefix}.ndjson`

  await concat(
    tmpFiles.map(file => `${props.originBucket.name}-${props.prefix}/${file}`),
    outputFile
  )

  log('ending merge list...')

  log('upload to web3.storage...')

  // Store data to web3.storage
  const w3Client = new Web3Storage({
    token: props.web3StorageToken
  })

  const files = [{
    name: outputFile,
    stream: () => createReadStream(outputFile)
  }]

  const cid = await w3Client.put(files, {
    name: outputFile,
    wrapWithDirectory: false
  })
  console.log('cid', cid)

  log('closing HTTP server...')
  server.close()
}

/**
 * @param {import('./types').BucketDiffUpdateListProps} props
 */
export async function startUpdateList (props) {
  const buckets = getUpdateListBuckets(props)


}

/**
 * @param {import('./types').BucketDiffCreateListProps} props
 */
async function * getList (props) {
  const buckets = getCreateListBuckets(props)
  let outList = []

  for await (const contents of buckets.originBucket.list({
    MaxKeys: props.readBatchSize,
    Prefix: props.prefix,
    ContinuationToken: props.continuationToken
  })) {
    const results = await Promise.all(
      contents
        // We can't filter by suffix within List object command
        .filter(c => c.Key?.endsWith('.car'))
        .map(async c => {
          const key = await getDestinationKey(c.Key || '')
          return {
            inKey: c.Key,
            outKey: key,
            size: c.Size || 0
          }
        })
    )

    outList = [
      ...outList,
      // remove duplicates from new
      ...(results.filter((r, index, array) => {
        return array.findIndex(a => a.outKey === r.outKey) === index
      }))
    ]

    if (outList.length >= props.writeBatchSize) {
      yield outList.splice(0, props.writeBatchSize)
    }
  }
}

/**
 * @param {import('./types').BucketDiffCreateListProps} props
 */
function logListResult (props) {
  let counter = 0

  return async function (source) {
    const outDir = `${props.originBucket.name}-${props.prefix}`
    // Allow failure if exists
    try {
      await fsProm.mkdir(outDir)
    } catch {}

    for await (const page of source) {
      const outputFile = `${outDir}/${counter}.ndjson`
      const transformStream = ndjson.stringify()
      const outputStream = transformStream.pipe(createWriteStream(outputFile))

      for (const result of page) {
        transformStream.write({
          in: result.inKey || '',
          out: result.outKey,
          size: result.size || 0
        })
      }

      counter++

      console.log(counter, 'wrote', outputFile)
      transformStream.end()
      await new Promise((resolve) => {
        outputStream.on('finish', () => resolve(true))
      })
    }
  }
}
