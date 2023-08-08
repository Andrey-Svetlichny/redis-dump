import fs from 'fs'
import redis from 'redis'
import YAML from 'yaml'

const config = YAML.parse(fs.readFileSync('./config.yml', 'utf8'))

if (!fs.existsSync(config.dataDir.key)) fs.mkdirSync(config.dataDir.key, { recursive: true })
if (!fs.existsSync(config.dataDir.channel)) fs.mkdirSync(config.dataDir.channel, { recursive: true })

const client = redis.createClient(config.clientOpts)
client.on('error', (err) => console.log('Redis Client Error', err))
const clientSub = client.duplicate()
clientSub.on('error', (err) => console.log('Redis ClientSub Error', err))
await client.connect()
await clientSub.connect()

console.log(`keys("${config.pattern.key}")=`, await client.keys(config.pattern.key))
console.log(`pubSubChannels("${config.pattern.channel}")=`, await client.pubSubChannels(config.pattern.channel))

const kvp = {}
const valNum = {}

async function dumpKeys() {
  const keys = await client.keys(config.pattern.key)
  for (const k of keys) {
    const v = await client.get(k)
    if (kvp[k] != v) {
      console.log('new value for key=', k)
      const n = valNum[k] || 0
      valNum[k] = n + 1
      fs.writeFileSync(`${config.dataDir.key}/${k}_${n}.json`, v)
      kvp[k] = v
    }
  }
}

await dumpKeys()
setInterval(dumpKeys, 1000)

// subscribe channels
let msgNum = {}
await clientSub.pSubscribe(config.pattern.channel, (message, channel) => {
  const n = msgNum[channel] || 0
  msgNum[channel] = n + 1
  console.log("message in channel='" + channel)
  fs.writeFileSync(`${config.dataDir.channel}/${channel}_${n}.json`, message)
})
