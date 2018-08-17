const PAPQ = require('./dist/structures.js')

const q = new PAPQ(data => data.id, (d1, d2) => d1.value < d2.value)

const wait = time => new Promise(resolve => setTimeout(resolve, time))

let MAX_NUM_PARTITIONS = 20
let TARGET_NUM_EVENTS = 5000


let numPartitions = 0

const startStopMonkey = setInterval(() => {
  const id = String(Math.floor(Math.random() * 3) + 1)
 
  console.log(`manually stopping partition ${id}`)
  q.stop(id)

  setTimeout(() => {
    q.start(id)
    console.log(`manually restarting partition ${id}`)
  }, 2500)
}, 2500)

const manualBreakerMonkey = setInterval(() => {
  const id = String(Math.floor(Math.random() + 3) + 1)

  console.log(`manually throwing breaker for partition ${id}`)
  
  q.throwPartitionBreaker(id)
  
  setTimeout(() => {
    console.log(`manually resetting breaker for partition ${id}`)
    q.resetPartitionBreaker(id)
  }, 2500)
}, 2500)



q.on('breaker:thrown', async partitionKey => {
   const ttr = Math.floor(Math.random() * 5000) + 1
   console.log(`partition ${partitionKey} breaker thrown.  resetting in ${ttr}ms`)
   await wait(ttr)
   q.resetPartitionBreaker(partitionKey)
});
q.on('breaker:reset', partitionKey => console.log(`partition ${partitionKey} breaker reset`))
q.on('partition:start', partitionKey => console.log(`partition ${partitionKey} started`))
q.on('partition:stop', partitionKey => console.log(`partition ${partitionKey} stopeed`))
q.on('partition:created', partitionKey => console.log(`partition ${partitionKey} created`))
q.on('partition:destroyed', partitionKey => console.log(`partition ${partitionKey} destroyed`))
q.on('partition:empty', partitionKey => {
	console.log(`partition ${partitionKey} empty`)
	numPartitions --;

        if (numPartitions < 1) {
		clearInterval(startStopMonkey)
		clearInterval(manualBreakerMonkey)
	}
})
q.on('error', (partitionKey, err) => console.trace(`error in partition ${partitionKey}`, err))


const map = {}

for (var i = 0; i < TARGET_NUM_EVENTS; i++) {
  const id = String(Math.floor(Math.random() * MAX_NUM_PARTITIONS) + 1)
  
  const value = Math.floor(Math.random() * 999) + 1

  if (map[id]) {
    const val = map[id]
    val.push({ id, value })
    val.sort((d1, d2) => d1.value - d2.value)
    map[id] = val 
  } else {
    numPartitions ++
    map[id] = [{ id, value }]
  }
}

Object.keys(map).map(key => map[key].forEach(data => q.enqueue(data)))

q.subscribe(async (data, partitionKey) => {
  await wait(250)

  const key = String(partitionKey)
  const val = map[key]
  console.log(`partition ${key} - ${data.value}`)
  if (Math.floor(Math.random() * 10) + 1 <= 2) {
    throw new Error('random error that was thrown')
  }

  // console.log(map, key, val)

  const next = val.shift()
  if (next.value !== data.value) {
    console.error(`PARTITION ${partitionKey} OUT OF ORDER! ${next.value} !== ${data.value}`)
    process.exit(1)
    return
  }
  map[partitionKey] = val
})


