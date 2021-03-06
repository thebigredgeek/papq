import * as BinHeap from 'qheap'
import ExtendableError from 'extendable-error'
import * as EventEmitter from 'events'

const wait = time => new Promise(resolve => setTimeout(resolve, time))

export interface PAPQOptions {
  removePartitionOnEmpty?: boolean,
  partitionGCDelay?: number,
  rejectEnqueueWhenBreakerThrown?: boolean,
  maxRetries?: number,
  backoff?: (retries: number) => number
}

interface PAPQPartitionOptions {
  backoff: (retries: number) => number,
  maxRetries: number,
  rejectEnqueueWhenBreakerThrown: boolean
}

const typeCheck = (d: any, t: string) => Object.prototype.toString.call(d) === `[object ${t}]`

const events = {
  BREAKER_THROWN: 'breaker:thrown',
  BREAKER_RESET: 'breaker:reset',
  PARTITION_START: 'partition:start',
  PARTITION_STOP: 'partition:stop',
  PARTITION_CREATED: 'partition:created',
  PARTITION_DESTROYED: 'partition:destroyed',
  PARTITION_EMPTY: 'partition:empty',
  PARTITION_EXEC_FINISHED: 'partition:exec_finished',
  ERROR: 'error',

}

class Deferred {
  public promise: Promise<any>
  public resolve: Function
  public reject: Function
  constructor () {
    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve
      this.reject = reject
    })
  }
}

class PartitionBreakerThrownError extends ExtendableError {
  constructor (partitionKey: string) {
    super(`PAPQPartition $[partitionKey} has thrown breaker.  Cannot enqueue incoming message`)
  }
}

class PAPQNode <T> {
  public deferred: Deferred = new Deferred()
  public partitionKey: string

  constructor (
    public data: T
  ) {}
}

class PAPQPartition <T> extends EventEmitter {

  private heap: BinHeap
  private subscriber: (d: T) => Promise<void> | void = (d: T) => null
  private running: boolean = false
  private executing: boolean = false
  private skipsRemaining: number = 0
  public healthy: boolean = true

  constructor (
    private comparator: (nd: T, d: T) => boolean,
    public partitionKey: string,
    private options: PAPQPartitionOptions
  ) {
    super()

    this.heap = new BinHeap({
      comparBefore: (n1, n2) => this.comparator(n1.data, n2.data)
    })
  }

  private async exec (next: PAPQNode<T>, retries: number = 0) {
    if (!next) {
      return
    }

    if (this.executing) {
      return
    }

    if (this.skipsRemaining) {
      this.skipsRemaining --
      this.emit(events.PARTITION_EXEC_FINISHED, this.partitionKey, next.data)
      return
    }

    this.executing = true

    try {
      await this.subscriber(next.data)
      next.deferred.resolve()
      this.executing = false
      this.emit(events.PARTITION_EXEC_FINISHED, this.partitionKey, next.data)
    } catch (err) {
      this.executing = false
      if (retries < this.options.maxRetries) {
        await wait(this.options.backoff(retries))
        return await this.exec(next, retries + 1)
      }

      this.heap.insert(next)

      this.emit(events.ERROR, this.partitionKey, err)

      this.throwBreaker()

      throw err
    }
  }

  public enqueue (n: PAPQNode<T>) {
    if (!this.healthy && this.options.rejectEnqueueWhenBreakerThrown) {
      throw new PartitionBreakerThrownError(this.partitionKey)
    }

    this.heap.insert(n)
  }


  public async start (): Promise<void> {
    try {
      if (!this.healthy) {
        return
      }

      if (this.running || this.executing) {
        return
      }

      this.running = true

      this.emit(events.PARTITION_START, this.partitionKey)

      while (this.heap.peek()) {
        const next = this.heap.dequeue()

        await this.exec(next)

        if (!this.running || !this.healthy) {
          break
        }
      }

      this.running = false

      if (this.heap.length < 1) {
        this.emit(events.PARTITION_EMPTY, this.partitionKey)
      }
    } catch (err) {}
  }


  public stop (): void {
    this.running = false
    this.emit(events.PARTITION_STOP, this.partitionKey)
  }

  public empty (): void {
    let next = this.heap.dequeue()

    while (next) {
      next.deferred.reject()
      next = this.heap.dequeue()
    }
    this.emit(events.PARTITION_EMPTY, this.partitionKey)
  }

  public throwBreaker (): void {
    this.healthy = false
    this.stop()
    this.emit(events.BREAKER_THROWN, this.partitionKey)
 }

  public resetBreaker (restart : boolean = true): void {
    this.healthy = true
    this.emit(events.BREAKER_RESET, this.partitionKey)
    if (restart) {
      this.start()
    }
  }

  public subscribe(subscriber: (d: T) => Promise<void> | void): void {
    this.subscriber = subscriber
  }

  public skip (num: number = 1) : void {
    this.skipsRemaining = num
  }
}

export class PAPQ <T> extends EventEmitter {
  private partitions: Map<string, PAPQPartition<T>> = new Map<string, PAPQPartition<T>>()
  private partitionGCs: Map<string, NodeJS.Timer> = new Map<string, NodeJS.Timer>()
  private subscriber: (d: T, partitionKey?: string) => Promise<void> | void

  constructor (
    private partitioner: (d: any) => string,
    private comparator: (nd: any, d: any) => boolean,
    private options: PAPQOptions
  ) {

    super()

    if (!typeCheck(this.partitioner, 'Function')) {
      throw new Error('PAPQ constructor requires partitioner function as first parameter')
    }

    if (!typeCheck(this.comparator, 'Function')) {
      throw new Error('PAPQ constructor requires comparator function as second parameter')
    }

    if (!typeCheck(this.options, 'Object')){
      this.options = {
        removePartitionOnEmpty: true,
        partitionGCDelay: 60000,
        rejectEnqueueWhenBreakerThrown: true,
        maxRetries: undefined,
        backoff: (retries: number) => retries * 1000
      }
    }

    if (!typeCheck(this.options.removePartitionOnEmpty, 'Boolean')) {
      this.options.removePartitionOnEmpty = true
    }

    if (!typeCheck(this.options.partitionGCDelay, 'Number')) {
      this.options.partitionGCDelay = 60000
    }

    if (!typeCheck(this.options.rejectEnqueueWhenBreakerThrown, 'Boolean')) {
      this.options.rejectEnqueueWhenBreakerThrown = true
    }

    if (!typeCheck(this.options.maxRetries, 'Number')) {
      this.options.maxRetries = undefined
    }

    if (!typeCheck(this.options.backoff, 'Function')) {
      this.options.backoff = (retries: number) => retries * 1000
    }

  }

  public get partitionKeys (): Array<string> {
    return Array.from(this.partitions.keys())
  }


  private destroyPartition (partitionKey: string) : void {
    this.partitions.delete(partitionKey)
    this.emit(events.PARTITION_DESTROYED, partitionKey)
  }

  private handlePartitionStart (partitionKey: string) : void {
    this.emit(events.PARTITION_START, partitionKey)
  }

  private handlePartitionStop (partitionKey: string) : void {
    this.emit(events.PARTITION_STOP, partitionKey)
  }

  private handlePartitionEmpty (partitionKey: string) : void {
    if (this.options.removePartitionOnEmpty) {
      this.partitionGCs.set(partitionKey, global.setTimeout(this.destroyPartition.bind(this), this.options.partitionGCDelay))
    }
    this.emit(events.PARTITION_EMPTY, partitionKey)
  }

  private handleBreakerThrown (partitionKey: string) : void {
    this.emit(events.BREAKER_THROWN, partitionKey)
  }

  private handleBreakerReset (partitionKey: string) : void {
    this.emit(events.BREAKER_RESET, partitionKey)
  }

  private handleExecFinished (partitionKey: string, data: T) : void {
    this.emit(events.PARTITION_EXEC_FINISHED, partitionKey, data)
  }

  private handlePartitionError (partitionKey: string, err: Error): void {
    this.emit(events.ERROR, partitionKey, err)
  }

  public enqueue (data: T): Promise<void> {
    const n: PAPQNode<T> = new PAPQNode<T>(data)

    const partitionKey: string = this.partitioner(data)

    let p: PAPQPartition<T> = this.partitions.get(partitionKey.toString())

    if (!p) {

      const options: PAPQPartitionOptions = {
        backoff: this.options.backoff,
        maxRetries: this.options.maxRetries,
        rejectEnqueueWhenBreakerThrown: this.options.rejectEnqueueWhenBreakerThrown
      }

      p = new PAPQPartition<T>(this.comparator, partitionKey, options)

      p.on(events.PARTITION_START, this.handlePartitionStart.bind(this))
      p.on(events.PARTITION_STOP, this.handlePartitionStop.bind(this))
      p.on(events.PARTITION_EMPTY, this.handlePartitionEmpty.bind(this))
      p.on(events.BREAKER_THROWN,  this.handleBreakerThrown.bind(this))
      p.on(events.BREAKER_RESET, this.handleBreakerReset.bind(this))
      p.on(events.PARTITION_EXEC_FINISHED, this.handleExecFinished.bind(this))
      p.on(events.ERROR, this.handlePartitionError.bind(this))

      this.partitions.set(partitionKey.toString(), p)

      this.emit(events.PARTITION_CREATED, partitionKey)

      p.subscribe((d: T) => this.subscriber(d, partitionKey))
    }

    p.enqueue(n)

    if (this.subscriber) {
      p.start()
    }

    if (this.partitionGCs.get(partitionKey)) {
      global.clearTimeout(this.partitionGCs.get(partitionKey))
      this.partitionGCs.delete(partitionKey)
    }

    return n.deferred.promise
  }

  public resetPartitionBreaker(key: string, restart?: boolean) : void {
    const partition = this.partitions.get(key)

    if (!partition) {
      return
    }

    partition.resetBreaker(restart)
  }

  public throwPartitionBreaker(key: string) : void {
    const partition = this.partitions.get(key)

    if (!partition) {
      return
    }

    partition.throwBreaker()
  }

  public emptyPartition(key: string) : void {
    const partition = this.partitions.get(key)

    if (!partition) {
      return
    }

    partition.empty()
  }

  public subscribe(fn: (d: T, partitionKey?: string) => void) : void {

    let wasSubscribed = !!this.subscriber

    this.subscriber = fn

    if (!wasSubscribed) {
      this.start()
    }
  }

  public skip (key: string, num?: number) : void {
    const partition = this.partitions.get(key)

    if (!partition) {
      return
    }

    return partition.skip(num)
  }

  public stop(key?: string): void {
    if (key !== undefined && key !== null) {
      this.partitions.get(key).stop()
      return
    }
    this.partitionKeys.forEach(key => this.partitions.get(key).stop())
  }

  public start(key?: string): void {
    if (key !== undefined && key !== null) {
      this.partitions.get(key).start()
      return
    }
    this.partitionKeys.forEach(key => this.partitions.get(key).start())
  }
}

