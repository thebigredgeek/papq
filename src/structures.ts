import * as BinHeap from 'qheap'
import ExtendableError from 'extendable-error'


const wait = time => new Promise(resolve => setTimeout(resolve, time))

export interface PAPQOptions {
  removePartitionOnEmpty: boolean,
  partitionGCDelay: number,
  rejectEnqueueWhenBreakerThrown: boolean,
  maxRetries?: number,
  backoff?: (retries: number) => number
}

class Deferred {
  public promise: Promise<Function>
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

export class PAPQNode <T> {
  public data: T
  public deferred: Deferred
  public partitionKey: string

  constructor (data) {
    this.data = data
    this.deferred = new Deferred()
  }
}

export class PAPQPartition <T> {
  private heap: BinHeap
  private subscriber: (d: T) => Promise<void> | void = (d: T) => null
  public healthy: boolean = true


  constructor (
    private comparator: (nd: T, d: T) => boolean,
    public partitionKey: string,
    private onEmpty: (key: string) => void,
    private onThrownBreaker: (key: string) => void,
    private backoff: (retries: number) => number = (retries: number) => retries * 1000,
    private maxRetries: number = 3,
    private rejectEnqueueWhenBreakerThrown: boolean = true
  ) {
    this.heap = new BinHeap({
      comparBefore: (n1, n2) => this.comparator(n1.data, n2.data)
    })
  }

  private async exec (next: PAPQNode<T>, retries: number = 0) {
    if (!next) {
      return
    }

    try {
      await this.subscriber(next.data)
      next.deferred.resolve()
    } catch (err) {
      if (retries < this.maxRetries) {
        await wait(this.backoff(retries))
        return await this.exec(next, retries + 1)
      }

      next.deferred.reject(err)

      this.throwBreaker()

      throw err;
    }
  }

  public enqueue (n: PAPQNode<T>) {
    if (!this.healthy) {
      throw new PartitionBreakerThrownError(this.partitionKey)
    }

    this.heap.insert(n)
  }


  public async start (): Promise<void> {
    try {
      while (this.heap.peek()) {
        await this.exec(this.heap.dequeue())
      }
      this.onEmpty(this.partitionKey)
    } catch (err) {}
  }

  public empty (): void {
    let next = this.heap.dequeue()

    while (next) {
      next.deferred.reject()
      next = this.heap.dequeue()
    }
  }

  public throwBreaker (): void {
    this.healthy = false
    this.onThrownBreaker(this.partitionKey)
  }

  public resetBreaker (): void {
    this.healthy = true
    this.start()
  }

  public subscribe(subscriber: (d: T) => Promise<void> | void): void {
    this.subscriber = subscriber
  }
}

export class PAPQ <T> {
  private partitions: Map<string, PAPQPartition<T>>
  private partitionGCs: Map<string, NodeJS.Timer>
  private thrownBreakerHandler: (key: string) => void = (key: string) => console.warn(`Unhandled thrown breaker on partition ${key}`)
  private subscriber: (d: T) => Promise<void> | void = (d: T) => console.warn(`Received enqueued item but no subscriber is attached`)

  constructor (
    private partitioner: (d: any) => string,
    private comparator: (nd: any, d: any) => boolean,
    private options: PAPQOptions = { removePartitionOnEmpty: false, partitionGCDelay: 60000, rejectEnqueueWhenBreakerThrown: true, backoff: undefined, maxRetries: undefined }
  ) {}

  public get partitionKeys (): Array<string> {
    return Array.from(this.partitions.keys())
  }


  private handlePartitionEmpty (partitionKey: string) {
    if (this.options.removePartitionOnEmpty) {
      this.partitionGCs.set(partitionKey, global.setTimeout(() => this.partitions.delete(partitionKey), this.options.partitionGCDelay))
    }
  }

  public enqueue (data: T): Promise<Function> {
    const n: PAPQNode<T> = new PAPQNode<T>(data)

    const partitionKey: string = this.partitioner(data)

    let p: PAPQPartition<T> = this.partitions.get(partitionKey)

    if (!p) {
      p = new PAPQPartition<T>(this.comparator, partitionKey, this.handlePartitionEmpty, this.thrownBreakerHandler, this.options.backoff, this.options.maxRetries, this.options.rejectEnqueueWhenBreakerThrown)


      this.partitions.set(partitionKey, p)
    }

    p.enqueue(n)

    if (this.partitionGCs.get(partitionKey)) {
      global.clearTimeout(this.partitionGCs.get(partitionKey))
      this.partitionGCs.delete(partitionKey)
    }

    return n.deferred.promise
  }


  public onThrownBreaker(fn: (key: string) => void) {
    this.thrownBreakerHandler = fn
  }

  public resetPartitionBreaker(key: string) {
    const partition = this.partitions.get(key)

    if (!partition) {
      console.warn(`PAPQ#resetPartitionKey called with unknown partition key ${key}`)
      return
    }

    partition.resetBreaker()
  }

  public throwPartitionBreaker(key: string) {
    const partition = this.partitions.get(key)

    if (!partition) {
      console.warn(`PAPQ#throwPartitionBreaker called with unknown partition key ${key}`)
      return
    }

    partition.throwBreaker()
  }

  public emptyPartition(key: string) {
    const partition = this.partitions.get(key)

    if (!partition) {
      console.warn(`PAPQ#emptyPartition called with unknown partition key ${key}`)
      return
    }

    partition.empty()
  }

  public subscribe(fn: (d: T) => void) {
    this.subscriber = fn
  }
}
