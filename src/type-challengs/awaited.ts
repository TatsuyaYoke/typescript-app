;(() => {
  type MyAwaited<T> = T extends Promise<infer K> ? MyAwaited<K> : T
  // A = string
  type A = MyAwaited<Promise<string>>
  // B = number
  type B = MyAwaited<Promise<Promise<Promise<number>>>>
  // C = boolean | number
  type C = MyAwaited<boolean | Promise<number>>
})()
