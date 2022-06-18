;(() => {
  const func = (text: string, num: number) => {
    return `${text}${num}`
  }

  type MyParameters<T> = T extends (...args: infer R) => unknown ? R : never
  type TypeA = MyParameters<typeof func> // [string, number]
})()
