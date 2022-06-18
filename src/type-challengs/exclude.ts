;(() => {
  interface TypeA {
    id: number
    name: string
  }
  interface TypeB {
    id: number
    address: string
  }

  type MyExclude<T, U> = T extends U ? never : T

  type ExcludedType = MyExclude<keyof TypeA, keyof TypeB> // "name"
  type ExcludedType2 = MyExclude<string, number> // string
  type ExcludedType3 = MyExclude<string | number | boolean, string | boolean> // number
})()
