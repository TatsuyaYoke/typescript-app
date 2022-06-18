;(() => {
  //   type Includes<T extends unknown[], K> = K extends T[number] ? true : false
  type Equal<X, Y> = (<T>() => T extends X ? true : false) extends <T>() => T extends Y ? true : false ? true : false
  type Includes<T extends unknown[], U> = T extends [infer First, ...infer Rest]
    ? Equal<First, U> extends true
      ? true
      : Includes<Rest, U>
    : false

  type isPillarMen = Includes<['Kars', 'Esidisi', 'Wamuu', 'Santana'], 'Dio'> // expected to be `false`
})()
