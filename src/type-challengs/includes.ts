;(() => {
  type Includes<T extends unknown[], K> = K extends T[number] ? true : false
  type isPillarMen = Includes<['Kars', 'Esidisi', 'Wamuu', 'Santana'], 'Dio'> // expected to be `false`
})()
