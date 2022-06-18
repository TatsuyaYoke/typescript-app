;(() => {
  type Assert<L, R extends L> = L extends R ? true : false
  type FixedArray<E, L extends number, Result extends E[] = []> = Result['length'] extends L
    ? Result
    : FixedArray<E, L, [...Result, E]>

  type Tuple3 = Assert<[unknown, unknown, unknown], FixedArray<unknown, 3>>
  type Three = Assert<3, FixedArray<unknown, 3>['length']>

  type Counter<Value extends number = 0> = {
    count: Value
    up: Counter<Up<Value>>
    down: Counter<Down<Value>>
  }

  type Up<Value extends number> = ([...FixedArray<unknown, Value>, unknown] extends [...infer U] ? U : [])['length']
  type Down<Value extends number> = (FixedArray<unknown, Value> extends [unknown, ...infer Rest] ? Rest : [])['length']
  type Fifth = Assert<Counter['up']['up']['down']['up']['up']['count'], Up<Up<Down<Up<Up<0>>>>>>
})()
