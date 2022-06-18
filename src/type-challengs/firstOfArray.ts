;() => {
  type arr1 = ['a', 'b', 'c']
  type arr2 = [3, 2, 1]
  // type First<T extends unknown[]> = T extends [] ? never : T[0]
  type First<T> = T extends [infer Head, ...unknown[]] ? Head : never
  type Last<T> = T extends [...unknown[], infer Last] ? Last : never
  type head1 = First<arr1> // expected to be 'a'
  type head2 = First<arr2> // expected to be 3
  type last1 = Last<arr1> // expected to be 'c'
  type last2 = Last<arr2> // expected to be 1
}
