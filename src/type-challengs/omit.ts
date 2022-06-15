interface Todo {
  title: string
  description: string
  completed: boolean
}

type MyOmit<T, K extends keyof T> = {
  [V in { [U in keyof T]: U extends K ? never : U }[keyof T]]: T[V]
}
type TodoOmit = MyOmit<Todo, 'description' | 'title'>

const todoOmit: TodoOmit = {
  completed: false,
}

