;(() => {
  interface Todo {
    title: string
    description: string
    completed: boolean
  }

  type MyOmit<T, K extends keyof T> = {
    // [V in { [U in keyof T]: U extends K ? never : U }[keyof T]]: T[V]
    [P in keyof T as P extends K ? never : P]: T[P]
  }
  type TodoPreview = MyOmit<Todo, 'description' | 'title'>

  const todo: TodoPreview = {
    completed: false,
  }
})()
