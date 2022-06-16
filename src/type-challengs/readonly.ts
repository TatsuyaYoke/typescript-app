;(() => {
  interface Todo {
    title: string
    description: string
  }

  type MyReadonly<T> = { readonly [U in keyof T]: T[U] }
  const todo: MyReadonly<Todo> = {
    title: 'Hey',
    description: 'foobar',
  }

  todo.title = 'Hello' // Error: cannot reassign a readonly property
  todo.description = 'barFoo' // Error: cannot reassign a readonly property
})()
