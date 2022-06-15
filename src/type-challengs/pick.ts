interface Todo {
  title: string
  description: string
  completed: boolean
}

type MyPick<T, K extends keyof T> = { [U in K]: T[U] }
type TodoPick = MyPick<Todo, 'title' | 'completed'>

const todoPick: TodoPick = {
  title: 'Clean room',
  completed: false,
}
