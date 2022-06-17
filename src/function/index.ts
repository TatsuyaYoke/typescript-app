export const uniqueArray = <T>(array: T[]) => [...new Set(array)]

export const getStringFromDateFixedTime = (date: Date, time?: string) => {
  const year = date.getFullYear().toString()
  const month = `0${date.getMonth() + 1}`.slice(-2)
  const day = `0${date.getDate()}`.slice(-2)
  if (time !== undefined) return `${year}-${month}-${day} ${time}`
  return `${year}-${month}-${day}`
}

export const trimQuery = (query: string) =>
  query
    .split('\n')
    .map((s) => s.trim())
    .join('\n')
    .replace(/(^\n)|(\n$)/g, '')
    .replace(/^\n/gm, '')
    .replace(/\(tab\)/g, '  ')
    .replace(/\n$/, '')
    .replace(/,$/, '')
