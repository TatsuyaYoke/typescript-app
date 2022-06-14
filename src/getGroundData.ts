import { join } from 'path'
import * as glob from 'glob'
import sqlite3 from 'sqlite3'
import * as z from 'zod'
import {
  ArrayObjectType,
  groundArrayObjectTypeSchema,
  isNotNumber,
  nonNullable,
  ObjectArrayIncludingDateTimeType,
  QueryReturnType,
  RequestDataType,
  ResponseDataType,
} from './types'
import { getStringFromUTCDateFixedTime, trimQuery, uniqueArray } from './function'

export const readGroundTablesSync = (
  path: string
): Promise<{ success: true; data: { path: string; tableList: string[] } } | { success: false; error: string }> =>
  new Promise((resolve) => {
    const db = new sqlite3.Database(path)
    db.serialize(() => {
      db.all("SELECT * FROM sqlite_master WHERE type='table'", (error, records) => {
        if (error) {
          resolve({
            success: false,
            error: error.message,
          })
          return
        }
        const tableList = records
          .map((e) => {
            const schema = z.string().regex(/^table/)
            const schemaResult = schema.safeParse(e.name)
            if (schemaResult.success) return schemaResult.data
            return null
          })
          .filter(nonNullable)
        if (tableList.length === 0) {
          resolve({
            success: false,
            error: 'Not found table',
          })
        } else {
          resolve({
            success: true,
            data: {
              path: path,
              tableList: tableList,
            },
          })
        }
      })
      db.close()
    })
  })

export const readGroundDbColumnsSync = (
  path: string,
  table: string,
  tlmAllList: string[]
): Promise<
  | { success: true; data: { path: string; table: string; existColumns: string[]; notExistColumns: string[] } }
  | { success: false; error: string }
> =>
  new Promise((resolve) => {
    const db = new sqlite3.Database(path)
    db.serialize(() => {
      db.get(`SELECT * FROM ${table}`, (error, record) => {
        if (error) {
          resolve({
            success: false,
            error: error.message,
          })
          return
        }

        const columns = Object.keys(record)
        if (columns.length !== 0) {
          let existColumns: string[] = []
          let notExistColumns: string[] = []
          tlmAllList.forEach((tlm) => {
            if (columns.includes(tlm)) {
              existColumns.push(tlm)
            } else {
              notExistColumns.push(tlm)
            }
          })

          resolve({
            success: true,
            data: {
              path: path,
              table: table,
              existColumns: existColumns,
              notExistColumns: notExistColumns,
            },
          })
        } else {
          resolve({
            success: false,
            error: 'Columns not found',
          })
        }
      })
      db.close()
    })
  })

export const toObjectArrayGround = (
  records: ArrayObjectType['ground'],
  notExistColumns: string[]
): ObjectArrayIncludingDateTimeType['ground'] | null => {
  const objectArray: ObjectArrayIncludingDateTimeType['ground'] = { DATE: [] }
  const keys = [...Object.keys(records[0] ?? {}), ...notExistColumns]
  const keysDATE = keys.filter((e) => e.indexOf('_DATE') !== -1)

  keys.forEach((key) => {
    objectArray[key] = []
  })

  records.forEach((record) => {
    const DATE = keysDATE
      .map((e) => record[e])
      .filter(isNotNumber)
      .filter(nonNullable)[0]

    if (DATE) {
      objectArray.DATE.push(DATE)
      keys.forEach((key) => {
        if (key.indexOf('_DATE') === -1) objectArray[key]?.push(record[key] ?? null)
      })
    }
  })
  return objectArray
}

export const readGroundDbSync = (queryObject: {
  path: string
  query: string
  notExistColumns: string[]
}): Promise<QueryReturnType<ObjectArrayIncludingDateTimeType['ground']>> => {
  const { path, query, notExistColumns } = queryObject
  return new Promise((resolve) => {
    const db = new sqlite3.Database(path)
    db.serialize(() => {
      db.all(query, (error, records) => {
        if (error) {
          resolve({
            success: false,
            error: error.message,
          })
          return
        }

        const schemaResult = groundArrayObjectTypeSchema.safeParse(records)
        if (!schemaResult.success) {
          resolve({
            success: false,
            error: `${JSON.stringify(schemaResult.error.issues[0])}`,
          })
          return
        }

        const data = toObjectArrayGround(schemaResult.data, notExistColumns)
        if (data) {
          resolve({
            success: true,
            data: data,
          })
          return
        }

        resolve({
          success: false,
          error: 'Cannot convert from arrayObject to objectArray',
        })
      })
      db.close()
    })
  })
}

export const getGroundData = async (request: RequestDataType, dbTopPath: string) => {
  const { groundTestPath, isStored, isChosen, dateSetting, testCase, tlm } = request
  const tlmAllList = tlm.map((e) => e.tlmList).flat()
  let dbPathList: string[] = []
  const globStored = isStored ? '_All_Telemetry_stored.db' : '_All_Telemetry.db'

  if (isChosen) {
    dbPathList = testCase
      .map((element) => glob.sync(join(dbTopPath, `../${groundTestPath}/${element.value}/**/*${globStored}`)))
      .flat()
  } else {
    let dayList: string[] = []
    const { startDate, endDate } = dateSetting
    const dbAllPathList = glob.sync(join(dbTopPath, `../${groundTestPath}/**/**/*.db`))
    for (let day = new Date(startDate.toDateString()); day <= endDate; day.setDate(day.getDate() + 1)) {
      const dayString = getStringFromUTCDateFixedTime(day)
      if (dbAllPathList.some((filePath) => filePath.indexOf(dayString) !== -1)) dayList.push(dayString)
    }
    dbPathList = dayList.map((day) => glob.sync(join(dbTopPath, `../${groundTestPath}/**/${day}/*${globStored}`))).flat()
  }

  const tableCheckResults = await Promise.all(dbPathList.map((dbPath) => readGroundTablesSync(dbPath)))
  const columnCheckResults = await Promise.all(
    tableCheckResults.map(async (result) => {
      if (result.success) {
        const { path, tableList } = result.data
        const columnCheckResult = await Promise.all(
          tableList.map((table) => readGroundDbColumnsSync(path, table, tlmAllList))
        )
        const existColumnsList = columnCheckResult
          .map((e) => (e.success ? { table: e.data.table, columns: e.data.existColumns } : null))
          .filter(nonNullable)
        const notExistColumns = columnCheckResult.map((e) => (e.success ? e.data.notExistColumns : [])).flat()
        const filteredNotExistColumns = notExistColumns.filter(
          (notExistColumn) =>
            !existColumnsList
              .map((existColumnObject) => existColumnObject.columns)
              .flat()
              .includes(notExistColumn)
        )
        if (columnCheckResult.every((e) => e.success)) {
          return {
            success: true,
            data: {
              path: result.data.path,
              existColumnsList: existColumnsList,
              notExistColumns: filteredNotExistColumns,
            },
          } as const
        } else {
          const errorMessages = columnCheckResult.map((e) => (!e.success ? e.error : null)).filter(nonNullable)
          return {
            success: false,
            errorMessages: errorMessages,
          } as const
        }
      } else {
        return {
          success: false,
          errorMessages: [result.error],
        } as const
      }
    })
  )

  const queryTargetList = columnCheckResults
    .map((result) => {
      if (result && result.success) {
        return result.data
      }
      return null
    })
    .filter(nonNullable)

  const errorMessages = columnCheckResults
    .map((result) => {
      if (result && !result.success) {
        return result.errorMessages
      }
      return null
    })
    .filter(nonNullable)
    .flat()

  const queryObjectList = queryTargetList.map((queryTarget) => {
    const queryWith = trimQuery(
      queryTarget.existColumnsList.reduce((prevQuery, currentElement) => {
        const { table, columns } = currentElement
        const tlmListQuery = columns
          .reduce((prev, current) => `${prev}\n(tab)(tab)(tab)${current},`, '\n(tab)(tab)(tab)DATE,')
          .replace(/,$/, '')

        return `${prevQuery}
        (tab)${table}_tlm as (
        (tab)(tab)SELECT DISTINCT${tlmListQuery}
        (tab)(tab)FROM ${table}
        (tab)(tab)ORDER BY DATE
        (tab)),
        `
      }, 'WITH\n')
    )
    const querySelect = trimQuery(
      queryTarget.existColumnsList.reduce((prevQuery, currentElement) => {
        const { table, columns } = currentElement
        const tlmListQuery = columns.reduce((prev, current) => `${prev}\n(tab)${current},`, '')
        return `${prevQuery}
        (tab)${table}_tlm.DATE AS ${table}_DATE,
        ${tlmListQuery}
        `
      }, 'SELECT\n')
    )

    const baseTable = queryTarget.existColumnsList[0]?.table
    const queryJoin = trimQuery(
      queryTarget.existColumnsList.slice(1, queryTarget.existColumnsList.length).reduce((prevQuery, currentElement) => {
        const { table } = currentElement
        return `${prevQuery}
      LEFT OUTER JOIN ${table}_tlm
      (tab)ON ${baseTable}_tlm.DATE = ${table}_tlm.DATE
      `
      }, `FROM ${baseTable}_tlm`)
    )

    const query = `${queryWith}\n${querySelect}\n${queryJoin}`
    return {
      path: queryTarget.path,
      query: query,
      notExistColumns: queryTarget.notExistColumns,
    }
  })

  const responsesFromDb = await Promise.all(queryObjectList.map((object) => readGroundDbSync(object)))
  const responseData: ResponseDataType = {
    success: true,
    tlm: { time: [], data: {} },
    errorMessages: [],
  }

  tlmAllList.forEach((tlmName) => {
    responseData.tlm.data[tlmName] = []
  })

  responsesFromDb.forEach((responseFromDb) => {
    if (responseFromDb.success) {
      responseData.tlm.time.push(...responseFromDb.data.DATE)
      tlmAllList.forEach((tlmName) => {
        const data = responseFromDb.data[tlmName]
        if (data) {
          const dataNotIncludingString = data.map((d) => {
            if (typeof(d) === 'string') return null
            return d
          })
          responseData.tlm.data[tlmName]?.push(...dataNotIncludingString)
        }
      })
    } else {
      errorMessages.push(responseFromDb.error)
    }
  })
  if (responseData.tlm.time.length === 0) responseData.success = false
  responseData.errorMessages = uniqueArray(errorMessages)
  return responseData
}
