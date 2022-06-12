import { join } from 'path'
import * as dotenv from 'dotenv'
import * as glob from 'glob'
import sqlite3 from 'sqlite3'
import * as z from 'zod'
dotenv.config()

export const DB_TOP_PATH = process.env.DB_TOP_PATH ?? ''

export const isNotNull = <T>(item: T): item is Exclude<T, null> => item !== null
export const isNotUndefined = <T>(item: T): item is Exclude<T, undefined> => item !== undefined
export const isNotNumber = <T>(item: T): item is Exclude<T, number> => typeof item !== 'number'

export type selectOptionType = {
  label: string
  value: string
}
export type dateSettingType = {
  startDate: Date
  endDate: Date
}
export type requestTlmType = {
  tlmId: number
  tlmList: string[]
}
export type requestDataType = {
  project: string
  isOrbit: boolean
  isStored: boolean
  isChosen: boolean
  bigqueryTable: string
  dateSetting: dateSettingType
  testCase: selectOptionType[]
  tlm: requestTlmType[]
}

const mode = ['orbit', 'ground'] as const
export type mode = typeof mode[number]

export type querySuccess<T> = { success: true; data: T }
export type queryError = { success: false; error: string }
export type queryReturnType<T> = querySuccess<T> | queryError

const regexGroundTestDateTime =
  /^[0-9]{4}(-|\/)(0?[1-9]|1[0-2])(-|\/)(0?[1-9]|[12][0-9]|3[01]) ([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]/
export const groundDateTimeTypeSchema = z.string().regex(regexGroundTestDateTime)

export const groundDataTypeSchema = z.union([z.number().nullable(), groundDateTimeTypeSchema])
export const groundObjectTypeSchema = z.record(groundDataTypeSchema)

export const groundArrayObjectTypeSchema = z.array(groundObjectTypeSchema)
export const groundObjectArrayTypeSchema = z.record(z.array(groundDataTypeSchema))
export const groundObjectArrayIncludingDateTimeTypeSchema = z
  .object({ DATE: z.array(z.string()) })
  .and(groundObjectArrayTypeSchema)

const regexOrbitDateTime =
  /^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9].[0-9]{3}Z/
export const orbitDateTimeTypeSchema = z
  .object({ value: z.string().regex(regexOrbitDateTime) })
  .transform((e) => e.value)

export const orbitDataTypeSchema = z.union([z.number().nullable(), orbitDateTimeTypeSchema])
export const orbitObjectTypeSchema = z.record(orbitDataTypeSchema)

export const orbitArrayObjectTypeSchema = z.array(orbitObjectTypeSchema)
export const orbitObjectArrayTypeSchema = z.record(z.array(orbitDataTypeSchema))
export const orbitObjectArrayIncludingDateTimeTypeSchema = z
  .object({
    OBCTimeUTC: z.array(orbitDateTimeTypeSchema),
    CalibratedOBCTimeUTC: z.array(orbitDateTimeTypeSchema),
  })
  .and(orbitObjectArrayTypeSchema)

export type DateTimeType = {
  orbit: z.infer<typeof orbitDateTimeTypeSchema>
  ground: z.infer<typeof groundDateTimeTypeSchema>
}
export type DataType = {
  orbit: z.infer<typeof orbitDataTypeSchema>
  ground: z.infer<typeof groundDataTypeSchema>
}
export type ArrayObjectType = {
  orbit: z.infer<typeof orbitArrayObjectTypeSchema>
  ground: z.infer<typeof groundArrayObjectTypeSchema>
}
export type ObjectArrayType = {
  orbit: z.infer<typeof orbitObjectArrayTypeSchema>
  ground: z.infer<typeof groundObjectArrayTypeSchema>
}
export type ObjectArrayIncludingDateTimeType = {
  orbit: z.infer<typeof orbitObjectArrayIncludingDateTimeTypeSchema>
  ground: z.infer<typeof groundObjectArrayIncludingDateTimeTypeSchema>
}

export type responseDataType<T extends mode> = {
  tlm: {
    [key: string]: {
      time: DateTimeType[T][]
      data: DataType[T][]
    }
  }
  warningMessages: string[]
}
export const getStringFromUTCDateFixedTime = (date: Date, time?: string) => {
  const year = date.getUTCFullYear().toString()
  const month = ('0' + (date.getUTCMonth() + 1)).slice(-2)
  const day = ('0' + date.getUTCDate()).slice(-2)
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
    .replace(/,$/, '')

export const readGroundTablesSync = (
  path: string
): Promise<{ success: true; data: { path: string; tableList: string[] } } | { success: false; error: string }> =>
  new Promise((resolve) => {
    const db = new sqlite3.Database(path)
    db.serialize(() => {
      let results: any[] = []
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
          .filter(isNotNull)
        if (tableList.length === 0) {
          resolve({
            success: false,
            error: 'Not found table',
          })
        } else {
          results.push(tableList)
          resolve({
            success: true,
            data: {
              path: path,
              tableList: tableList,
            },
          })
        }
      })
      db.get('SELECT * FROM table1', (_error, record) => {
        console.log(results)
        console.log(Object.keys(record)[0])
      })
      db.close()
    })
  })

export const readGroundDbColumnsSync = (
  path: string,
  table: string,
  tlmAllList: string[]
): Promise<
  | { success: true; data: { path: string; existColumns: string[]; notExistColumns: string[] } }
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

const includeDate = (value: unknown): value is ObjectArrayIncludingDateTimeType['ground'] => {
  if ((value as ObjectArrayIncludingDateTimeType['ground']).DATE !== undefined) return true
  return false
}

export const toObjectArrayGround = (
  records: ArrayObjectType['ground'],
  notExistColumns: string[]
): ObjectArrayIncludingDateTimeType['ground'] | null => {
  const objectArray: ObjectArrayType['ground'] = {}
  const keys = [...Object.keys(records[0] ?? {}), ...notExistColumns]
  keys.forEach((key) => {
    objectArray[key] = []
  })
  records.forEach((record) => {
    keys.forEach((key) => {
      objectArray[key]?.push(record[key] ?? null)
    })
  })
  if (includeDate(objectArray)) {
    return objectArray
  }
  return null
}

export const readGroundDbSync = (queryObject: {
  path: string
  query: string
  notExistColumns: string[]
}): Promise<queryReturnType<ObjectArrayIncludingDateTimeType['ground']>> => {
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

const getGroundData = async (request: requestDataType) => {
  const { project, isStored, isChosen, dateSetting, testCase, tlm } = request
  const tlmAllList = tlm.map((e) => e.tlmList).flat()
  let dbPathList: string[] = []
  const globStored = isStored ? '*_All_Telemetry_stored.db' : '*_All_Telemetry.db'

  if (isChosen) {
    dbPathList = testCase
      .map((element) => glob.sync(join(DB_TOP_PATH, `../${project}/500_SystemFM/${element.value}/**/${globStored}`)))
      .flat()
  } else {
    let dayList: string[] = []
    const { startDate, endDate } = dateSetting
    const dbAllPathList = glob.sync(join(DB_TOP_PATH, `../${project}/500_SystemFM/**/**/*.db`))
    for (let day = startDate; day <= endDate; day.setDate(day.getDate() + 1)) {
      const dayString = getStringFromUTCDateFixedTime(day)
      if (dbAllPathList.some((filePath) => filePath.indexOf(dayString) !== -1)) dayList.push(dayString)
    }
    dbPathList = dayList
      .map((day) => glob.sync(join(DB_TOP_PATH, `../${project}/500_SystemFM/**/${day}/${globStored}`)))
      .flat()
  }

  const tableCheckResults = await Promise.all(dbPathList.map(async (dbPath) => await readGroundTablesSync(dbPath)))
  const columnCheckResults = await Promise.all(
    tableCheckResults.map(async (result) => {
      if (result.success) {
        const { path, tableList } = result.data
        const columnCheckResult = await Promise.all(
          tableList.map((table) => readGroundDbColumnsSync(path, table, tlmAllList))
        )
        const existColumnsList = columnCheckResult.map((e) => (e.success ? e.data.existColumns : []))
        const notExistColumns = columnCheckResult.map((e) => (e.success ? e.data.notExistColumns : [])).flat()
        const filteredNotExistColumns = notExistColumns.filter((e) => !existColumnsList.flat().includes(e))
        if (columnCheckResult.every((e) => e.success)) {
          return {
            success: true,
            data: {
              existColumnsList: existColumnsList,
              notExistColumns: filteredNotExistColumns,
            },
          } as const
        } else {
          const errorMessages = columnCheckResult.map((e) => (!e.success ? e.error : null)).filter(isNotNull)
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
  columnCheckResults.forEach((result) => {
    if (result && result.success) {
      console.log(result.data)
    } else {
      console.log(result.errorMessages)
    }
  })

  // const queryObjectList = columnsCheckResult
  //   .map((object) => {
  //     if (!object.success) return null
  //     const { path, existColumns, notExistColumns } = object.data
  //     const tlmListQuery = existColumns
  //       .reduce((prev, current) => `${prev}\n(tab)${current},`, '(tab)DATE,')
  //       .replace(/,$/, '')
  //     const query = trimQuery(`
  //     SELECT DISTINCT
  //     ${tlmListQuery}
  //     FROM table1
  //     ORDER BY DATE
  //   `)
  //     return {
  //       path: path,
  //       query: query,
  //       notExistColumns: notExistColumns,
  //     }
  //   })
  //   .filter(isNotNull)

  // const queryResponses = await Promise.all(
  //   queryObjectList.map(async (queryObject) => await readGroundDbSync(queryObject))
  // )
  // const data: ObjectArrayIncludingDateTimeType['ground'] = { DATE: [] }
  // const keys = queryResponses[0]?.success ? Object.keys(queryResponses[0].data) : []
  // keys.forEach((key) => {
  //   const dataEachTlm = queryResponses
  //     .map((response) => {
  //       if (response.success) {
  //         return response.data[key]
  //       }
  //       return null
  //     })
  //     .filter(isNotNull)
  //     .filter(isNotUndefined)
  //     .flat()
  //   data[key] = dataEachTlm
  // })
  // return data
}

const isOrbit = false
const request = {
  project: 'DSX0201',
  isOrbit: isOrbit,
  bigqueryTable: 'strix_b_telemetry_v_6_17',
  isStored: false,
  isChosen: false,
  dateSetting: {
    // startDate: isOrbit ? new Date(2022, 3, 28) : new Date(2022, 4, 18),
    // endDate: isOrbit ? new Date(2022, 3, 28) : new Date(2022, 4, 19),
    startDate: isOrbit ? new Date(2022, 3, 28) : new Date(2021, 10, 11),
    endDate: isOrbit ? new Date(2022, 3, 28) : new Date(2021, 10, 17),
  },
  testCase: [
    { value: '510_FlatSat', label: '510_FlatSat' },
    { value: '511_Hankan_Test', label: '511_Hankan_Test' },
  ],
  tlm: [
    { tlmId: 1, tlmList: ['PCDU_BAT_CURRENT', 'PCDU_BAT_VOLTAGE', 'AB_DSS2_ONOFF'] },
    { tlmId: 2, tlmList: ['OBC_AD590_01', 'OBC_AD590_02'] },
  ],
}

getGroundData(request).then((response) => console.log(response))
