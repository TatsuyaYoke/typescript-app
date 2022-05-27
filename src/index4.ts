import { join } from 'path'
import sqlite3 from 'sqlite3'
import * as z from 'zod'

const DB_TOP_PATH = 'G:/共有ドライブ/0705_Sat_Dev_Tlm/db'

const getStringFromUTCDateFixedTime = (date: Date, time: string) => {
  const year = date.getUTCFullYear().toString()
  const month = ('0' + (date.getUTCMonth() + 1)).slice(-2)
  const day = ('0' + date.getUTCDate()).slice(-2)
  return `${year}-${month}-${day} ${time}`
}

const queryTrim = (query: string) =>
  query
    .split('\n')
    .map((s) => s.trim())
    .join('\n')
    .replace(/(^\n)|(\n$)/g, '')
    .replace(/^\n/gm, '')
    .replace(/\(tab\)/g, '  ')
    .replace(/,$/, '')

const request = {
  project: 'DSX0201',
  isOrbit: false,
  bigqueryTable: 'strix_b_telemetry_v_6_17',
  isStored: false,
  isChosen: true,
  dateSetting: {
    startDate: new Date(2022, 4, 18),
    endDate: new Date(2022, 4, 19),
  },
  tesCase: [
    { value: '510_FlatSat', label: '510_FlatSat' },
    { value: '511_Hankan_Test', label: '511_Hankan_Test' },
  ],
  tlm: [
    { tlmId: 1, tlmList: ['PCDU_BAT_CURRENT', 'PCDU_BAT_VOLTAGE'] },
    { tlmId: 2, tlmList: ['OBC_AD590_01', 'OBC_AD590_02'] },
  ],
}

const startDateStr = getStringFromUTCDateFixedTime(request.dateSetting.startDate, '00:00:00')
const endDateStr = getStringFromUTCDateFixedTime(request.dateSetting.endDate, '23:59:59')

const tlmList = request.tlm.map((e) => e.tlmList).flat()
const queryObjectGroundList = tlmList.map((tlm) => {
  const queryTestCase = request.tesCase
    .reduce((prev, current) => {
      return `${prev}test_case = \'${current.value}\' OR `
    }, '')
    .replace(/ OR $/, '')

  return {
    tlmName: tlm,
    query: queryTrim(`
    SELECT DISTINCT
    (tab)DATE,
    (tab)${tlm}
    FROM tlm
    WHERE
    (tab)${!request.isChosen ? `DATE BETWEEN \'${startDateStr}\' AND \'${endDateStr}\'` : queryTestCase}
    ${request.isStored ? '(tab)AND is_stored = 1' : '(tab)AND is_stored = 0'}
    ORDER BY DATE
  `),
  }
})
console.log(queryObjectGroundList)
export type querySuccess<T> = { success: true; tlmId?: number; tlmName?: string; data: T }
export type queryError = { success: false; tlmId?: number; tlmName?: string; error: string }
export type queryReturnType<T> = querySuccess<T> | queryError

const regexGroundTestDateTime =
  /^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) ([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]/
export const groundDateTimeTypeSchema = z.string().regex(regexGroundTestDateTime)

export const groundDataTypeSchema = z.union([z.number().nullable(), groundDateTimeTypeSchema])
export const groundObjectTypeSchema = z.record(groundDataTypeSchema)

export const groundArrayObjectSchema = z.array(groundObjectTypeSchema)
export const groundObjectArrayTypeSchema = z.record(z.array(groundDataTypeSchema))
export const groundObjectArrayIncludingDateTimeTypeSchema = z
  .object({ DATE: z.array(z.string()) })
  .and(groundObjectArrayTypeSchema)

export type GroundDateTimeType = z.infer<typeof groundDateTimeTypeSchema>
export type GroundDataType = z.infer<typeof groundDataTypeSchema>
export type GroundArrayObjectType = z.infer<typeof groundArrayObjectSchema>
export type GroundObjectArrayType = z.infer<typeof groundObjectArrayTypeSchema>
export type GroundObjectArrayIncludingDateTimeType = z.infer<typeof groundObjectArrayIncludingDateTimeTypeSchema>

export type responseDataType = {
  tlm: {
    [key: string]: {
      time: GroundDateTimeType[]
      data: GroundDataType[]
    }
  }
  errorMessages: string[]
}

const includeDate = (
  value: GroundObjectArrayType | GroundObjectArrayIncludingDateTimeType
): value is GroundObjectArrayIncludingDateTimeType => {
  if ((value as GroundObjectArrayIncludingDateTimeType).DATE !== undefined) return true
  return false
}

export const toObjectArray = (records: GroundArrayObjectType): GroundObjectArrayIncludingDateTimeType | null => {
  const objectArray: GroundObjectArrayType = {}
  const keys = Object.keys(records[0] ?? {})
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

const readGroundDbSync = async (
  path: string,
  query: string,
  tlmName: string
): Promise<queryReturnType<GroundObjectArrayIncludingDateTimeType>> =>
  new Promise((resolve) => {
    const db = new sqlite3.Database(path)
    db.serialize(() => {
      db.all(query, (error, records) => {
        if (error) {
          resolve({
            success: false,
            tlmName: tlmName,
            error: `${tlmName}: ${error.message}`,
          })
          return
        }

        const schemaResult = groundArrayObjectSchema.safeParse(records)
        if (!schemaResult.success) {
          resolve({
            success: false,
            tlmName: tlmName,
            error: `${tlmName}: ${JSON.stringify(schemaResult.error.issues[0])}`,
          })
          return
        }

        const data = toObjectArray(schemaResult.data)
        if (data) {
          resolve({
            success: true,
            tlmName: tlmName,
            data: data,
          })
          return
        }

        resolve({
          success: false,
          tlmName: tlmName,
          error: `${tlmName}: Cannot convert from arrayObject to objectArray`,
        })
      })
    })
  })

console.time('test')
Promise.all(
  queryObjectGroundList.map(async (queryObject) => {
    const dbPath = join(DB_TOP_PATH, request.project, `${queryObject.tlmName}.db`)
    return await readGroundDbSync(dbPath, queryObject.query, queryObject.tlmName)
  })
).then((responses) => {
  const responseData: responseDataType = { tlm: {}, errorMessages: [] }
  responses.forEach((response) => {
    if (response.success) {
      const tlmName = response.tlmName
      const data = tlmName ? response.data[tlmName] : null
      if (data && tlmName) responseData.tlm[tlmName] = { time: response.data.DATE, data: data }
    } else {
      const error = response.error
      responseData.errorMessages.push(error)
    }
  })
  console.log(responseData.tlm['PCDU_BAT_CURRENT'])
  console.timeEnd('test')
})
