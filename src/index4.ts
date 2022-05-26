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
  isChosen: false,
  dateSetting: {
    startDate: new Date(2022, 4, 18),
    endDate: new Date(2022, 4, 19),
  },
  tesCase: [{ value: '510_FlatSat', label: '510_FlatSat' }],
  tlm: [
    { tlmId: 1, tlmList: ['PCDU_BAT_CURRENT', 'PCDU_BAT_VOLTAGE'] },
    { tlmId: 2, tlmList: ['OBC_AD590_01', 'OBC_AD590_02'] },
  ],
}

const startDateStr = getStringFromUTCDateFixedTime(request.dateSetting.startDate, '00:00:00')
const endDateStr = getStringFromUTCDateFixedTime(request.dateSetting.endDate, '23:59:59')

const tlmList = request.tlm.map((e) => e.tlmList).flat()
const queryObjectSqliteList = tlmList.map((tlm) => {
  return {
    tlmName: tlm,
    query: queryTrim(`
    SELECT DISTINCT
    (tab)DATE,
    (tab)${tlm}
    FROM tlm
    WHERE
    (tab)DATE BETWEEN \'${startDateStr}\' AND \'${endDateStr}\'
    ${request.isStored ? '(tab)AND is_stored = 1' : '(tab)AND is_stored = 0'}
    ORDER BY DATE
  `),
  }
})

console.time('test')

export type querySuccess<T> = { success: true; tlmId?: number; data: T }
export type queryError = { success: false; tlmId?: number; error: string }
export type queryReturnType<T> = querySuccess<T> | queryError

const regexGroundTestDateTime =
  /^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) ([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]/
export const groundDateTimeTypeSchema = z.string().regex(regexGroundTestDateTime)

export const groundDataTypeSchema = z.union([z.number().nullable(), z.string()])
export const groundObjectTypeSchema = z.record(groundDataTypeSchema)

export const groundArrayObjectSchema = z.array(groundObjectTypeSchema)
export const groundObjectArrayTypeSchema = z.record(z.array(groundDataTypeSchema))
export const groundObjectArrayIncludingDateTimeTypeSchema = z
  .object({ DATE: z.array(z.string()) })
  .and(groundObjectArrayTypeSchema)

export type GroundDateArrayType = z.infer<typeof groundDateTimeTypeSchema>
export type GroundDataType = z.infer<typeof groundDataTypeSchema>
export type GroundArrayObjectType = z.infer<typeof groundArrayObjectSchema>
export type GroundObjectArrayType = z.infer<typeof groundObjectArrayTypeSchema>
export type GroundObjectArrayIncludingDateTimeType = z.infer<typeof groundObjectArrayIncludingDateTimeTypeSchema>

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

const readDbSync = async (path: string, query: string): Promise<GroundObjectArrayIncludingDateTimeType> =>
  new Promise((resolve) => {
    const db = new sqlite3.Database(path)
    db.serialize(() => {
      db.all(query, (_err, records) => {
        const schemaResult = groundArrayObjectSchema.safeParse(records)
        if (schemaResult.success) {
          const data = toObjectArray(schemaResult.data)
          if (data) {
            resolve(data)
          }
        } else {
          console.log(schemaResult.error.issues)
        }
      })
    })
  })

Promise.all(
  queryObjectSqliteList.map(async (queryObject) => {
    const dbPath = join(DB_TOP_PATH, request.project, `${queryObject.tlmName}.db`)
    const data = await readDbSync(dbPath, queryObject.query)
    return {
      [queryObject.tlmName]: { time: data.DATE, data: data[queryObject.tlmName] },
    }
  })
).then((responses) => {
  console.log(responses)
  console.timeEnd('test')
})
