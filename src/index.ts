import * as z from 'zod'
import { BigQuery } from '@google-cloud/bigquery'
import { join } from 'path'
import sqlite3 from 'sqlite3'
import * as dotenv from 'dotenv'
dotenv.config()

const DB_TOP_PATH = process.env.DB_TOP_PATH ?? ''
const BIGQUERY_PROJECT = process.env.BIGQUERY_PROJECT
const OBCTIME_INITIAL = process.env.OBCTIME_INITIAL
const SETTING_PATH = process.env.SETTING_PATH ?? ''

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
  tesCase: selectOptionType[]
  tlm: requestTlmType[]
}

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

const mode = ['orbit', 'ground'] as const
export type mode = typeof mode[number]

export type querySuccess<T> = {
  orbit: { success: true; tlmId: number; data: T }
  ground: { success: true; tlmName: string; data: T }
}
export type queryError = {
  orbit: { success: false; tlmId: number; error: string }
  ground: { success: false; tlmName: string; error: string }
}
export type queryReturnType<T, U extends mode> = querySuccess<T>[U] | queryError[U]

const regexGroundTestDateTime =
  /^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) ([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]/
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

const includeObcTime = (value: unknown): value is ObjectArrayIncludingDateTimeType['orbit'] => {
  if ((value as ObjectArrayIncludingDateTimeType['orbit']).OBCTimeUTC !== undefined) return true
  return false
}

export const toObjectArrayOrbit = (
  records: ArrayObjectType['orbit']
): ObjectArrayIncludingDateTimeType['orbit'] | null => {
  const objectArray: ObjectArrayType['orbit'] = {}
  const keys = Object.keys(records[0] ?? {})
  keys.forEach((key) => {
    objectArray[key] = []
  })
  records.forEach((record) => {
    keys.forEach((key) => {
      objectArray[key]?.push(record[key] ?? null)
    })
  })
  if (includeObcTime(objectArray)) {
    return objectArray
  }
  return null
}

const includeDate = (value: unknown): value is ObjectArrayIncludingDateTimeType['ground'] => {
  if ((value as ObjectArrayIncludingDateTimeType['ground']).DATE !== undefined) return true
  return false
}

export const toObjectArrayGround = (
  records: ArrayObjectType['ground']
): ObjectArrayIncludingDateTimeType['ground'] | null => {
  const objectArray: ObjectArrayType['ground'] = {}
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

const readOrbitDbSync = (
  path: string,
  query: string,
  tlmId: number
): Promise<queryReturnType<ObjectArrayIncludingDateTimeType['orbit'], 'orbit'>> => {
  const bigquery = new BigQuery({
    keyFilename: path,
  })

  return bigquery
    .query(query)
    .then((data) => {
      const schemaResult = orbitArrayObjectTypeSchema.safeParse(data[0])
      if (schemaResult.success) {
        const convertedData = toObjectArrayOrbit(schemaResult.data)
        if (convertedData)
          return {
            success: true,
            tlmId: tlmId,
            data: convertedData,
          } as const

        return {
          success: false,
          tlmId: tlmId,
          error: `tlmId${tlmId}: Cannot convert from arrayObject to objectArray`,
        } as const
      }

      return {
        success: false,
        tlmId: tlmId,
        error: `tlmId${tlmId}: ${JSON.stringify(schemaResult.error.issues[0])}`,
      } as const
    })
    .catch((err) => {
      return {
        success: false,
        tlmId: tlmId,
        error: `tlmId${tlmId}: ${JSON.stringify(err.errors[0])}`,
      } as const
    })
}

const readGroundDbSync = (
  path: string,
  query: string,
  tlmName: string
): Promise<queryReturnType<ObjectArrayIncludingDateTimeType['ground'], 'ground'>> =>
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

        const schemaResult = groundArrayObjectTypeSchema.safeParse(records)
        if (!schemaResult.success) {
          resolve({
            success: false,
            tlmName: tlmName,
            error: `${tlmName}: ${JSON.stringify(schemaResult.error.issues[0])}`,
          })
          return
        }

        const data = toObjectArrayGround(schemaResult.data)
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

const getOrbitData = (request: requestDataType) => {
  const startDateStr = getStringFromUTCDateFixedTime(request.dateSetting.startDate, '00:00:00')
  const endDateStr = getStringFromUTCDateFixedTime(request.dateSetting.endDate, '23:59:59')
  const queryObjectList = request.tlm.map((currentElement) => {
    const datasetTableQuery = `\n(tab)\`${BIGQUERY_PROJECT}.${request.bigqueryTable}.tlm_id_${currentElement.tlmId}\``
    const tlmListQuery = currentElement.tlmList.reduce(
      (prev, current) => `${prev}\n(tab)${current},`,
      `
    (tab)OBCTimeUTC,
    (tab)CalibratedOBCTimeUTC,
    `
    )
    const whereQuery = `
      (tab)CalibratedOBCTimeUTC > \'${OBCTIME_INITIAL}\'
      (tab)AND OBCTimeUTC BETWEEN \'${startDateStr}\' AND \'${endDateStr}\'
      ${request.isStored ? '(tab)AND Stored = True' : '(tab)AND Stored = False'}
      `

    const query = queryTrim(`
    SELECT DISTINCT${tlmListQuery}
    FROM${datasetTableQuery}
    WHERE${whereQuery}
    ORDER BY OBCTimeUTC
  `)

    return {
      tlmId: currentElement.tlmId,
      query: query,
    }
  })

  return Promise.all(
    queryObjectList.map((element) => readOrbitDbSync(SETTING_PATH, element.query, element.tlmId))
  ).then((responses) => {
    const responseData: responseDataType<'orbit'> = { tlm: {}, warningMessages: [] }
    responses.forEach((response) => {
      const tlmIdIndex = request.tlm.findIndex((e) => e.tlmId === response.tlmId)
      const tlmListEachTlmId = request.tlm[tlmIdIndex]?.tlmList
      if (response.success && tlmListEachTlmId) {
        tlmListEachTlmId.forEach((tlm) => {
          const data = response.data[tlm]
          if (data) responseData.tlm[tlm] = { time: response.data.OBCTimeUTC, data: data }
        })
      } else if (!response.success && tlmListEachTlmId) {
        const error = response.error
        responseData.warningMessages.push(error)
      }
    })
    return responseData
  })
}

const getGroundData = (request: requestDataType) => {
  const tlmList = request.tlm.map((e) => e.tlmList).flat()
  const startDateStr = getStringFromUTCDateFixedTime(request.dateSetting.startDate, '00:00:00')
  const endDateStr = getStringFromUTCDateFixedTime(request.dateSetting.endDate, '23:59:59')
  const queryObjectList = tlmList.map((tlm) => {
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
  return Promise.all(
    queryObjectList.map(async (queryObject) => {
      const dbPath = join(DB_TOP_PATH, request.project, `${queryObject.tlmName}.db`)
      return await readGroundDbSync(dbPath, queryObject.query, queryObject.tlmName)
    })
  ).then((responses) => {
    const responseData: responseDataType<'ground'> = { tlm: {}, warningMessages: [] }
    responses.forEach((response) => {
      if (response.success) {
        const tlmName = response.tlmName
        const data = tlmName ? response.data[tlmName] : null
        if (data && tlmName) responseData.tlm[tlmName] = { time: response.data.DATE, data: data }
      } else {
        const error = response.error
        responseData.warningMessages.push(error)
      }
    })
    return responseData
  })
}

const getData = async (request: requestDataType) => {
  if (isOrbit) {
    const response = await getOrbitData(request)
    console.log(response)
    console.timeEnd('test')
    return response
  } else {
    const response = await getGroundData(request)
    console.log(response)
    console.timeEnd('test')
    return response
  }
}

const isOrbit = true
const request = {
  project: 'DSX0201',
  isOrbit: isOrbit,
  bigqueryTable: 'strix_b_telemetry_v_6_17',
  isStored: false,
  isChosen: true,
  dateSetting: {
    startDate: isOrbit ? new Date(2022, 3, 28) : new Date(2022, 4, 18),
    endDate: isOrbit ? new Date(2022, 3, 28) : new Date(2022, 4, 19),
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

console.time('test')
getData(request)
