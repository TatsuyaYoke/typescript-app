import * as z from 'zod'
import { BigQuery } from '@google-cloud/bigquery'
import { join } from 'path'
import sqlite3 from 'sqlite3'

const DB_TOP_PATH = 'G:/共有ドライブ/0705_Sat_Dev_Tlm/db'
const BIGQUERY_PROJECT = 'syns-sol-grdsys-external-prod'
const OBCTIME_INITIAL = '2016-1-1 00:00:00 UTC'
const SETTING_PATH = 'G:/共有ドライブ/0705_Sat_Dev_Tlm/settings/strix-tlm-bq-reader-service-account.json'

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

let request: requestDataType
const isOrbitSetting = false

if (isOrbitSetting) {
  request = {
    project: 'DSX0201',
    isOrbit: isOrbitSetting,
    bigqueryTable: 'strix_b_telemetry_v_6_17',
    isStored: true,
    isChosen: false,
    dateSetting: {
      startDate: new Date(2022, 3, 28),
      endDate: new Date(2022, 3, 28),
    },
    tesCase: [{ value: '510_FlatSat', label: '510_FlatSat' }],
    tlm: [
      { tlmId: 1, tlmList: ['PCDU_BAT_CURRENT', 'PCDU_BAT_VOLTAGE'] },
      { tlmId: 2, tlmList: ['OBC_AD590_01', 'OBC_AD590_02'] },
    ],
  }
} else {
  request = {
    project: 'DSX0201',
    isOrbit: isOrbitSetting,
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

const startDateStr = getStringFromUTCDateFixedTime(request.dateSetting.startDate, '00:00:00')
const endDateStr = getStringFromUTCDateFixedTime(request.dateSetting.endDate, '23:59:59')
const tlmList = request.tlm.map((e) => e.tlmList).flat()

const queryObjectOrbitList = request.tlm.map((currentElement) => {
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

export type querySuccess<T, IsOrbit extends boolean = false> = IsOrbit extends true
  ? { success: true; tlmId: number; data: T }
  : { success: true; tlmName: string; data: T }
export type queryError<IsOrbit extends boolean = false> = IsOrbit extends true
  ? { success: false; tlmId: number; error: string }
  : { success: false; tlmName: string; error: string }
export type queryReturnType<T, IsOrbit extends boolean = false> = querySuccess<T, IsOrbit> | queryError<IsOrbit>

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

export type OrbitDateTimeType = z.infer<typeof orbitDateTimeTypeSchema>
export type OrbitDataType = z.infer<typeof orbitDataTypeSchema>
export type OrbitArrayObjectType = z.infer<typeof orbitArrayObjectTypeSchema>
export type OrbitObjectArrayType = z.infer<typeof orbitObjectArrayTypeSchema>
export type OrbitObjectArrayIncludingDateTimeType = z.infer<typeof orbitObjectArrayIncludingDateTimeTypeSchema>

export type DateTimeType<IsOrbit extends boolean = false> = IsOrbit extends true
  ? OrbitDateTimeType
  : GroundDateTimeType
export type DataType<IsOrbit extends boolean = false> = IsOrbit extends true ? OrbitDataType : GroundDataType
export type ArrayObjectType<IsOrbit extends boolean = false> = IsOrbit extends true
  ? OrbitArrayObjectType
  : GroundArrayObjectType
export type ObjectArrayType<IsOrbit extends boolean = false> = IsOrbit extends true
  ? OrbitObjectArrayType
  : GroundObjectArrayType
export type ObjectArrayIncludingDateTimeType<IsOrbit extends boolean = false> = IsOrbit extends true
  ? OrbitObjectArrayIncludingDateTimeType
  : GroundObjectArrayIncludingDateTimeType

export type responseDataType<IsOrbit extends boolean> = {
  tlm: {
    [key: string]: {
      time: DateTimeType<IsOrbit>[]
      data: DataType<IsOrbit>[]
    }
  }
  errorMessages: string[]
}

const includeObcTime = (
  value: OrbitObjectArrayType | OrbitObjectArrayIncludingDateTimeType
): value is OrbitObjectArrayIncludingDateTimeType => {
  if ((value as OrbitObjectArrayIncludingDateTimeType).OBCTimeUTC !== undefined) return true
  return false
}

export const toObjectArrayOrbit = (records: OrbitArrayObjectType): OrbitObjectArrayIncludingDateTimeType | null => {
  const objectArray: OrbitObjectArrayType = {}
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

const includeDate = (
  value: GroundObjectArrayType | GroundObjectArrayIncludingDateTimeType
): value is GroundObjectArrayIncludingDateTimeType => {
  if ((value as GroundObjectArrayIncludingDateTimeType).DATE !== undefined) return true
  return false
}

export const toObjectArrayGround = (records: GroundArrayObjectType): GroundObjectArrayIncludingDateTimeType | null => {
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

const readOrbitDbSync = (
  path: string,
  query: string,
  tlmId: number
): Promise<queryReturnType<OrbitObjectArrayIncludingDateTimeType, true>> => {
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

const getOrbitData = () => {
  Promise.all(queryObjectOrbitList.map((element) => readOrbitDbSync(SETTING_PATH, element.query, element.tlmId))).then(
    (responses) => {
      const responseData: responseDataType<true> = { tlm: {}, errorMessages: [] }
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
          responseData.errorMessages.push(error)
        }
      })

      console.log(responseData)
      console.timeEnd('test')
    }
  )
}

const getGroundData = () => {
  Promise.all(
    queryObjectGroundList.map(async (queryObject) => {
      const dbPath = join(DB_TOP_PATH, request.project, `${queryObject.tlmName}.db`)
      return await readGroundDbSync(dbPath, queryObject.query, queryObject.tlmName)
    })
  ).then((responses) => {
    const responseData: responseDataType<false> = { tlm: {}, errorMessages: [] }
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
}

const getData = (isOrbit: boolean) => {
  if (isOrbit) return getOrbitData()
  return getGroundData()
}

console.time('test')
getData(request.isOrbit)
