import { BigQuery } from '@google-cloud/bigquery'
import * as dotenv from 'dotenv'
import {
  isNotNumber,
  nonNullable,
  RequestDataType,
  QueryReturnType,
  Mode,
  DateTimeType,
  DataType,
  ArrayObjectType,
  ObjectArrayIncludingDateTimeType,
  orbitArrayObjectTypeSchema,
} from './types'
import { getStringFromUTCDateFixedTime, trimQuery } from './function'
dotenv.config()

const BIGQUERY_PROJECT = process.env.BIGQUERY_PROJECT
const OBCTIME_INITIAL = process.env.OBCTIME_INITIAL
const SETTING_PATH = process.env.SETTING_PATH ?? ''

export type responseDataType<T extends Mode> = {
  tlm: {
    [key: string]: {
      time: DateTimeType[T][]
      data: DataType[T][]
    }
  }
  errorMessages: string[]
}

export const toObjectArrayOrbit = (records: ArrayObjectType['orbit']): ObjectArrayIncludingDateTimeType['orbit'] => {
  const objectArray: ObjectArrayIncludingDateTimeType['orbit'] = { OBCTimeUTC: [], CalibratedOBCTimeUTC: [] }
  const keys = Object.keys(records[0] ?? {})
  const keysOBCTimeUTC = keys.filter((e) => e.indexOf('_OBCTimeUTC') !== -1)
  const keysCalibratedOBCTimeUTC = keys.filter((e) => e.indexOf('_CalibratedOBCTimeUTC') !== -1)

  keys.forEach((key) => {
    objectArray[key] = []
  })

  records.forEach((record) => {
    const OBCTimeUTC = keysOBCTimeUTC
      .map((e) => record[e])
      .filter(isNotNumber)
      .filter(nonNullable)[0]

    const CalibratedOBCTimeUTC = keysCalibratedOBCTimeUTC
      .map((e) => record[e])
      .filter(isNotNumber)
      .filter(nonNullable)[0]

    if (OBCTimeUTC && CalibratedOBCTimeUTC) {
      objectArray.OBCTimeUTC.push(OBCTimeUTC)
      objectArray.CalibratedOBCTimeUTC.push(CalibratedOBCTimeUTC)
      keys.forEach((key) => {
        if (key.indexOf('OBCTimeUTC') === -1) objectArray[key]?.push(record[key] ?? null)
      })
    }
  })
  return objectArray
}

export const readOrbitDbSync = (
  path: string,
  query: string
): Promise<QueryReturnType<ObjectArrayIncludingDateTimeType['orbit']>> => {
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
            data: convertedData,
          } as const

        return {
          success: false,
          error: `Cannot convert from arrayObject to objectArray`,
        } as const
      }

      return {
        success: false,
        error: `${JSON.stringify(schemaResult.error.issues[0])}`,
      } as const
    })
    .catch((err) => {
      return {
        success: false,
        error: `${JSON.stringify(err.errors[0])}`,
      } as const
    })
}

const getOrbitData = async (request: RequestDataType) => {
  const startDateStr = getStringFromUTCDateFixedTime(request.dateSetting.startDate, '00:00:00')
  const endDateStr = getStringFromUTCDateFixedTime(request.dateSetting.endDate, '23:59:59')
  const queryWith = trimQuery(
    request.tlm.reduce((prevQuery, currentElement) => {
      const datasetTableQuery = `\n(tab)(tab)\`${BIGQUERY_PROJECT}.${request.bigqueryTable}.tlm_id_${currentElement.tlmId}\``
      const tlmListQuery = currentElement.tlmList.reduce(
        (prev, current) => `${prev}\n(tab)(tab)${current},`,
        `
        (tab)(tab)OBCTimeUTC,
        (tab)(tab)CalibratedOBCTimeUTC,
        `
      )
      const whereQuery = `
      (tab)(tab)CalibratedOBCTimeUTC > \'${OBCTIME_INITIAL}\'
      (tab)(tab)AND OBCTimeUTC BETWEEN \'${startDateStr}\' AND \'${endDateStr}\'
      ${request.isStored ? '(tab)(tab)AND Stored = True' : '(tab)(tab)AND Stored = False'}
      `

      return `${prevQuery}
    (tab)id${currentElement.tlmId} as (
    (tab)SELECT DISTINCT${tlmListQuery}
    (tab)FROM${datasetTableQuery}
    (tab)WHERE${whereQuery}
    (tab)ORDER BY OBCTimeUTC),
  `
    }, 'WITH\n')
  )

  const querySelect = trimQuery(
    request.tlm.reduce((prevQuery, currentElement) => {
      const tlmListQuery = currentElement.tlmList.reduce((prev, current) => `${prev}\n(tab)${current},`, '')
      return `${prevQuery}
    (tab)id${currentElement.tlmId}.OBCTimeUTC AS id${currentElement.tlmId}_OBCTimeUTC,
    (tab)id${currentElement.tlmId}.CalibratedOBCTimeUTC AS id${currentElement.tlmId}_CalibratedOBCTimeUTC,
    ${tlmListQuery}
   `
    }, 'SELECT\n')
  )

  const baseId = request.tlm[0]?.tlmId
  const queryJoin = trimQuery(
    request.tlm.slice(1, request.tlm.length).reduce((prevQuery, currentElement) => {
      return `${prevQuery}
    FULL JOIN id${currentElement.tlmId}
    (tab)ON id${baseId}.OBCTimeUTC = id${currentElement.tlmId}.OBCTimeUTC
    `
    }, `FROM id${baseId}`)
  )

  const query = `${queryWith}\n${querySelect}\n${queryJoin}`
  const responseFromDb = await readOrbitDbSync(SETTING_PATH, query)

  const responseData: responseDataType<'orbit'> = { tlm: {}, errorMessages: [] }
  if (responseFromDb.success) {
    console.log(responseFromDb.data)
    const tlmNameList = request.tlm.map((e) => e.tlmList).flat()
    tlmNameList.forEach((tlmName) => {
      const data = responseFromDb.data[tlmName]
      if (data) responseData.tlm[tlmName] = { time: responseFromDb.data.OBCTimeUTC, data: data }
    })
  } else if (!responseFromDb.success) {
    const error = responseFromDb.error
    responseData.errorMessages.push(error)
  }
  return responseData
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
  testCase: [
    { value: '510_FlatSat', label: '510_FlatSat' },
    { value: '511_Hankan_Test', label: '511_Hankan_Test' },
  ],
  tlm: [
    { tlmId: 1, tlmList: ['PCDU_BAT_CURRENT', 'PCDU_BAT_VOLTAGE'] },
    { tlmId: 2, tlmList: ['OBC_AD590_01', 'OBC_AD590_02'] },
  ],
}

console.time('test')
getOrbitData(request).then((response) => {
  console.log(response)
  console.timeEnd('test')
})
