import { BigQuery } from '@google-cloud/bigquery'
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
  ResponseDataType,
} from './types'
import { getStringFromUTCDateFixedTime, trimQuery, uniqueArray } from './function'

const OBCTIME_INITIAL = '2016-1-1 00:00:00 UTC'

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

export const getOrbitData = async (request: RequestDataType, bigquerySettingPath: string) => {
  const startDateStr = getStringFromUTCDateFixedTime(request.dateSetting.startDate, '00:00:00')
  const endDateStr = getStringFromUTCDateFixedTime(request.dateSetting.endDate, '23:59:59')
  const queryWith = trimQuery(
    request.tlm.reduce((prevQuery, currentElement) => {
      const datasetTableQuery = `\n(tab)(tab)\`${request.orbitDatasetPath}.tlm_id_${currentElement.tlmId}\``
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
  const responseFromDb = await readOrbitDbSync(bigquerySettingPath, query)
  const errorMessages: string[] = []

  const responseData: ResponseDataType<'orbit'> = { success: true, tlm: { time: [], data: {} }, errorMessages: [] }
  if (responseFromDb.success) {
    responseData.tlm.time = responseFromDb.data.OBCTimeUTC
    const tlmAllList = request.tlm.map((e) => e.tlmList).flat()
    tlmAllList.forEach((tlmName) => {
      const data = responseFromDb.data[tlmName]
      responseData.tlm.data[tlmName] = []
      if (data) responseData.tlm.data[tlmName]?.push(...data)
    })
  } else if (!responseFromDb.success) {
    const error = responseFromDb.error
    errorMessages.push(error)
  }
  if (responseData.tlm.time.length === 0) responseData.success = false
  responseData.errorMessages = uniqueArray(errorMessages)
  return responseData
}
