// import sqlite3 from 'sqlite3'
// const query = "select distinct DATE, PCDU_BAT_VOLTAGE, PCDU_BAT_CURRENT from DSX0201_tlm_id_1 where DATE between '2022-04-18' and '2022-04-19'"
import * as z from 'zod'
import { BigQuery } from '@google-cloud/bigquery'

const BIGQUERY_PROJECT = 'syns-sol-grdsys-external-prod'
const OBCTIME_INITIAL = '2016-1-1 00:00:00 UTC'

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
  isStored: true,
  isChoosed: false,
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

const includeObcTime = (
  value: BigQueryObjectArrayDataType | BigQueryObjectArrayDataIncludingObcTimeType
): value is BigQueryObjectArrayDataIncludingObcTimeType => {
  if ((value as BigQueryObjectArrayDataIncludingObcTimeType).OBCTimeUTC !== undefined) {
    const result = bigqueryObcTimeArrayTypeSchema.safeParse(value.OBCTimeUTC)
    return result.success
  }
  return false
}

export const toObjectArrayBigQuery = (
  records: BigQueryArrayObjectDataType
): BigQueryObjectArrayDataIncludingObcTimeType | null => {
  const objectArray: BigQueryObjectArrayDataType = {}
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

const addMasterObcTime = (data: BigQueryArrayObjectDataType) => {
  return data.map((d) => {
    const tlmIdList = request.tlm.map((e) => e.tlmId)
    const timeList = tlmIdList.map((tlmId) => [d[`OBCTimeUTC_id${tlmId}`], d[`CalibratedOBCTimeUTC_id${tlmId}`]]).flat()
    console.log(timeList)
    return d
  })
}

const startDateStr = getStringFromUTCDateFixedTime(request.dateSetting.startDate, '00:00:00')
const endDateStr = getStringFromUTCDateFixedTime(request.dateSetting.endDate, '23:59:59')

const queryEachTable = queryTrim(
  request.tlm.reduce((prevQuery, currentElement) => {
    const datasetTableQuery = `\n(tab)(tab)(tab)\`${BIGQUERY_PROJECT}.${request.bigqueryTable}.tlm_id_${currentElement.tlmId}\``
    const tlmListQuery = currentElement.tlmList.reduce(
      (prev, current) => `${prev}\n(tab)(tab)(tab)${current},`,
      `
      (tab)(tab)(tab)OBCTimeUTC,
      (tab)(tab)(tab)CalibratedOBCTimeUTC,
      `
    )
    const whereQuery = `
      (tab)(tab)(tab)CalibratedOBCTimeUTC > \'${OBCTIME_INITIAL}\'
      (tab)(tab)(tab)AND OBCTimeUTC BETWEEN \'${startDateStr}\' AND \'${endDateStr}\'
      ${request.isStored ? '(tab)(tab)(tab)AND Stored = True' : ''}
      `

    return `
      ${prevQuery}
      (tab)id${currentElement.tlmId} AS (
      (tab)(tab)SELECT DISTINCT${tlmListQuery}
      (tab)(tab)FROM${datasetTableQuery}
      (tab)(tab)WHERE${whereQuery}
      (tab)(tab)ORDER BY OBCTimeUTC
      (tab)),
      `
  }, '')
)

const queryAllCol = queryTrim(
  request.tlm.reduce((prevQuery, currentElement) => {
    const tlmListQuery = currentElement.tlmList.reduce((prev, current) => `${prev}\n(tab)${current},`, '')
    const timeColQuery = `
    (tab)id${currentElement.tlmId}.OBCTimeUTC AS OBCTimeUTC_id${currentElement.tlmId},
    (tab)id${currentElement.tlmId}.CalibratedOBCTimeUTC AS CalibratedOBCTimeUTC_id${currentElement.tlmId},
    `
    return `${prevQuery}${timeColQuery}${tlmListQuery}`
  }, 'SELECT DISTINCT')
)

const queryJoin = queryTrim(
  request.tlm.reduce((prevQuery, currentElement, index, array) => {
    if (index === 0) {
      return `FROM id${currentElement.tlmId}`
    }
    return `
      ${prevQuery}\n(tab)FULL JOIN id${currentElement.tlmId}
      (tab)(tab)ON id${array[0]?.tlmId}.OBCTimeUTC = id${currentElement.tlmId}.OBCTimeUTC`
  }, '')
)

const query = `WITH
${queryEachTable}
${queryAllCol}
${queryJoin}
WHERE id${request.tlm[0]?.tlmId}.OBCTimeUTC BETWEEN \'${startDateStr}\' AND \'${endDateStr}\'`
console.log(query)

const bigquery = new BigQuery({
  keyFilename: 'G:/共有ドライブ/0705_Sat_Dev_Tlm/settings/strix-tlm-bq-reader-service-account.json',
})

export type apiSuccess<T> = { success: true; data: T }
export type apiError = { success: false; error: string }
export type apiReturnType<T> = apiSuccess<T> | apiError

const regexBigQueryObcTime =
  /^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9].[0-9]{3}Z/
export const bigqueryObcTimeTypeSchema = z.string().regex(regexBigQueryObcTime)
export const bigqueryObcTimeArrayTypeSchema = z.array(bigqueryObcTimeTypeSchema)
export const bigqueryDateTypeSchema = z.object({ value: bigqueryObcTimeTypeSchema }).transform((e) => e.value)
export const bigqueryDataTypeSchema = z.union([z.number().nullable(), z.string(), bigqueryDateTypeSchema])
export const bigqueryObjectDataTypeSchema = z.record(bigqueryDataTypeSchema)
export const bigqueryArrayObjectDataTypeSchema = z.array(bigqueryObjectDataTypeSchema)
export const bigqueryObjectArrayDataTypeSchema = z.record(z.array(bigqueryDataTypeSchema))
export const bigqueryObjectArrayDataIncludingObcTimeTypeSchema = z
  .object({
    OBCTimeUTC: z.array(bigqueryObcTimeTypeSchema),
    CalibratedOBCTimeUTC: z.array(bigqueryObcTimeTypeSchema),
  })
  .and(bigqueryObjectArrayDataTypeSchema)

export type BigQueryArrayObjectDataType = z.infer<typeof bigqueryArrayObjectDataTypeSchema>
export type BigQueryObjectArrayDataType = z.infer<typeof bigqueryObjectArrayDataTypeSchema>
export type BigQueryObjectArrayDataIncludingObcTimeType = z.infer<
  typeof bigqueryObjectArrayDataIncludingObcTimeTypeSchema
>

console.time('test')
const promiseResponse: Promise<apiReturnType<BigQueryObjectArrayDataIncludingObcTimeType>> = bigquery
  .query(query)
  .then((data) => {
    const schemaResult = bigqueryArrayObjectDataTypeSchema.safeParse(data[0])
    if (schemaResult.success) {
      const dataIncludingMasterObcTime = addMasterObcTime(schemaResult.data)
      console.log(dataIncludingMasterObcTime)
      //   const convertedData = toObjectArrayBigQuery(dataIncludingMasterObcTime)
      //   if (convertedData)
      //     return {
      //       success: true,
      //       data: convertedData,
      //     } as const

      return {
        success: false,
        error: 'Cannot convert from arrayObject to objectArray',
      } as const
    }

    JSON.stringify(schemaResult.error.issues[0])
    return {
      success: false,
      error: JSON.stringify(schemaResult.error.issues[0]),
    } as const
  })
  .catch((err) => {
    return {
      success: false,
      error: JSON.stringify(err.errors[0]),
    }
  })

promiseResponse.then((response) => {
  console.log(response)
})
// Promise.all(
//   querSingleTableList.map((element): Promise<apiReturnType<BigQueryObjectArrayDataIncludingObcTimeType>> => {
//     return bigquery
//       .query(element.query)
//       .then((data) => {
//         const schemaResult = bigqueryArrayObjectDataTypeSchema.safeParse(data[0])
//         if (schemaResult.success) {
//           const convertedData = toObjectArrayBigQuery(schemaResult.data)
//           if (convertedData)
//             return {
//               success: true,
//               tlmId: element.tlmId,
//               data: convertedData,
//             } as const

//           return {
//             success: false,
//             tlmId: element.tlmId,
//             error: 'Cannot convert from arrayObject to objectArray',
//           } as const
//         }

//         JSON.stringify(schemaResult.error.issues[0])
//         return {
//           success: false,
//           tlmId: element.tlmId,
//           error: JSON.stringify(schemaResult.error.issues[0]),
//         } as const
//       })
//       .catch((err) => {
//         return {
//           success: false,
//           tlmId: element.tlmId,
//           error: JSON.stringify(err.errors[0]),
//         }
//       })
//   })
// ).then((responses) => {
//   responses.forEach((response) => {
//     if (response.success) {
//       console.log(response.data)
//     } else {
//       console.log(response.error)
//     }
//   })
//   console.timeEnd('test')
// })
