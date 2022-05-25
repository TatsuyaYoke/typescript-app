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

const startDateStr = getStringFromUTCDateFixedTime(request.dateSetting.startDate, '00:00:00')
const endDateStr = getStringFromUTCDateFixedTime(request.dateSetting.endDate, '23:59:59')

const querySingleTableList = request.tlm.map((currentElement) => {
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
      ${request.isStored ? '(tab)AND Stored = True' : ''}
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

const bigquery = new BigQuery({
  keyFilename: 'G:/共有ドライブ/0705_Sat_Dev_Tlm/settings/strix-tlm-bq-reader-service-account.json',
})

export type apiSuccess<T> = { success: true; tlmId: number; data: T }
export type apiError = { success: false; tlmId: number; error: string }
export type apiReturnType<T> = apiSuccess<T> | apiError

const regexBigQueryObcTime =
  /^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9].[0-9]{3}Z/
export const bigqueryObcTimeTypeSchema = z.string().regex(regexBigQueryObcTime)
export const bigqueryObcTimeArrayTypeSchema = z.array(bigqueryObcTimeTypeSchema)
export const bigqueryDateTypeSchema = z.object({ value: bigqueryObcTimeTypeSchema }).transform((e) => e.value)
export const bigqueryDataTypeSchema = z.union([z.number().nullable(), bigqueryDateTypeSchema])
export const bigqueryObjectDataTypeSchema = z.record(bigqueryDataTypeSchema)
export const bigqueryArrayObjectDataTypeSchema = z.array(bigqueryObjectDataTypeSchema)
export const bigqueryObjectArrayDataTypeSchema = z.record(z.array(bigqueryDataTypeSchema))
export const bigqueryObjectArrayDataIncludingObcTimeTypeSchema = z
  .object({
    OBCTimeUTC: z.array(bigqueryObcTimeTypeSchema),
    CalibratedOBCTimeUTC: z.array(bigqueryObcTimeTypeSchema),
  })
  .and(bigqueryObjectArrayDataTypeSchema)

export type BigQueryObcTimeArrayType = z.infer<typeof bigqueryObcTimeArrayTypeSchema>
export type BigQueryDataType = z.infer<typeof bigqueryDataTypeSchema>
export type BigQueryArrayObjectDataType = z.infer<typeof bigqueryArrayObjectDataTypeSchema>
export type BigQueryObjectArrayDataType = z.infer<typeof bigqueryObjectArrayDataTypeSchema>
export type BigQueryObjectArrayDataIncludingObcTimeType = z.infer<
  typeof bigqueryObjectArrayDataIncludingObcTimeTypeSchema
>
export type responseDataType = {
  [key: string]: {
    time: BigQueryObcTimeArrayType
    data: BigQueryDataType[]
  }
}

console.time('test')
Promise.all(
  querySingleTableList.map((element): Promise<apiReturnType<BigQueryObjectArrayDataIncludingObcTimeType>> => {
    return bigquery
      .query(element.query)
      .then((data) => {
        const schemaResult = bigqueryArrayObjectDataTypeSchema.safeParse(data[0])
        if (schemaResult.success) {
          const convertedData = toObjectArrayBigQuery(schemaResult.data)
          if (convertedData)
            return {
              success: true,
              tlmId: element.tlmId,
              data: convertedData,
            } as const

          return {
            success: false,
            tlmId: element.tlmId,
            error: 'Cannot convert from arrayObject to objectArray',
          } as const
        }

        JSON.stringify(schemaResult.error.issues[0])
        return {
          success: false,
          tlmId: element.tlmId,
          error: JSON.stringify(schemaResult.error.issues[0]),
        } as const
      })
      .catch((err) => {
        return {
          success: false,
          tlmId: element.tlmId,
          error: JSON.stringify(err.errors[0]),
        }
      })
  })
).then((responses) => {
  const responseData: responseDataType = {}
  responses.forEach((response) => {
    const tlmIdIndex = request.tlm.findIndex((e) => e.tlmId === response.tlmId)
    const tlmList = request.tlm[tlmIdIndex]?.tlmList
    if (response.success && tlmList) {
      tlmList.forEach((tlm) => {
        const data = response.data[tlm]
        if (data) responseData[tlm] = { time: response.data.OBCTimeUTC, data: data }
      })
    }
  })

  console.log(responseData)
  console.timeEnd('test')
})
