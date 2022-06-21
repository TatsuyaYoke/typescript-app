import { getGroundData } from './getGroundData'
import { getOrbitData } from './getOrbitData'
import type { RequestDataType, ResponseDataType } from './types'
import * as dotenv from 'dotenv'
dotenv.config()

const DB_PATH = process.env.DB_PATH ?? ''
const BIGQUERY_SETTING_PATH = process.env.BIGQUERY_SETTING_PATH ?? ''

const isOrbit = false
const request = {
  pjName: 'DSX0201',
  isOrbit: isOrbit,
  orbitDatasetPath: 'strix_b_telemetry_v_6_17',
  groundTestPath: 'DSX0201/500_SystemFM',
  isStored: false,
  isChosen: false,
  dateSetting: {
    // startDate: isOrbit ? new Date(2022, 3, 28) : new Date(2022, 4, 18),
    // endDate: isOrbit ? new Date(2022, 3, 28) : new Date(2022, 4, 19),
    // startDate: isOrbit ? new Date(2022, 3, 28) : new Date(2021, 10, 11),
    // endDate: isOrbit ? new Date(2022, 3, 28) : new Date(2021, 10, 17),
    startDate: isOrbit ? new Date(2022, 3, 28) : new Date(2022, 5, 10),
    endDate: isOrbit ? new Date(2022, 3, 28) : new Date(2022, 5, 10),
  },
  testCase: [
    { value: '510_FlatSat', label: '510_FlatSat' },
    { value: '511_Hankan_Test', label: '511_Hankan_Test' },
  ],
  tlm: [
    { tlmId: 1, tlmList: ['PCDU_BAT_CURRENT', 'PCDU_BAT_VOLTAGE'] },
    // { tlmId: 2, tlmList: ['OBC_AD590_01', 'OBC_AD590_02'] },
  ],
}

const timeoutError = (timeoutMs: number) => {
  return new Promise((resolve) => setTimeout(() => resolve('Timeout'), timeoutMs))
}

const isResponseDataType = (item: unknown): item is ResponseDataType => {
  if (
    (item as ResponseDataType).success !== undefined &&
    ((item as ResponseDataType).tlm !== undefined && (item as ResponseDataType).errorMessages) !== undefined
  )
    return true
  return false
}

const getData = async (request: RequestDataType, timeoutMs: number = 1000): Promise<ResponseDataType> => {
  if (request.isOrbit) {
    const result = await Promise.race([getOrbitData(request, BIGQUERY_SETTING_PATH), timeoutError(timeoutMs)])
    if (isResponseDataType(result)) return result
  } else {
    const result = await Promise.race([getGroundData(request, DB_PATH), timeoutError(timeoutMs)])
    if (isResponseDataType(result)) return result
  }
  return {
    success: false,
    tlm: {
      time: [],
      data: {},
    },
    errorMessages: ['Timeout Error'],
  }
}

console.log(request)
console.time('test')
getData(request).then((response) => {
  console.log(response)
  console.timeEnd('test')
})
