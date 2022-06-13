import { getGroundData } from './getGroundData'
import { getOrbitData } from './getOrbitData'
import type { RequestDataType } from './types'
import * as dotenv from 'dotenv'
dotenv.config()

const DB_PATH = process.env.DB_PATH ?? ''
const BIGQUERY_SETTING_PATH = process.env.BIGQUERY_SETTING_PATH ?? ''

const isOrbit = true
const request = {
  project: 'DSX0201',
  isOrbit: isOrbit,
  orbitDatasetPath: 'strix_b_telemetry_v_6_17',
  groundTestPath: 'DSX0201/500_SystemFM',
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

const getData = (request: RequestDataType) => {
  if (request.isOrbit) {
    return getOrbitData(request, BIGQUERY_SETTING_PATH)
  } else {
    return getGroundData(request, DB_PATH)
  }
}

console.time('test')
getData(request).then((response) => {
  console.log(response)
  console.timeEnd('test')
})
