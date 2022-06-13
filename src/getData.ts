import { getGroundData } from './getGroundData'
import { getOrbitData } from './getOrbitData'
import type { RequestDataType } from './types'

const isOrbit = true
const request = {
  project: 'DSX0201',
  isOrbit: isOrbit,
  bigqueryTable: 'strix_b_telemetry_v_6_17',
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
    return getOrbitData(request)
  } else {
    return getGroundData(request)
  }
}

console.time('test')
getData(request).then((response) => {
  console.log(response)
  console.timeEnd('test')
})
