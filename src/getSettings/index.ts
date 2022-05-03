import * as fs from 'fs'
import { join } from 'path'
import * as z from 'zod'

export const pjSettingSchema = z.object({
  pjName: z.string().regex(/^DSX[0-9]{4}/),
  groundTestPath: z.string(),
  orbitDatasetPath: z.string().regex(/^syns-sol-grdsys-external-prod.strix_/),
})
export const pjSettingsSchema = z.array(pjSettingSchema)
export const appSettingsSchema = z.object({
  project: pjSettingsSchema,
})

export type pjSettingType = z.infer<typeof pjSettingSchema>
export type pjSettingsType = z.infer<typeof pjSettingsSchema>
export type pjSettingsKeyType = keyof pjSettingType
export type appSettingsType = z.infer<typeof appSettingsSchema>

export const tlmIdSchema = z.record(z.number())
export type tlmIdType = z.infer<typeof tlmIdSchema>
export type pjSettingWithTlmIdType = pjSettingType & {
  tlmId?: tlmIdType
}

const resolvePath = (path: string, resolveName1: string, resolveName2: string): string | null => {
  if (fs.existsSync(path)) return path
  let resolvedPath = ''
  if (path.indexOf(resolveName1) !== -1) {
    resolvedPath = path.replace(resolveName1, resolveName2)
    if (fs.existsSync(resolvedPath)) return resolvedPath
  }
  if (path.indexOf(resolveName2) !== -1) {
    resolvedPath = path.replace(resolveName2, resolveName1)
    if (fs.existsSync(resolvedPath)) return resolvedPath
  }
  return null
}

const TOP_PATH = 'G:/Shared drives/0705_Sat_Dev_Tlm'
const PROJECT_SETTING_PATH = 'settings/pj-settings.json'
const PROJECT = 'DSX0201'
const resolvePathGdrive = (path: string): string | null => resolvePath(path, '共有ドライブ', 'Shared drives')
const isNotNull = <T>(item: T): item is Exclude<T, null> => item !== null
const isNotUndefined = <T>(item: T): item is Exclude<T, undefined> => item !== undefined

const filePath = resolvePathGdrive(join(TOP_PATH, PROJECT_SETTING_PATH))
let pjSettings: pjSettingsType | undefined
if (filePath) {
  const settings = JSON.parse(fs.readFileSync(filePath, 'utf8'))
  const schemaResult = appSettingsSchema.safeParse(settings)
  if (schemaResult.success) {
    pjSettings = schemaResult.data.project
  }
}

if (pjSettings) {
  const pjSettingWithTlmIdList = pjSettings.map((value) => {
    const tlmIdfilePath = resolvePathGdrive(join(TOP_PATH, 'settings', value.pjName, 'tlm_id.json'))
    const response: pjSettingWithTlmIdType = value
    if (tlmIdfilePath) {
      const tlmIdsettingsBeforeParse = JSON.parse(fs.readFileSync(tlmIdfilePath, 'utf-8'))
      const tlmIdSchemaResult = tlmIdSchema.safeParse(tlmIdsettingsBeforeParse)
      if (tlmIdSchemaResult.success) {
        response.tlmId = tlmIdSchemaResult.data
      }
    }
    return response
  })
  const settings = pjSettingWithTlmIdList.filter((value) => {
    return value.pjName === PROJECT
  })[0]
  console.log(settings)
}
