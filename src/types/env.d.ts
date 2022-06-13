/**
 * extend type for process.env
 * @see https://qiita.com/akameco/items/6567ccb1fd3b2e787f56
 */
 declare namespace NodeJS {
    interface ProcessEnv {
      readonly DB_PATH?: string
      readonly BIGQUERY_SETTING_PATH?: string
    }
  }
  