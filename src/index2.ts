// import sqlite3 from 'sqlite3'
// const db = new sqlite3.Database('./db/test.db')

// // db.serialize(() => {
// //   db.run('drop table if exists members')
// //   db.run('create table if not exists members(name,age)')
// //   db.run('insert into members(name,age) values(?,?)', 'hoge', 33)
// //   db.run('insert into members(name,age) values(?,?)', 'foo', 44)
// //   db.run('update members set age = ? where name = ?', 55, 'foo')
// //   db.each('select * from members', (_err, row) => {
// //     console.log(`${row.name} ${row.age}`)
// //   })
// //   db.all('select * from members', (_err, rows) => {
// //     console.log(JSON.stringify(rows))
// //   })
// //   db.get('select count(*) from members', (_err, count) => {
// //     console.log(count['count(*)'])
// //   })
// // })

// db.serialize(() => {
//   db.all('SELECT * FROM members', (_err, records) => {
//     console.log(records)
//   })
// })

// db.close()

import { join, resolve } from 'path'
console.log(join(__dirname, '..', 'node_modules', '.bin', 'electron'))
console.log(resolve(__dirname, '..', 'node_modules', '.bin', 'electron'))