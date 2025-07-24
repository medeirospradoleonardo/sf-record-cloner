import 'dotenv/config'
import inquirer from 'inquirer'
import jsforce from 'jsforce'
import ora from 'ora'

async function loginToOrg(username, password, label, url = 'https://test.salesforce.com') {
  const conn = new jsforce.Connection({
    loginUrl : url
  })
  const spinner = ora(`Conectando na org ${label}...`).start()
  try {
    await conn.login(username, password)
    spinner.succeed(`Conectado na org ${label}`)
    return conn
  } catch (error) {
    spinner.fail(`Erro ao conectar na org ${label}: ${error.message}`)
    process.exit(1)
  }
}

function chunkArray(array, size) {
  return Array.from({ length: Math.ceil(array.length / size) }, (_, i) =>
    array.slice(i * size, i * size + size)
  )
}

async function main() {
  const connSource = await loginToOrg(process.env.SF_SOURCE_USERNAME, process.env.SF_SOURCE_PASSWORD, 'origem')
  const connDest = await loginToOrg(process.env.SF_DEST_USERNAME, process.env.SF_DEST_PASSWORD, 'destino')

  const choices = (await connSource.describeGlobal()).sobjects.map((obj) => obj.name)

  const { objects } = await inquirer.prompt([
    {
      type: 'checkbox',
      name: 'objects',
      message: 'Quais objetos você quer clonar?',
      choices: ['Account', 'Contact', 'Opportunity', 'Lead', 'Territory2'], // pode substituir com `conn.describeGlobal()` depois
    }
  ])

  for (const object of objects) {
  const spinner = ora(`Clonando registros de ${object}...`).start()
  try {
    const metadata = await connSource.sobject(object).describe()
    const writableFields = metadata.fields
      .filter(f => f.createable && f.name !== 'Id')
      .map(f => f.name)

    const records = await connSource.sobject(object).find({}, writableFields)

    // const soql = `SELECT ${writableFields.join(',')} FROM ${object}`
    // let result = await connSource.query(soql)
    // let records = result.records

    // while (!result.done) {
    //   result = await connSource.queryMore(result.nextRecordsUrl)
    //   records = records.concat(result.records)
    // }

    spinner.succeed(`Encontrados ${records.length} registros de ${object}`)

    const batches = chunkArray(records, 200)
    let totalSuccess = 0

    for (const [index, batch] of batches.entries()) {
      const result = await connDest.sobject(object).create(batch)
      const successCount = result.filter(r => r.success).length
      totalSuccess += successCount

      ora().info(`Lote ${index + 1}/${batches.length} - ${successCount} registros inseridos com sucesso`)
    }

    ora().succeed(`✅ Total inserido na org destino: ${totalSuccess}`)

    // const result = await connDest.sobject(object).create(records)

    // const successCount = result.filter(r => r.success).length
    // spinner.succeed(Clonados ${successCount} registros de 
  } catch (err) {
    spinner.fail(`Erro ao clonar ${object}: ${err.message}`)
  }
}

}

main()
