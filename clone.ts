import 'dotenv/config'
import inquirer from 'inquirer'
import jsforce, { Connection, DescribeSObjectResult } from 'jsforce'
import ora from 'ora'

type SObjectRecord = Record<string, any>

async function loginToOrg(
  username: string,
  password: string,
  label: string,
  url: string = 'https://test.salesforce.com'
): Promise<Connection> {
  const conn = new jsforce.Connection({ loginUrl: url })
  const spinner = ora(`Conectando na org ${label}...`).start()
  try {
    await conn.login(username, password)
    spinner.succeed(`Conectado na org ${label}`)
    return conn
  } catch (error: any) {
    spinner.fail(`Erro ao conectar na org ${label}: ${error.message}`)
    process.exit(1)
  }
}

function chunkArray<T>(array: T[], size: number): T[][] {
  return Array.from({ length: Math.ceil(array.length / size) }, (_, i) =>
    array.slice(i * size, i * size + size)
  )
}

const getAllRecords = async (connSource: Connection, fields: string[], objectName: string) => {
  const soql = `SELECT ${fields.join(',')} FROM ${objectName}`
  let result = await connSource.query<SObjectRecord>(soql)
  let records = result.records

  while (!result.done) {
    result = await connSource.queryMore<SObjectRecord>(result.nextRecordsUrl!)
    records = records.concat(result.records)
  }

  return records
}

async function main() {
  const connSource = await loginToOrg(
    process.env.SF_SOURCE_USERNAME!,
    process.env.SF_SOURCE_PASSWORD!,
    'origem'
  )

  const connDest = await loginToOrg(
    process.env.SF_DEST_USERNAME!,
    process.env.SF_DEST_PASSWORD!,
    'destino'
  )

  const allObjects = (await connSource.describeGlobal()).sobjects.map(obj => obj.name)

  const { objects } = await inquirer.prompt<{ objects: string[] }>([
    {
      type: 'checkbox',
      name: 'objects',
      message: 'Quais objetos você quer clonar?',
      choices: ['Account', 'Contact', 'Opportunity', 'Lead', 'Territory2'],
    },
  ])

  for (const object of objects) {
    const spinner = ora(`Clonando registros de ${object}...`).start()
    try {
      const metadata: DescribeSObjectResult = await connSource.sobject(object).describe()

      const writableFields = metadata.fields
        .filter(f => f.createable && f.name !== 'Id')
        .map(f => f.name)

      const records = await getAllRecords(connSource, writableFields, object)

      spinner.succeed(`Encontrados ${records.length} registros de ${object}`)

      const batches = chunkArray(records, 200)
      let totalSuccess = 0

      for (const [index, batch] of batches.entries()) {
        const result = await connDest.sobject(object).create(batch)
        const successCount = result.filter(r => r.success).length
        totalSuccess += successCount

        ora().info(
          `Lote ${index + 1}/${batches.length} - ${successCount} registros inseridos com sucesso`
        )
      }

      ora().succeed(`✅ Total inserido na org destino: ${totalSuccess}`)
    } catch (err: any) {
      spinner.fail(`Erro ao clonar ${object}: ${err.message}`)
    }
  }
}

main()
