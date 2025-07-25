import jsforce, { Connection } from "jsforce"
import ora from "ora"

export async function loginToOrg(
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