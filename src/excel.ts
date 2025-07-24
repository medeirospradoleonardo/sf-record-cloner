import ExcelJS from 'exceljs'
import fs from 'fs'
import path from 'path'

type RecordInput = Record<string, any>
export type RecordResult = Record<string, any> & {
    Inserido: string
    IdSalesforce?: string
    Erro?: string
}

export async function generateExcelReport(
    objectName: string,
    inputRecords: RecordInput[],
    outputResults: RecordResult[]
) {
    const workbook = new ExcelJS.Workbook()

    // Aba: Entrada
    const inputSheet = workbook.addWorksheet(`${objectName}_Entrada`)
    if (inputRecords.length > 0) {
        inputSheet.columns = Object.keys(inputRecords[0]).map(key => ({ header: key, key }))
        inputSheet.addRows(inputRecords)
    }

    // Aba: Resultados
    const resultSheet = workbook.addWorksheet(`${objectName}_Resultados`)
    if (outputResults.length > 0) {
        resultSheet.columns = Object.keys(outputResults[0]).map(key => ({ header: key, key }))
        resultSheet.addRows(outputResults)
    }

    const outputDir = path.resolve('logs')
    if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir)

    const filePath = path.join(outputDir, `${objectName}_log.xlsx`)
    await workbook.xlsx.writeFile(filePath)

    console.log(`📄 Log salvo em: ${filePath}`)
}
