// utils.ts

import { Connection, DescribeSObjectResult } from 'jsforce'
import ora from 'ora'

export type SObjectRecord = Record<string, any>

export function chunkArray<T>(array: T[], size: number): T[][] {
    return Array.from({ length: Math.ceil(array.length / size) }, (_, i) =>
        array.slice(i * size, i * size + size)
    )
}

export async function getAllRecords(
    conn: Connection,
    fields: string[],
    objectName: string
): Promise<SObjectRecord[]> {
    const soql = `SELECT ${fields.join(',')} FROM ${objectName}`
    let result = await conn.query<SObjectRecord>(soql)
    let records = result.records

    while (!result.done) {
        result = await conn.queryMore<SObjectRecord>(result.nextRecordsUrl!)
        records = records.concat(result.records)
    }

    return records
}

export async function getWritableFields(
    conn: Connection,
    objectName: string
): Promise<string[]> {
    const metadata: DescribeSObjectResult = await conn.sobject(objectName).describe()
    return metadata.fields
        .filter((f) => f.createable && f.name !== 'Id')
        .map((f) => f.name)
}

export function groupByParentTerritory(records: SObjectRecord[]) {
    const rootTerritories = records.filter(r => !r.ParentTerritoryId)
    const children = records.filter(r => r.ParentTerritoryId)
    return { rootTerritories, children }
}

export async function insertInChunks(
    conn: Connection,
    objectName: string,
    records: SObjectRecord[],
    externalIdMap: Record<string, string> = {}
) {
    const chunks = chunkArray(records, 200)
    const insertedIds: string[] = []
    const results: { success: boolean; id?: string; errors?: any[] }[] = []

    for (const [index, chunk] of chunks.entries()) {
        const resolvedChunk = chunk.map(r => {
            const recordCopy = { ...r }
            for (const field in recordCopy) {
                if (externalIdMap[recordCopy[field]]) {
                    recordCopy[field] = externalIdMap[recordCopy[field]]
                }
            }
            return recordCopy
        })

        const res = await conn.sobject(objectName).create(resolvedChunk)
        insertedIds.push(...res.filter(r => r.success).map(r => r.id!))
        results.push(...res)

        ora().info(`Lote ${index + 1}/${chunks.length} - ${res.filter(r => r.success).length} inseridos`)
    }

    return { insertedIds, results }
}

type ResultLog = {
    Inserido: string
    IdSalesforce?: string
    Erro?: string
}

export async function insertWithHierarchyHandling(
    conn: Connection,
    objectName: string,
    records: any[]
): Promise<ResultLog[]> {
    const spinner = ora(`Inserindo registros hierárquicos de ${objectName}...`).start()
    const logs: ResultLog[] = []

    const roots = records.filter(r => !r.ParentTerritory2Id)
    const children = records.filter(r => r.ParentTerritory2Id)

    // Inserir os registros raiz
    const rootResults = await conn.sobject(objectName).create(roots)
    const idMap = new Map<string, string>()

    rootResults.forEach((res, i) => {
        const oldId = roots[i].Id
        if (res.success && oldId) idMap.set(oldId, res.id!)
        logs.push({
            Inserido: res.success ? '✅' : '❌',
            IdSalesforce: res.id,
            Erro: res.success ? undefined : res.errors?.[0]?.message,
        })
    })

    spinner.succeed(`${roots.length} registros raiz inseridos`)

    // Atualizar filhos com novos ParentTerritory2Id
    const updatedChildren = children.map(record => {
        const newParentId = idMap.get(record.ParentTerritory2Id)
        return {
            ...record,
            ParentTerritory2Id: newParentId ?? null,
        }
    })

    // Inserir filhos em batches
    const childBatches = chunkArray(updatedChildren, 200)
    for (const [i, batch] of childBatches.entries()) {
        const res = await conn.sobject(objectName).create(batch)
        spinner.info(`Lote ${i + 1}/${childBatches.length}: ${res.filter(r => r.success).length} filhos inseridos`)
        res.forEach((r, j) => {
            logs.push({
                Inserido: r.success ? '✅' : '❌',
                IdSalesforce: r.id,
                Erro: r.success ? undefined : r.errors?.[0]?.message,
            })
        })
    }

    spinner.succeed(`✅ Hierarquia de ${objectName} clonada com sucesso`)
    return logs
}

