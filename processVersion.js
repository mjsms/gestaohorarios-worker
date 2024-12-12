const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const copyFrom = require('pg-copy-streams').from;

const pool = new Pool({
    user: process.env.DB_USER || 'gestaohorarios', 
    host: process.env.DB_HOST || 'localhost',
    database: process.env.DB_NAME || 'gestaohorarios',
    password: process.env.DB_PASSWORD || 'menezes91', 
    port: process.env.DB_PORT || 5432,
});

const processVersion = async (version) => {
    const filePath = path.join(__dirname, `temp/schedule_${version.id}.csv`); // Caminho temporário do arquivo

    let client;
    try {
        if (!version.binaryFile) {
            throw new Error(`Nenhum arquivo encontrado para a versão ${version.id}`);
        }

        // Salvar o binário como arquivo temporário
        fs.writeFileSync(filePath, version.binaryFile);
        const stats = fs.statSync(filePath);
        if (stats.size === 0) {
            throw new Error(`Arquivo CSV está vazio: ${filePath}`);
        }
        
        client = await pool.connect();

        try {
            // Iniciar a transação
            await client.query('BEGIN');

            const startTime = Date.now();

            await client.query('DROP TABLE IF EXISTS TempSchedule;');
            await client.query(`
                CREATE TABLE TempSchedule (
                    Curso VARCHAR(255),
                    UnidadeExecucao VARCHAR(255),
                    Turno VARCHAR(255),
                    Turma VARCHAR(255),
                    InscritosNoTurno INT,
                    DiaDaSemana VARCHAR(50),
                    Inicio TIME,
                    Fim TIME,
                    Dia TEXT,
                    CaracteristicasSalaPedida TEXT,
                    SalaAula VARCHAR(255),
                    Lotacao INT,
                    CaracteristicasReais TEXT
                );
            `);
            console.log(`Temporary table created in ${Date.now() - startTime} ms.`);

            const copyStartTime = Date.now();

            // Use pg-copy-streams to handle the COPY
            const fileStream = fs.createReadStream(filePath);
            const copyQuery = `COPY TempSchedule FROM STDIN WITH CSV HEADER DELIMITER ';';`;

            await new Promise((resolve, reject) => {
                const stream = client.query(copyFrom(copyQuery));
                fileStream.pipe(stream);

                stream.on('finish', resolve);
                stream.on('error', reject);
                fileStream.on('error', reject);
            });

            console.log(`COPY process completed in ${Date.now() - copyStartTime} ms.`);

            // Inserir dados em AcademicProgram
            const academicProgramStartTime = Date.now();
            await client.query(`
                INSERT INTO "AcademicProgram" (name)
                SELECT DISTINCT Curso
                FROM TempSchedule ts
                WHERE NOT EXISTS (
                    SELECT 1 FROM "AcademicProgram" ap WHERE ap.name = ts.Curso
                );
            `);
            console.log(`Inserted AcademicProgram in ${Date.now() - academicProgramStartTime} ms.`);

            // Inserir dados em Subject
            const subjectStartTime = Date.now();
            await client.query(`
                INSERT INTO "Subject" (name, "academicProgramId")
                SELECT DISTINCT ts.UnidadeExecucao, ap.id
                FROM TempSchedule ts
                JOIN "AcademicProgram" ap ON ap.name = ts.Curso
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM "Subject" s
                    WHERE s.name = ts.UnidadeExecucao AND s."academicProgramId" = ap.id
                );
            `);
            console.log(`Inserted Subject in ${Date.now() - subjectStartTime} ms.`);

            // Inserir dados em ClassGroup
            const classGroupStartTime = Date.now();
            await client.query(`
                INSERT INTO "ClassGroup" (name)
                SELECT DISTINCT Turma
                FROM TempSchedule ts
                WHERE 
                    ts.Turma IS NOT NULL AND
                    NOT EXISTS (
                        SELECT 1 FROM "ClassGroup" cg WHERE cg.name = ts.Turma
                    );
            `);
            console.log(`Inserted ClassGroup in ${Date.now() - classGroupStartTime} ms.`);

            // Inserir dados em Shift
            const shiftStartTime = Date.now();
            await client.query(`
                INSERT INTO "Shift" (name, "subjectId", "classGroupId", enrollment)
                SELECT DISTINCT ts.Turno, s.id, cg.id, COALESCE(ts.InscritosNoTurno, 0)
                FROM TempSchedule ts
                JOIN "Subject" s ON s.name = ts.UnidadeExecucao
                JOIN "ClassGroup" cg ON cg.name = ts.Turma
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM "Shift" sh
                    WHERE sh.name = ts.Turno AND sh."subjectId" = s.id AND sh."classGroupId" = cg.id
                );
            `);
            console.log(`Inserted Shift in ${Date.now() - shiftStartTime} ms.`);

            // Inserir dados em Schedule
            const scheduleStartTime = Date.now();
            await client.query(`
                INSERT INTO "Schedule" ("versionId", "shiftId", "classRoomId", "weekdayId", "startTime", "endTime", date)
                SELECT 
                    $1,
                    sh.id,
                    cr.id,
                    wd.id,
                    ts.Inicio,
                    ts.Fim,
                    TO_DATE(ts.Dia, 'DD/MM/YYYY')
                FROM TempSchedule ts
                JOIN "Shift" sh ON sh.name = ts.Turno
                LEFT JOIN "ClassRoom" cr ON cr.name = ts.SalaAula
                LEFT JOIN "Weekday" wd ON wd.abbreviation = ts.DiaDaSemana;
            `, [version.id]);
            console.log(`Inserted Schedule in ${Date.now() - scheduleStartTime} ms.`);

            // Inserir características na tabela Feature
            const featureStartTime = Date.now();
            await client.query(`
                WITH extracted_features AS (
                    SELECT DISTINCT TRIM(feature_name) AS feature_name
                    FROM (
                        SELECT UNNEST(STRING_TO_ARRAY(ts.CaracteristicasSalaPedida, ',')) AS feature_name
                        FROM TempSchedule ts
                        WHERE ts.CaracteristicasSalaPedida IS NOT NULL
                        UNION
                        SELECT UNNEST(STRING_TO_ARRAY(ts.CaracteristicasReais, ',')) AS feature_name
                        FROM TempSchedule ts
                        WHERE ts.CaracteristicasReais IS NOT NULL
                    ) AS all_features
                )
                INSERT INTO "Feature" (name)
                SELECT ef.feature_name
                FROM extracted_features ef
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM "Feature" f
                    WHERE f.name = ef.feature_name
                );
            `);
            console.log(`Inserted Features in ${Date.now() - featureStartTime} ms.`);

            // Inserir características pedidas
            const requestedFeaturesStartTime = Date.now();
            await client.query(`
                WITH extracted_features AS (
                    SELECT
                        ts.Turno,
                        ts.Inicio,
                        ts.Fim,
                        TO_DATE(ts.Dia, 'DD/MM/YYYY') AS date,
                        TRIM(feature_name) AS feature_name
                    FROM TempSchedule ts,
                    UNNEST(STRING_TO_ARRAY(ts.CaracteristicasSalaPedida, ',')) AS feature_name
                    WHERE ts.CaracteristicasSalaPedida IS NOT NULL
                )
                INSERT INTO "ScheduleFeature" ("scheduleId", "featureId", "featureType")
                SELECT DISTINCT
                    s.id AS schedule_id,
                    f.id AS feature_id,
                    'requested'::"enum_ScheduleFeature_featureType" AS feature_type
                FROM extracted_features ef
                JOIN "Schedule" s ON s."shiftId" = (
                        SELECT sh.id
                        FROM "Shift" sh
                        WHERE sh.name = ef.Turno
                    )
                AND s."startTime" = ef.Inicio
                AND s."endTime" = ef.Fim
                AND s.date = ef.date
                JOIN "Feature" f ON f.name = ef.feature_name
                WHERE
                s."versionId" = $1  and
                NOT EXISTS (
                    SELECT 1
                    FROM "ScheduleFeature" sf
                    WHERE sf."scheduleId" = s.id
                      AND sf."featureId" = f.id
                      AND sf."featureType" = 'requested'
                );
            `, [version.id]);
            console.log(`Inserted requested ScheduleFeatures in ${Date.now() - requestedFeaturesStartTime} ms.`);

            // Inserir características reais
            const realFeaturesStartTime = Date.now();
            await client.query(`
                WITH extracted_features AS (
                    SELECT
                        ts.Turno,
                        ts.Inicio,
                        ts.Fim,
                        TO_DATE(ts.Dia, 'DD/MM/YYYY') AS date,
                        TRIM(feature_name) AS feature_name
                    FROM TempSchedule ts,
                    UNNEST(STRING_TO_ARRAY(ts.CaracteristicasReais, ',')) AS feature_name
                    WHERE ts.CaracteristicasReais IS NOT NULL
                )
                INSERT INTO "ScheduleFeature" ("scheduleId", "featureId", "featureType")
                SELECT DISTINCT
                    s.id AS schedule_id,
                    f.id AS feature_id,
                    'real'::"enum_ScheduleFeature_featureType" AS feature_type
                FROM extracted_features ef
                JOIN "Schedule" s ON s."shiftId" = (
                        SELECT sh.id
                        FROM "Shift" sh
                        WHERE sh.name = ef.Turno
                    )
                AND s."startTime" = ef.Inicio
                AND s."endTime" = ef.Fim
                AND s.date = ef.date
                JOIN "Feature" f ON f.name = ef.feature_name
                WHERE 
                s."versionId" = $1  and
                NOT EXISTS (
                    SELECT 1
                    FROM "ScheduleFeature" sf
                    WHERE sf."scheduleId" = s.id
                      AND sf."featureId" = f.id
                      AND sf."featureType" = 'real'
                );
            `, [version.id]);
            console.log(`Inserted real ScheduleFeatures in ${Date.now() - realFeaturesStartTime} ms.`);

            // Inserir Quality Issues para aulas em sobrelotação
            const overcrowdingStartTime = Date.now();
            await client.query(`
                INSERT INTO "QualityIssue" ("scheduleId", "issueType", "description")
                SELECT DISTINCT
                    s.id AS schedule_id,
                    'sobrelotação' AS issueType,
                    CONCAT(
                        'Turno com ', sh.enrollment, ' alunos excede a capacidade da sala (', cr.capacity, ')'
                    ) AS description
                FROM "Schedule" s
                JOIN "Shift" sh ON s."shiftId" = sh.id
                JOIN "ClassRoom" cr ON s."classRoomId" = cr.id
                WHERE 
                    s."versionId" = $1 AND 
                    sh.enrollment > cr.capacity;
            `, [version.id]);
            console.log(`Inserted Quality Issues for overcrowding in ${Date.now() - overcrowdingStartTime} ms.`);

            // Inserir Quality Issues para salas desadequadas
            const inadequateRoomStartTime = Date.now();
            await client.query(`
                INSERT INTO "QualityIssue" ("scheduleId", "issueType", "description")
                SELECT DISTINCT
                    s.id AS schedule_id,
                    'desadequado' AS issueType,
                    'Aula em sala desadequada'
                FROM "Schedule" s
                JOIN "ScheduleFeature" sf_req ON sf_req."scheduleId" = s.id AND sf_req."featureType" = 'requested'
                LEFT JOIN "ScheduleFeature" sf_real 
                    ON sf_real."scheduleId" = s.id 
                    AND sf_real."featureType" = 'real'
                    AND sf_real."featureId" = sf_req."featureId"
                LEFT JOIN "Feature" f_req ON f_req.id = sf_req."featureId"
                WHERE
                    s."versionId" = $1 AND 
                    sf_real."featureId" IS NULL
                GROUP BY s.id;
            `, [version.id]);
            console.log(`Inserted Quality Issues for inadequate rooms in ${Date.now() - inadequateRoomStartTime} ms.`);

            // Inserir Quality Issues para horários indesejados
            const unwantedScheduleStartTime = Date.now();
            await client.query(`
                INSERT INTO "QualityIssue" ("scheduleId", "issueType", "description")
                SELECT DISTINCT
                    s.id AS schedule_id,
                    'horário indesejado' AS issueType,
                    'Aula às 8h00 da manhã no sábado' AS description
                FROM "Schedule" s
                JOIN "Weekday" wd ON s."weekdayId" = wd.id
                WHERE
                    s."versionId" = $1 AND 
                    wd.name = 'Sábado' AND s."startTime" = '08:00:00';
            `, [version.id]);
            console.log(`Inserted Quality Issues for unwanted schedules in ${Date.now() - unwantedScheduleStartTime} ms.`);

            // Atualizar a versão para "processed"
            await version.update({ status: 'processed' });
            // Se tudo deu certo, comitar a transação
            await client.query('COMMIT');

            // Remover a tabela temporária
            await client.query('DROP TABLE IF EXISTS TempSchedule;');
            fs.unlinkSync(filePath);


            console.log(`Dados processados e inseridos na bd para versão ${version.id}.`);
            console.log(`CSV para versão ${version.id} processado com sucesso.`);

        } catch (error) {
            // Em caso de erro, rollback na transação
            await client.query('ROLLBACK');
            console.error(`Erro ao processar a versão ${version.id}:`, error);

            // Atualizar a versão para "erro" após o rollback
            await version.update({ status: 'error' });

            if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
            throw error;
        } finally {
            client.release();
        }

    } catch (error) {
        console.error(`Erro externo ao processar a versão ${version.id}:`, error);
        // Aqui você pode tentar alguma outra ação adicional se necessário
        throw error;
    }
};

module.exports = processVersion;
