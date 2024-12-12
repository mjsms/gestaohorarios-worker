const { models } = require('./db'); // Importa os modelos do banco de dados
const fs = require('fs');
const { parse } = require('fast-csv');

const cache = new Map();

async function findOrCreateWithCache(model, cacheKey, options) {
    const cacheValue = cache.get(cacheKey);

    if (cacheValue && cacheValue[options.where.name]) {
        return [cacheValue[options.where.name]];
    }

    const [instance] = await model.findOrCreate(options);

    if (!cache.has(cacheKey)) {
        cache.set(cacheKey, {});
    }

    cache.get(cacheKey)[options.where.name] = instance;

    return [instance];
}


// Função para processar características
const processScheduleFeatures = async (scheduleId, featuresString, featureType) => {
    if (!featuresString) return;

    const features = featuresString.split(',').map((f) => f.trim());
    for (const featureName of features) {
        const [feature] = await models.Feature.findOrCreate({ where: { name: featureName } });

        // Use findOrCreate para evitar duplicatas na tabela ScheduleFeature
        await models.ScheduleFeature.findOrCreate({
            where: {
                scheduleId,
                featureId: feature.id,
                featureType,
            },
            defaults: {
                scheduleId,
                featureId: feature.id,
                featureType,
            },
        });
    }
};

/*
const processCsvRow = async (row, versionId) => {
    try {
        // 1. Criar ou encontrar o AcademicProgram (Curso)
        const [academicProgram] = await findOrCreateWithCache(models.AcademicProgram,"academicPrograms",{
            where: { name: row['Curso'] },
        });

        // 2. Criar ou encontrar o Subject (Unidade de execução)
        const [subject] = await findOrCreateWithCache(models.Subject, "subjects",{
            where: {
                name: row['Unidade de execução'],
                academicProgramId: academicProgram.id,
            },
        });

        // 3. Criar ou encontrar o ClassGroup (Turma)
        const [classGroup] = await findOrCreateWithCache(models.ClassGroup, "classGroups",{
            where: { name: row['Turma'] },
        });

        // 4. Criar ou encontrar o Shift (Turno)
        const [shift] = await findOrCreateWithCache(models.Shift, "shifts",{
            where: { name: row['Turno'] },
            defaults: {
                subjectId: subject.id,
                classGroupId: classGroup.id,
                enrollment: parseInt(row['Inscritos no turno'], 10) || 0,
            },
        });

        // 5. Criar ou encontrar o Weekday (Dia da Semana)
        const [weekday] = await findOrCreateWithCache(models.Weekday, "weekdays",{
            where: { abbreviation: row['Dia da Semana'] },
        });

        // 6. Criar ou encontrar o ClassRoom (Sala)
        let classRoom = null;
        if (row['Sala da aula'] && row['Sala da aula'] !== 'Não necessita de sala') {
            [classRoom] = await findOrCreateWithCache(models.ClassRoom, "classRooms",{
                where: { name: row['Sala da aula'] },
                defaults: {
                    capacity: parseInt(row['Lotação'], 10) || null,
                },
            });
        }
        
        // Validar e formatar a data
        let formattedDate = null;
        if (row['Dia']) {
            const [day, month, year] = row['Dia'].split('/');
            if (day && month && year) {
                formattedDate = `${year}-${month}-${day}`; // Reorganiza a data para o formato ISO
            }
        }


        // Validar e converter os horários
        const startTime = row['Início'];
        const endTime = row['Fim'];
        if (!/^([0-1]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$/.test(startTime)) {
            throw new Error(`Horário de início inválido: ${startTime}`);
        }
        if (!/^([0-1]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$/.test(endTime)) {
            throw new Error(`Horário de fim inválido: ${endTime}`);
        }

        // 7. Criar o Schedule (Horário)
        const schedule = await models.Schedule.create({
            versionId,
            shiftId: shift.id,
            classRoomId: classRoom ? classRoom.id : null,
            weekdayId: weekday.id,
            startTime: startTime,
            endTime: endTime,
            date: formattedDate,
        });

        // 8. Processar características
        await processScheduleFeatures(schedule.id, row['Características da sala pedida para a aula'], 'requested');
        await processScheduleFeatures(schedule.id, row['Características reais da sala'], 'real');

        console.log(`Linha processada com sucesso para o turno ${row['Turno']}`);
    } catch (error) {
        console.error('Erro ao processar linha do CSV:', error, row);
        throw error;
    }
};
*/

// Função principal para processar o CSV
const csvProcessor = async (filePath, versionId) => {
    const results = [];

    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
            .pipe(parse({ headers: true, delimiter: ';' }))
            .on('data', (row) => results.push(row))
            .on('end', async () => {
                try {
                    for (const row of results) {
                        await processCsvRow(row, versionId);
                    }
                    console.log(`CSV processado com sucesso para a versão ${versionId}`);
                    resolve();
                } catch (error) {
                    console.error('Erro durante o processamento do CSV:', error);
                    reject(error);
                }
            })
            .on('error', (error) => {
                console.error('Erro ao ler o arquivo CSV:', error);
                reject(error);
            });
    });
};


module.exports = csvProcessor;