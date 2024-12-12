const { models } = require('./db'); // Importa conexão e modelos
const processVersion = require('./processVersion'); // Módulo de processamento de versões

(async () => {
    console.log('Worker iniciado.');

    // Função que processa os ScheduleVersion
    const processScheduleVersions = async () => {
        try {
            console.log('Processando versões de horários...');

            // Obtenha versões com status "pending"
            const pendingVersions = await models.ScheduleVersion.findAll({
                where: { status: 'pending' }
            });

            try{
                for (const version of pendingVersions) {
                    console.log(`Processando versão: ${version.id}`);

                    await processVersion(version);

                    // Atualizar status para "processed"
                    console.log(`Versão ${version.id} processada.`);
                }
            }catch (error) {
                console.error('Erro ao processar versões de horários:', error);
            }
            console.log('Todas as versões pendentes foram processadas.');

        } catch (error) {
            console.error('Erro ao processar versões de horários:', error);
        }

        // Agendar próxima execução após 5 segundos
        setTimeout(processScheduleVersions, 5 * 1000);
    };

    // Execute a primeira iteração imediatamente
    processScheduleVersions();

})();
