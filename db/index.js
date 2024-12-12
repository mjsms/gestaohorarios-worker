const { sequelize, ...models } = require('gestaohorarios-models'); // Importa os modelos do pacote

(async () => {
    try {
        await sequelize.authenticate();
        console.log('Connected to the database.');
    } catch (error) {
        console.error('Unable to connect to the database:', error);
    }
})();

module.exports = { sequelize, models };
