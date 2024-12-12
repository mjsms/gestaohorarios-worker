FROM node:18-alpine

WORKDIR /app

# Copiar a pasta de modelos usando caminho relativo ao Dockerfile
# `..` volta para `ui`, e `../gestaohorarios-models` acessa a pasta modelos
COPY ../gestaohorarios-models ./gestaohorarios-models

# Agora copiar o próprio projeto principal (o diretório atual do Dockerfile é gestaohorarios)
COPY . .

WORKDIR /app
WORKDIR gestaohorarios-worker

RUN npm install
EXPOSE 3000
CMD ["npm", "start"]
