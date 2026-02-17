FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
COPY packages/ingestion/package*.json ./packages/ingestion/
COPY packages/mock-api/package*.json ./packages/mock-api/

RUN npm ci

COPY . .

RUN npm run -w @datasync/mock-api build

WORKDIR /app/packages/mock-api

CMD ["npm", "run", "start"]
