FROM node:20-alpine

WORKDIR /usr/src/app

RUN npm install kafkajs

COPY . .

CMD ["node", "tcp_service.js"]