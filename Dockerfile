FROM node:12-alpine

WORKDIR /app

COPY . .

RUN npm install

CMD [ "node", "index.js" ]
