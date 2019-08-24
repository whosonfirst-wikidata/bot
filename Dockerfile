FROM node:lts

COPY ./ /app

WORKDIR /app

RUN npm --production --unsafe-perms --verbose install

CMD node index.js
