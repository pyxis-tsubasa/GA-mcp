FROM node:20-slim

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev

COPY server.js server.mjs ./

ENV PORT=8080
EXPOSE 8080

CMD ["npm", "run", "start"]