FROM node:20-bookworm-slim
WORKDIR /app
COPY intake/package*.json ./
RUN npm install
COPY intake/ ./
RUN npm run build
CMD ["node", "dist/index.js"]