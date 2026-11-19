FROM node:20-bookworm-slim
WORKDIR /app
COPY orchestrator/package*.json ./
RUN npm install
COPY orchestrator/ ./
RUN npm run build
CMD ["node", "dist/index.js"]