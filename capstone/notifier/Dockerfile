FROM node:20-bookworm-slim
WORKDIR /app
COPY notifier/package*.json ./
RUN npm install
COPY notifier/ ./
RUN npm run build
CMD ["node", "dist/index.js"]