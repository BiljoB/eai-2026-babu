FROM node:20-bookworm-slim
WORKDIR /app
COPY observer/package*.json ./
RUN npm install
COPY observer/ ./
RUN npm run build
CMD ["node", "dist/index.js"]