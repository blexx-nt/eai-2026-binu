FROM node:20-bookworm-slim
WORKDIR /app
COPY notifier/package.json ./
RUN npm install --omit=dev
COPY notifier/src ./src
ENV CONSUME_QUEUE=notifications.queue
CMD ["npm", "start"]
