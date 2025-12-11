FROM node:20-slim

WORKDIR /app

COPY . .

RUN rm -rf node_modules package-lock.json && npm install

EXPOSE 5173

CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]

