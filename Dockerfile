FROM node:14
WORKDIR /app/node
COPY . .
#RUN npm i
EXPOSE 3000:3000
