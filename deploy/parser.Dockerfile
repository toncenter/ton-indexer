FROM node:19.5.0

# app
COPY contracts-executor /opt/contracts-executor
WORKDIR /opt/contracts-executor
RUN npm install

ENTRYPOINT ["node", "executor.js"]
