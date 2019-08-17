import * as path from "path";
import { promises as fs } from "fs";
import * as url from "url";
import { cpus } from "os";
import * as cluster from "cluster";
import * as crypto from "crypto";

import * as program from "commander";
import axios from "axios";
import * as zlib from "zlib";
import { google } from "googleapis";

if (cluster.isMaster) {
  const getAccessToken = (serviceAccountPath: string): Promise<string> => {
    return new Promise(function(resolve, reject) {
      const key = require(serviceAccountPath);
      const jwtClient = new google.auth.JWT(
        key.client_email,
        null,
        key.private_key,
        ["hosting"],
        null
      );
      jwtClient.authorize(function(err, tokens) {
        if (err) {
          reject(err);
          return;
        }
        resolve(tokens.access_token);
      });
    });
  };
  const baseUrl = "https://firebasehosting.googleapis.com/v1beta1";
  let siteUrl = null;
  let serviceAccountAbsolutePath = null;
  let accessToken = null;
  let versionUrl = null;
  let firebaseConfigPath = null;
  let firebaseConfig = null;
  const cores = cpus().length;
  const deploy = async (
    serviceAccountPath: string,
    token: string,
    buildTarget: string,
    siteName: string
  ): Promise<void> => {
    debugger;
    if (!buildTarget) {
      buildTarget = __dirname;
    }
    if (token) {
      accessToken = token;
    }
    if (!serviceAccountPath && accessToken === null) {
      serviceAccountPath = "service-account.json";
    }
    if (!siteName) {
      throw new Error("siteName must be defined");
    }

    firebaseConfigPath = path.join(__dirname, buildTarget, "firebase.json");

    firebaseConfig = {
      config: JSON.parse(await fs.readFile(firebaseConfigPath, "utf8")).hosting
    };

    siteUrl = url.resolve(baseUrl, `/sites/${siteName}`);
    serviceAccountAbsolutePath = path.join(__dirname, serviceAccountPath);
    console.log("dave", token);
    if (accessToken === null) {
      accessToken = await getAccessToken(serviceAccountAbsolutePath);
    }
    versionUrl = url.resolve(baseUrl, await createVersion());
    const files = await getFiles(buildTarget);
    const batchLength = Math.ceil(files.length / cores);
    const clusters = [];
    for (let i = 0; i < cores; i++) {
      const work = {
        accessToken,
        versionUrl,
        files: files.splice(0, batchLength)
      };
      const worker = cluster.fork();
      worker.on("exit", (code, signal) => {
        if (code > 0) {
          process.exit(code);
        }
      });
      clusters.push(
        new Promise((resolve, reject) => {
          worker.on("message", message => {
            switch (message) {
              case "FINISH":
                resolve();
                break;
              case "ERROR":
                reject();
                break;
            }
          });
        })
      );
      worker.send({
        type: "WORK",
        accessToken,
        versionUrl,
        files
      });
    }
    await Promise.all(clusters);
  };

  /*
returns
{
  "name": "sites/site-name/versions/version-id",
  "status": "CREATED",
  "config": {
    "headers": [{
      "glob": "**",
      "headers": {
        "Cache-Control": "max-age=1800"
      }
    }]
  }
}
*/
  const createVersion = async () => {
    const headers = {
      "Content-Type": "application/json",
      Authorization: `Bearer ${accessToken}`
    };

    const options = {
      headers: headers,
      body: firebaseConfig
    };
    const response = await axios.post(siteUrl, options);
    if (response.data.status === "CREATED") {
      return response.data.name;
    } else {
      throw new Error(response.data);
    }
  };
  const getFiles = async dir => {
    let files = await fs.readdir(dir);
    files = (await Promise.all(
      files.map(async file => {
        const filePath = path.join(dir, file);
        const stats = await fs.stat(filePath);
        if (stats.isDirectory()) return getFiles(filePath);
        else if (stats.isFile()) return filePath;
      })
    )) as any;

    return files.reduce(
      (all, folderContents) => all.concat(folderContents),
      []
    );
  };

  program
    .option("-sc, --serviceAccount <path>", "service-account.json path")
    .option("-t, --token <token>", "raw access token")
    .option(
      "-t, --target <path>",
      "the directory to deploy to firebase hosting"
    )
    .option("-sn, --siteName <name>", "The name of the site");
  console.time("firebase-speedy-deploy complete");
  program.parse(process.argv);

  deploy(
    program.serviceAccount,
    program.token,
    program.target,
    program.siteName
  )
    .then(() => {
      console.timeEnd("firebase-speedy-deploy complete");
    })
    .catch(error => {
      console.error(error);
      process.exit(1);
    });
} else {
  process.on("message", async message => {
    if (message.type === "WORK") {
      const { accessToken, versionUrl, files } = message;
      while (files.length > 0) {
        const batch = files.splice(0, 1000);
        const payload = {
          files: {}
        };
        //if anything will kill the RAM, it's this.
        const fileCache = {};
        for await (const file of batch) {
          const absoluteFilePath = path.join(__dirname, file);
          const fileContents = await fs.readFile(absoluteFilePath);
          const compressedValue = await new Promise((resolve, reject) => {
            zlib.deflate(fileContents, function(err, buffer) {
              if (err) {
                return reject(err);
              }
              resolve(buffer);
            });
          });
          const hash = crypto
            .createHash("sha256")
            .update(compressedValue as any)
            .digest("base64");
          payload.files[file] = hash;
          fileCache[hash] = compressedValue;
        }

        var headers = {
          "Content-Type": "application/json",
          Authorization: `Bearer ${accessToken}`
        };

        var options = {
          headers: headers,
          body: payload
        };
        const filesToPopulate = await axios.post(
          `${versionUrl}:populateFiles`,
          options
        );
        for (const hash in fileCache) {
          if (filesToPopulate.data.uploadRequiredHashes.contains(hash)) {
            const uploadUrl = url.resolve(filesToPopulate.data.uploadUrl, hash);

            var headers = {
              Authorization: `Bearer ${accessToken}`,
              "Content-Type": "application/octet-stream"
            };

            var uploadOptions = {
              headers: headers,
              body: fileCache[hash]
            };

            await axios.post(uploadUrl, uploadOptions);
          }
        }
      }
    }
  });
}
