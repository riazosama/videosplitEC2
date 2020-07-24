const ffmpeg = require("fluent-ffmpeg");
const fs = require("fs");
const aws = require("aws-sdk");
const winston = require('winston');
aws.config.update({ region: "us-east-2" });
const sqs = new aws.SQS();
const s3 = new aws.S3();

const BUCKETNAME = "serverlessvideo1-bucket";
const SQS_URL = "https://sqs.us-east-2.amazonaws.com/506464813465/serverlessvideo1-sqs"

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  defaultMeta: { service: 'user-service' },
  transports: [
    new winston.transports.File({ filename: 'info.log' }),
  ],
});


const stopEC2 = async () => {
  const ec2 = new aws.EC2({ region: "us-east-2" });
  try {
    await ec2.stopInstances({ InstanceIds: ["i-0cdbc5787ca65c8ed"] }).promise();
    logger.info("Successfully Stoped i-0cdbc5787ca65c8ed")
  } catch (e) {
    logger.info(e);
  }
}

const getMessageFromSQS = async () => {
  const params = {
    QueueUrl: SQS_URL,
    AttributeNames: [
      "All"
    ],
    MaxNumberOfMessages: 1
  };

  try {
    return await sqs.receiveMessage(params).promise()
  } catch (e) {
    logger.info(e)
  }
};

const downloadFile = (key) => {
  const params = {
    Bucket: BUCKETNAME,
    Key: key,
  };

  return new Promise((res, rej) => {
    s3.getObject(params, (err, data) => {
      if (err) {
        logger.info(err);
        return rej(err)
      };
      fs.writeFileSync(`./input/${key}`, data.Body);
      res(`./input/${key} has been created!`);
    });
  });
};

const getVideoMeta = (key) => {
  return new Promise((res, rej) => {
    ffmpeg.ffprobe(`./input/${key}`, (err, meta) => {
      if (err) {
        logger.info(err)
        rej(err)
      } else {
        res(meta)
      }
    })
  })
}

const uploadFile = (path, key) => {

  if (!fs.existsSync(path)) {
    logger.info(`${path}: No file found to upload. Skipping now....`);
    return;
  }

  return new Promise((res, rej) => {
    fs.readFile(path, (err, data) => {
      if (err) console.error(err);
      var base64data = new Buffer(data, "binary");
      var params = {
        Bucket: BUCKETNAME,
        Key: key,
        Body: base64data,
      };
      s3.upload(params, (err, data) => {
        if (err) {
          logger.info(err);
          rej(err)
          return;
        };
        res("Upload Complete");
      });

    })

  });
}

const deleteFilesLocally = (path) => {

  if (!fs.existsSync(path)) {
    logger.info("No file found to delete. Skipping now....")
    return;
  }

  return new Promise((res, rej) => {
    fs.unlink(path, (err) => {
      if (err) rej(err);
      res("Files Deleted");
    });
  });
};

const deleteMessage = (receiptHandle) => {
  return new Promise((res, rej) => {
    const params = {
      QueueUrl: SQS_URL,
      ReceiptHandle: receiptHandle,
    };
    sqs.deleteMessage(params, function (err, data) {
      if (err) rej(err);
      else res(data);
    });
  });
};

const createAudio = async (filename, ext) => {

  const { streams } = await getVideoMeta(`${filename}.${ext}`)

  if (streams.length !== 2) {
    logger.info("No Audio Detected")
    return;
  }

  return new Promise((res, rej) => {
    ffmpeg(`./input/${filename}.${ext}`)
      .noVideo()
      .on("end", function () {
        logger.info("Done:Audio")
        res("Done:Audio");
      })
      .save(`./output/${filename}-audio.mp3`);
  });
};

const createVideoWithoutAudio = async (filename, ext) => {



  const { streams } = await getVideoMeta(`${filename}.${ext}`)
  return new Promise((res, rej) => {

    if (streams[0].width > 1080) {
      ffmpeg(`./input/${filename}.${ext}`)
        .size("1080x720")
        .noAudio()
        .on("end", function () {
          logger.info("Done:Video")
          res("Done:Video");
        })
        .on("progress", function (data) {
          logger.info(`Converting ${filename}: ${Math.ceil(data.percent)}%`)
        })
        .save(`./output/${filename}-vid.${ext}`);
    } else {
      ffmpeg(`./input/${filename}.${ext}`)
        .noAudio()
        .on("end", function () {
          logger.info("Done:Video")
          res("Done:Video");
        })
        .on("progress", function (data) {
          logger.info(`Converting ${filename}: ${Math.ceil(data.percent)}%`)
        })
        .save(`./output/${filename}-vid.${ext}`);
    }
  });
};


(async () => {
  const sqs = await getMessageFromSQS();
  let Messages = sqs.Messages ? sqs.Messages : []

  while (Messages.length > 0) {

    const cleaned = Messages.map(res => ({ message: JSON.parse(res.Body), ReceiptHandle: res.ReceiptHandle }))

    for (let file of cleaned) {
      const key = file.message.key.replace(/\+/g, " ");
      const ext = key.split(".").pop()
      const filename = key.split(".").slice(0, -1).join(".");
      logger.info("starting to download...")
      await downloadFile(key);
      logger.info("File Downloaded")
      logger.info("Converting...")
      await Promise.all([
        createAudio(filename, ext),
        createVideoWithoutAudio(filename, ext),
      ]);
      logger.info("File converted")
      const currentTime = new Date().toISOString();
      await Promise.all([
        uploadFile(
          `./output/${filename}-audio.mp3`,
          `output/${filename}-${currentTime}/${filename}-audio.mp3`
        ),
        uploadFile(
          `./output/${filename}-vid.${ext}`,
          `output/${filename}-${currentTime}/${filename}-vid.${ext}`
        ),
      ]);
      logger.info("Files Uploaded")
      await Promise.all([
        deleteFilesLocally(`./output/${filename}-audio.mp3`),
        deleteFilesLocally(`./output/${filename}-vid.${ext}`),
        deleteFilesLocally(`./input/${filename}.${ext}`),
      ]);
      logger.info("Files Deleted Locally")
      await deleteMessage(file.ReceiptHandle);
      logger.info("Message Deleted From SQS")
    }

    const sqs = await getMessageFromSQS()
    Messages = sqs.Messages ? sqs.Messages : []
  }

  logger.info("Nothing to process.\nStopping EC2 Instance.")

  await stopEC2();

})()