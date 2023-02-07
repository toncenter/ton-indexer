#!/usr/bin/env node

// Simple node.js contract executor
// It uses ton-contract-executor with pre-build WASM ton version (OMG!)
const { SmartContract }  = require("ton-contract-executor");
const { Cell, Address } = require("ton");
const bodyParser = require('body-parser')
const express = require('express');
const app = express();
app.use(bodyParser.json({limit: '50mb'}));

// TEP-64
METADATA_KEYS = {
  51065135818459385347574250312853146822620586594996463797054414300406918686668: 'uri',
  59089242681608890680090686026688704441792375738894456860693970539822503415433: 'name',
  90922719342317012409671596374183159143637506542604000676488204638996496437508: 'description',
  43884663033947008978309661017057008345326326811558777475113826163084742639165: 'image',
  98449690268711667050166283313913751402364107788915545466587557261600130787812: 'image_data',
  82961397245523513629401799123410942652413991882008909918554405086738284660097: 'symbol',
  107878361799212983662495570378745491379550006934010968359181619763835345146430: 'decimals',
  62901303891490913777575453651345555215047913278006836178809905342063884613376: 'amount_style',
  95542036767220248279389797734652506193411128639728265318048579598694325246656: 'render_type'
}

function parseSnakeFormat(slice) {
  let buff = slice.readRemainingBytes().toString();
  for (let ref of slice.refs) {
    buff += parseSnakeFormat(ref.beginParse())
  }
  return buff
}

function parseStrValue(slice, is_first=true) {
  if (slice.remaining == 0) {
    if (slice.refs.length > 0) {
      return slice.refs.map((cell, idx) => parseStrValue(cell.beginParse(), idx == 0)).join('')
    } else {
      return undefined;
    }
  }
  if (is_first) {
    const tag = slice.readUint(8);
    if (tag == 0) {
      return parseSnakeFormat(slice)
    } else {
      throw new Error("Wrong content tag: " + tag)
    }
  } else {
    return parseSnakeFormat(slice)
  }
}

async function execute(req) {
    console.log("Executing %s", req.method);
    let contract = await SmartContract.fromCell(
        Cell.fromBoc(Buffer.from(req.code, 'base64'))[0],
        Cell.fromBoc(Buffer.from(req.data, 'base64'))[0]
    )
  
    if (req.address !== undefined && req.address !== null && req.address.startsWith("EQ")) {
      contract.setC7Config({myself: Address.parse(req.address)})
    }
    let res;
    try {
      res = await contract.invokeGetMethod(req.method, []);
    } catch (e) {
      console.log("Unable to execute contract", e)
      return {exit_code: -100};
    }
    if (res.result.length != req.expected.length) {
      console.warn("wrong result size: " + res.result.length + ", expected " +
        req.expected.length + " (" + req.expected)
    }
    res.result = res.result.slice(0, req.expected.length).map((value, idx) => {
      if (req.expected[idx] == 'int') {
        try {
          return value.toString(10);
        } catch {
          console.warn("Unable to parse int", value)
        }
      } else if (req.expected[idx] == 'address') {
        try {
          return value.readAddress().toFriendly();
        } catch {
          console.warn("Unable to parse address", value)
        }
      } else if (req.expected[idx] == 'cell_hash') {
        try {
          return value.hash().toString('base64')
        } catch {
          console.warn("Unable to parse address", value)
        }
      } else if (req.expected[idx] == 'metadata') {
        try {
          const content = value.beginParse();
          if (content.remaining == 0) {
            console.warn("Wrong content layout format, zero bits")
            return {};
          }
          const layout = content.readUint(8).toNumber();
          if (layout == 0) {
            console.log("On chain metadata layout detected")
            const data = content.readDict(256, (slice) => parseStrValue(slice));
            let outData = {};
            data.forEach((value, key) => {
              if (parseInt(key) in METADATA_KEYS) {
                outData[METADATA_KEYS[parseInt(key)]] = value;
              } else {
                throw new Error("Unknown metadata field: " + key);
              }
            })
            console.log("On chain dict parsed", outData);
            return {
              content_layout: 'on-chain',
              content: outData
            }
          } else {
            console.log("Off chain metadata layout detected")
            // console.log(content.readRemainingBytes().toString(), content.remaining, content.remainingRefs)
            return {
              content_layout: 'off-chain',
              content: content.readRemainingBytes().toString()
            }
          }
        } catch (e) {
          console.warn("Unable to parse metadata", value, e)
        }
      } else if (req.expected[idx] == 'boc') {
        return value.toBoc().toString("base64");
      } else {
        throw new Error("Mapping type " + req.expected[idx] + " not supported")
      }
    })
    return res;
}

app.post('/execute', async (req, res, next) => {
  try {
    const result = await execute(req.body);
    res.send(result);
  } catch (err) {
    next(err);
  }
})

const server = app.listen(9090, function () {
  const host = server.address().address
  const port = server.address().port

  console.log("Listening at http://%s:%s", host, port)
})
