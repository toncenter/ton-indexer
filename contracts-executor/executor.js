#!/usr/bin/env node

// Simple node.js contract executor
// It uses ton-contract-executor with pre-build WASM ton version (OMG!)
const { SmartContract, stackInt, stackSlice, stackCell}  = require("ton-contract-executor");
const { Cell, Address, beginCell} = require("ton");
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

function safe(x) {
  return x.replace("\00", "")
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

function formatOutput(result, expected) {
  return result.slice(0, expected.length).map((value, idx) => {
    if (expected[idx] == 'int') {
      try {
        return value.toString(10);
      } catch {
        console.warn("Unable to parse int", value)
      }
    } else if (expected[idx] == 'address') {
      try {
        if (value.constructor.name === "BN") {
          return beginCell().storeUint(4, 3).storeUint(0, 8).storeUint(value, 256)
            .endCell().beginParse().readAddress().toFriendly();// .toFriendly();
        } else {
          return value.readAddress().toFriendly();
        }
      } catch(e) {
        // throw e;
        console.warn("Unable to parse address", value)
      }
    } else if (expected[idx] == 'cell_hash') {
      try {
        return value.hash().toString('base64')
      } catch {
        console.warn("Unable to parse address", value)
      }
    } else if (expected[idx] == 'metadata') {
      try {
        const content = value.beginParse();
        if (content.remaining == 0) {
          console.warn("Wrong content layout format, zero bits")
          return {};
        }
        const layout = content.readUint(8).toNumber();
        if (layout == 0) {
          console.log("On chain metadata layout detected")
          const data = content.readDict(256, (slice) => safe(parseStrValue(slice)));
          let outData = {};
          data.forEach((value, key) => {
            if (parseInt(key) in METADATA_KEYS) {
              outData[METADATA_KEYS[parseInt(key)]] = safe(value);
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
          let out = ''
          let currentCell = content
          while (true) {
            out += currentCell.readRemainingBytes().toString()
            if (currentCell.remainingRefs > 0) {
              currentCell = currentCell.readRef()
            } else {
              break
            }
          }
          return {
            content_layout: 'off-chain',
            content: out
          }
        }
      } catch (e) {
        console.warn("Unable to parse metadata", value, e)
      }
    } else if (expected[idx] == 'boc') {
      try {
        return value.toBoc().toString("base64");
      } catch (e) {
        return value.toCell().toBoc().toString("base64");
      }
    } else if (expected[idx] == 'string') {
      return value.readRemainingBytes().toString()
    } else {
      throw new Error("Mapping type " + expected[idx] + " not supported")
    }
  })
}

async function execute(req) {
    let gas_limit = req.gas_limit
    console.log("Executing %s with gas limit %s", req.method, gas_limit)
    let contract = await SmartContract.fromCell(
        Cell.fromBoc(Buffer.from(req.code, 'base64'))[0],
        Cell.fromBoc(Buffer.from(req.data, 'base64'))[0]
    )

    if (req.address !== undefined && req.address !== null && req.address.startsWith("EQ")) {
      contract.setC7Config({myself: Address.parse(req.address)})
    }
    let res;
    try {
      let argsFormatted = []
      if (req.arguments) {
        for (let arg of req.arguments) {
          if (typeof arg == "number") {
            argsFormatted.push(stackInt(arg))
          } else if (typeof arg == "string") {
            if (arg.startsWith("E")) { // parse address
              argsFormatted.push(stackSlice(beginCell().storeAddress(Address.parse(arg)).endCell()))
            } else { // otherwise treat as base64-encoded boc
              argsFormatted.push(stackCell(Cell.fromBoc(Buffer.from(arg, 'base64'))[0]))
            }
          } else {
            throw new Error("Argument type is not supported: " + typeof arg)
          }
        }
      }


      res = await contract.invokeGetMethod(req.method, args=argsFormatted, opts=gas_limit ? {gasLimits: {limit: gas_limit}} : {});
      console.log("finish execution for %s, exit code is %s, gas consumed %s", req.method, res.exit_code, res.gas_consumed)
    } catch (e) {
      console.log("Unable to execute contract", e)
      return {exit_code: -100};
    }
    if (res.result.length != req.expected.length && req.parse_list === undefined) {
      console.warn("wrong result size: " + res.result.length + ", expected " +
        req.expected.length + " (" + req.expected)
    }

    if (req.parse_list) {
      let out = []
      let current = res.result[0]
      while (current) {
        if (current.length == 2) {
          out.push(formatOutput(current[0], req.expected));
          current = current[1];
        } else {
          console.warn("broken list")
          break;
        }
      }
      res.result = out
    } else {
      res.result = formatOutput(res.result, req.expected);
    }
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
