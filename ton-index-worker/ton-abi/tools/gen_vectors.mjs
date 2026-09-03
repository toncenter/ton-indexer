// Development oracle for committed ABI fixtures and byte-stable pack/unpack
// vectors. The generated-struct gate compares both value dumps and repacked
// bytes against these results. Coverage includes every supported value kind,
// generic union labels, custom serializers, and expected failures.
// Requires the reference checkout at FIXTURES_SRC. Production declarations are
// compiled separately by gen_prod_abi.mjs.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

import { runTolkCompiler } from '@ton/tolk-js';
import { DynamicCtx, packToBuilderDynamic, unpackFromSliceDynamic } from '@ton/tolk-abi-to-typescript';
import { beginCell, Address, ExternalAddress, Dictionary } from '@ton/core';

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
// The reference repo's input/ (its source .tolk fixtures). This script only runs
// on a machine that has that checkout, so point TOLK_ABI_INPUT at it; the
// fallback assumes a sibling checkout next to this repository.
const FIXTURES_SRC = process.env.TOLK_ABI_INPUT ??
    path.resolve(ROOT, '..', '..', '..', 'tolk-abi-to-typescript', 'input');
const FIXTURES_DIR = path.join(ROOT, 'testdata', 'fixtures');
const VECTORS_FILE = path.join(ROOT, 'testdata', 'abi_vectors.json');

// Union-label helper is internal to the package (not re-exported from its
// public index). Reaching into dist/ is fine for a dev-only oracle: it is
// the same helper the reference pack/unpack uses, so label and variant
// selection stay in lockstep instead of being re-derived.
const { createLabelsForUnion } = require(
    path.join(__dirname, 'node_modules', '@ton', 'tolk-abi-to-typescript', 'dist', 'types-kernel.js'));

const tolkAbiPkg = require('@ton/tolk-abi-to-typescript/package.json');
const tolkJsPkg = require('@ton/tolk-js/package.json');
const corePkg = require('@ton/core/package.json');

async function compileTolk(absFileName) {
    const r = await runTolkCompiler({
        entrypointFileName: absFileName,
        fsReadCallback: p => fs.readFileSync(p, 'utf-8'),
    });
    if (r.status === 'error') {
        throw new Error(`tolk-js compile failed for ${absFileName}: ${r.message}`);
    }
    const abiJson = r.abiJson;
    abiJson.code_boc64 = r.codeBoc64;
    return abiJson;
}


function findStructDecl(abi, structName) {
    const decl = abi.declarations.find(d => d.kind === 'struct' && d.name === structName);
    if (!decl) throw new Error(`struct '${structName}' not found in declarations`);
    return decl;
}

function toValueJson(abi, tyIdx, v) {
    const ty = abi.unique_types[tyIdx];
    switch (ty.kind) {
        case 'intN': case 'uintN': case 'coins': case 'varintN': case 'varuintN':
            return v.toString();
        case 'bool':
            return v;
        case 'StructRef': {
            const decl = findStructDecl(abi, ty.struct_name);
            const out = { $: decl.name };
            for (const f of decl.fields) {
                out[f.name] = toValueJson(abi, f.ty_idx, v[f.name]);
            }
            return out;
        }
        default:
            throw new Error(`toValueJson: unsupported kind '${ty.kind}' (out of W0 scope, see file header)`);
    }
}

function fromValueJson(abi, tyIdx, j) {
    const ty = abi.unique_types[tyIdx];
    switch (ty.kind) {
        case 'intN': case 'uintN': case 'coins': case 'varintN': case 'varuintN':
            return BigInt(j);
        case 'bool':
            return j;
        case 'StructRef': {
            const decl = findStructDecl(abi, ty.struct_name);
            const out = { $: decl.name };
            for (const f of decl.fields) {
                out[f.name] = fromValueJson(abi, f.ty_idx, j[f.name]);
            }
            return out;
        }
        default:
            throw new Error(`fromValueJson: unsupported kind '${ty.kind}'`);
    }
}

function packHex(ctx, structTyIdx, obj) {
    const b = beginCell();
    packToBuilderDynamic(ctx, structTyIdx, obj, b);
    const cell = b.endCell();
    return cell.beginParse().toString();
}

function roundTripCheckOrThrow(ctx, abi, structTyIdx, obj, expectHex) {
    // (a) direct pack must equal expected golden hex
    const hex1 = packHex(ctx, structTyIdx, obj);
    if (hex1 !== expectHex) {
        throw new Error(`golden hex mismatch: got ${hex1} want ${expectHex}`);
    }
    // (a) value-JSON must be a lossless round-trip: json -> object -> pack -> same hex
    const json = toValueJson(abi, structTyIdx, obj);
    const reparsed = fromValueJson(abi, structTyIdx, json);
    const hex2 = packHex(ctx, structTyIdx, reparsed);
    if (hex2 !== expectHex) {
        throw new Error(`round-trip hex mismatch: got ${hex2} want ${expectHex} (value-JSON not lossless: ${JSON.stringify(json)})`);
    }
    // dynamic unpack must agree too (sanity against the reference lib itself)
    const s = beginCell().store(bb => packToBuilderDynamic(ctx, structTyIdx, obj, bb)).endCell().beginParse();
    const unpacked = unpackFromSliceDynamic(ctx, structTyIdx, s);
    if (s.remainingBits !== 0 || s.remainingRefs !== 0) {
        throw new Error(`unpack did not consume slice fully for ${JSON.stringify(json)}`);
    }
    return json;
}

// Full-kind value-JSON codec and synthetic sample generator. Both use
// ctx.symbols so monomorphs, aliases, and unions resolve correctly. Golden
// values come from unpacking the packed bytes, ensuring map entries follow the
// HashmapE wire traversal rather than JavaScript collection order.

function hexLower(buf) {
    return Buffer.from(buf).toString('hex');
}

// mapKV key helpers (createTonCoreDictionaryKey/Value are not exported from
// the package index either; only intN/uintN/address keys are standard).
function dictKeyOf(ctx, keyTyIdx) {
    const kty = ctx.symbols.tyByIdx(keyTyIdx);
    if (kty.kind === 'intN') return Dictionary.Keys.BigInt(kty.n);
    if (kty.kind === 'uintN') return Dictionary.Keys.BigUint(kty.n);
    if (kty.kind === 'address') return Dictionary.Keys.Address();
    throw new Error(`dictKeyOf: non-standard map key kind '${kty.kind}'`);
}
function dictValueOf(ctx, valueTyIdx) {
    return {
        serialize(v, b) { packToBuilderDynamic(ctx, valueTyIdx, v, b); },
        parse(s) { return unpackFromSliceDynamic(ctx, valueTyIdx, s); },
    };
}

// Synthesizes a RAW value (in @ton/core's own shapes -- Address/ExternalAddress
// instances, Dictionary, arrays, {$,...fields} objects) that packToBuilderDynamic
// accepts for `tyIdx`. `uLabelTyIdx` threads exactly like dynamicPack/Unpack's
// own parameter (needed so a monomorphized generic union picks the right
// label). `depth` is a termination heuristic for the (structurally rare but
// possible) self-referential type -- past kMaxDepth we take the shortest
// available option (null / empty array / a union's void variant if present).
const kMaxDepth = 6;

// Custom serializers that have a fixture struct to hang a vector off of.
// Tensor3Skipping1/Color/OnlyWithPack/OnlyWithUnpack have no wrapping struct
// and are covered by native C++ units only. Gated BEFORE normal
// StructRef/AliasRef resolution, matching the generated C++ path.
const W9_CUSTOM_SAMPLE = {
    TelegramString: () => beginCell().storeBuffer(Buffer.from('hello')).endCell().beginParse(),
    Custom8: () => 42n,
    MyBorderedInt: () => 20n,  // packs to the ">10" range tag; unpacks back as 10n
    CustomPoint: () => ({ x: 3n, y: 4n }),
};
const W9_CUSTOM_TO_JSON = {
    TelegramString: (v) => {
        const s = v.clone();
        const n = s.remainingBits;
        let bits = '';
        for (let i = 0; i < n; i++) bits += s.loadUint(1).toString();
        return { bits, refs: [] };
    },
    Custom8: (v) => v.toString(),
    MyBorderedInt: (v) => v.toString(),
    CustomPoint: (v) => ({ $: 'CustomPoint', x: v.x.toString(), y: v.y.toString() }),
};
const W9_CUSTOM_FROM_JSON = {
    TelegramString: (j) => {
        const b = beginCell();
        for (const ch of j.bits) b.storeUint(ch === '1' ? 1 : 0, 1);
        return b.endCell().beginParse();
    },
    Custom8: (j) => BigInt(j),
    MyBorderedInt: (j) => BigInt(j),
    CustomPoint: (j) => ({ x: BigInt(j.x), y: BigInt(j.y) }),
};

function sampleValue(ctx, tyIdx, uLabelTyIdx, depth) {
    const ty = ctx.symbols.tyByIdx(tyIdx);
    const d1 = depth + 1;
    if (ty.kind === 'AliasRef' && W9_CUSTOM_SAMPLE[ty.alias_name]) return W9_CUSTOM_SAMPLE[ty.alias_name]();
    if (ty.kind === 'StructRef' && W9_CUSTOM_SAMPLE[ty.struct_name]) return W9_CUSTOM_SAMPLE[ty.struct_name]();
    switch (ty.kind) {
        case 'intN': {
            const maxAbs = (1n << BigInt(Math.max(ty.n - 1, 0))) - 1n;  // signed range top
            return maxAbs < 5n ? maxAbs : 5n;
        }
        case 'uintN': {
            const max = (1n << BigInt(ty.n)) - 1n;
            return max < 5n ? max : 5n;
        }
        case 'varintN': case 'varuintN': case 'coins':
            return 5n;
        case 'bool':
            return true;
        case 'cell':
            return beginCell().storeUint(0x42, 8).endCell();
        case 'string':
            return 'hi';
        case 'remaining':
            return beginCell().storeUint(0xAB, 8).endCell().beginParse();
        case 'address':
            return new Address(0, Buffer.alloc(32, 0x11));
        case 'addressOpt':
            return depth > 2 ? null : new Address(0, Buffer.alloc(32, 0x22));
        case 'addressExt':
            return new ExternalAddress(0xABn, 8);
        case 'addressAny':
            return 'none';
        case 'bitsN':
            return beginCell().storeUint(0n, ty.n).endCell().beginParse();
        case 'nullLiteral':
            return null;
        case 'void':
            return undefined;
        case 'nullable':
            return depth > 2 ? null : sampleValue(ctx, ty.inner_ty_idx, undefined, d1);
        case 'cellOf':
            return { ref: sampleValue(ctx, ty.inner_ty_idx, undefined, d1) };
        case 'arrayOf': case 'lispListOf':
            return depth > kMaxDepth ? [] : [sampleValue(ctx, ty.inner_ty_idx, undefined, d1)];
        case 'tensor': case 'shapedTuple':
            return ty.items_ty_idx.map(t => sampleValue(ctx, t, undefined, d1));
        case 'mapKV': {
            const dict = Dictionary.empty(dictKeyOf(ctx, ty.key_ty_idx), dictValueOf(ctx, ty.value_ty_idx));
            const kty = ctx.symbols.tyByIdx(ty.key_ty_idx);
            const key = kty.kind === 'address' ? new Address(0, Buffer.alloc(32, 0x33)) : 7n;
            dict.set(key, sampleValue(ctx, ty.value_ty_idx, undefined, d1));
            return dict;
        }
        case 'EnumRef': {
            const en = ctx.symbols.getEnum(ty.enum_name);
            return sampleValue(ctx, en.encoded_as_ty_idx, undefined, d1);
        }
        case 'StructRef': {
            const out = { $: ty.struct_name };
            for (const f of ctx.symbols.structFieldsOf(tyIdx, false)) {
                out[f.name] = sampleValue(ctx, f.ty_idx, f.uLabelTyIdx, d1);
            }
            return out;
        }
        case 'AliasRef': {
            const target = ctx.symbols.aliasTargetOf(tyIdx);
            return sampleValue(ctx, target.ty_idx, target.uLabelTyIdx, d1);
        }
        case 'union': {
            const variants = createLabelsForUnion(ctx.symbols, ty.variants, uLabelTyIdx);
            let idx = 0;
            if (depth > kMaxDepth) {
                const voidIdx = ty.variants.findIndex(v => ctx.symbols.tyByIdx(v.variant_ty_idx).kind === 'void');
                if (voidIdx >= 0) idx = voidIdx;
            }
            const chosenTy = ctx.symbols.tyByIdx(ty.variants[idx].variant_ty_idx);
            if (chosenTy.kind === 'void') {
                return { $: 'void', value: undefined };
            }
            const inner = sampleValue(ctx, ty.variants[idx].variant_ty_idx, undefined, d1);
            return variants[idx].hasValueField ? { $: variants[idx].labelStr, value: inner } : inner;
        }
        default:
            throw new Error(`sampleValue: unsupported/non-serializable kind '${ty.kind}'`);
    }
}

function toValueJsonFull(ctx, tyIdx, v, uLabelTyIdx) {
    const ty = ctx.symbols.tyByIdx(tyIdx);
    if (ty.kind === 'AliasRef' && W9_CUSTOM_TO_JSON[ty.alias_name]) return W9_CUSTOM_TO_JSON[ty.alias_name](v);
    if (ty.kind === 'StructRef' && W9_CUSTOM_TO_JSON[ty.struct_name]) return W9_CUSTOM_TO_JSON[ty.struct_name](v);
    switch (ty.kind) {
        case 'intN': case 'uintN': case 'varintN': case 'varuintN': case 'coins':
            return v.toString();
        case 'bool':
            return v;
        case 'cell':
            return v.toBoc({ idx: false, crc32: true }).toString('base64');
        case 'string':
            return v;
        case 'address':
            return `${v.workChain}:${hexLower(v.hash)}`;
        case 'addressOpt':
            return v === null ? null : `${v.workChain}:${hexLower(v.hash)}`;
        case 'addressExt':
            return { extern: { bits: v.bits, value: v.value.toString(16).padStart(Math.ceil(v.bits / 4), '0') } };
        case 'addressAny':
            if (v === 'none') return 'none';
            if (v instanceof ExternalAddress) {
                return { extern: { bits: v.bits, value: v.value.toString(16).padStart(Math.ceil(v.bits / 4), '0') } };
            }
            return `${v.workChain}:${hexLower(v.hash)}`;
        case 'bitsN': case 'remaining': {
            const s = v.clone();
            const nbits = s.remainingBits;  // capture BEFORE the loop -- loadUint(1) shrinks remainingBits each call
            let bits = '';
            for (let i = 0; i < nbits; i++) bits += s.loadUint(1).toString();
            const refs = [];
            while (s.remainingRefs) refs.push(s.loadRef().toBoc({ idx: false, crc32: true }).toString('base64'));
            return { bits, refs };
        }
        case 'nullLiteral':
            return null;
        case 'void':
            return { $: 'void' };
        case 'nullable':
            return v === null ? null : toValueJsonFull(ctx, ty.inner_ty_idx, v, undefined);
        case 'cellOf':
            return { ref: toValueJsonFull(ctx, ty.inner_ty_idx, v.ref, undefined) };
        case 'arrayOf': case 'lispListOf':
            return v.map(x => toValueJsonFull(ctx, ty.inner_ty_idx, x, undefined));
        case 'tensor': case 'shapedTuple':
            return ty.items_ty_idx.map((t, i) => toValueJsonFull(ctx, t, v[i], undefined));
        case 'mapKV': {
            const out = [];
            for (const [k, val] of v) {
                out.push([toValueJsonFull(ctx, ty.key_ty_idx, k, undefined), toValueJsonFull(ctx, ty.value_ty_idx, val, undefined)]);
            }
            return out;
        }
        case 'EnumRef': {
            const en = ctx.symbols.getEnum(ty.enum_name);
            return toValueJsonFull(ctx, en.encoded_as_ty_idx, v, undefined);
        }
        case 'StructRef': {
            const out = { $: ty.struct_name };
            for (const f of ctx.symbols.structFieldsOf(tyIdx, false)) {
                out[f.name] = toValueJsonFull(ctx, f.ty_idx, v[f.name], f.uLabelTyIdx);
            }
            return out;
        }
        case 'AliasRef': {
            const target = ctx.symbols.aliasTargetOf(tyIdx);
            return toValueJsonFull(ctx, target.ty_idx, v, target.uLabelTyIdx);
        }
        case 'union': {
            const variants = createLabelsForUnion(ctx.symbols, ty.variants, uLabelTyIdx);
            const hasVoid = ctx.symbols.tyByIdx(ty.variants[ty.variants.length - 1].variant_ty_idx).kind === 'void';
            if (hasVoid && v === undefined) {
                return { $: 'void' };
            }
            const idx = variants.findIndex(vv => v && v.$ === vv.labelStr);
            if (idx < 0) throw new Error(`toValueJsonFull: no union variant matches $ for ${JSON.stringify(v)}`);
            const variant = variants[idx];
            const actual = variant.hasValueField ? v.value : v;
            const inner = toValueJsonFull(ctx, ty.variants[idx].variant_ty_idx, actual, undefined);
            return variant.hasValueField ? { $: variant.labelStr, value: inner } : inner;
        }
        default:
            throw new Error(`toValueJsonFull: unsupported kind '${ty.kind}'`);
    }
}

function fromValueJsonFull(ctx, tyIdx, j, uLabelTyIdx) {
    const ty = ctx.symbols.tyByIdx(tyIdx);
    if (ty.kind === 'AliasRef' && W9_CUSTOM_FROM_JSON[ty.alias_name]) return W9_CUSTOM_FROM_JSON[ty.alias_name](j);
    if (ty.kind === 'StructRef' && W9_CUSTOM_FROM_JSON[ty.struct_name]) return W9_CUSTOM_FROM_JSON[ty.struct_name](j);
    switch (ty.kind) {
        case 'intN': case 'uintN': case 'varintN': case 'varuintN': case 'coins':
            return BigInt(j);
        case 'bool':
            return j;
        case 'cell':
            return require('@ton/core').Cell.fromBoc(Buffer.from(j, 'base64'))[0];
        case 'string':
            return j;
        case 'address': {
            const [wc, hex] = j.split(':');
            return new Address(parseInt(wc, 10), Buffer.from(hex, 'hex'));
        }
        case 'addressOpt': {
            if (j === null) return null;
            const [wc, hex] = j.split(':');
            return new Address(parseInt(wc, 10), Buffer.from(hex, 'hex'));
        }
        case 'addressExt':
            return new ExternalAddress(BigInt('0x' + j.extern.value), j.extern.bits);
        case 'addressAny': {
            if (j === 'none') return 'none';
            if (typeof j === 'object') return new ExternalAddress(BigInt('0x' + j.extern.value), j.extern.bits);
            const [wc, hex] = j.split(':');
            return new Address(parseInt(wc, 10), Buffer.from(hex, 'hex'));
        }
        case 'bitsN': case 'remaining': {
            const b = beginCell();
            for (const ch of j.bits) b.storeUint(ch === '1' ? 1 : 0, 1);
            for (const r of j.refs) b.storeRef(require('@ton/core').Cell.fromBoc(Buffer.from(r, 'base64'))[0]);
            return b.endCell().beginParse();
        }
        case 'nullLiteral':
            return null;
        case 'void':
            return undefined;
        case 'nullable':
            return j === null ? null : fromValueJsonFull(ctx, ty.inner_ty_idx, j, undefined);
        case 'cellOf':
            return { ref: fromValueJsonFull(ctx, ty.inner_ty_idx, j.ref, undefined) };
        case 'arrayOf': case 'lispListOf':
            return j.map(x => fromValueJsonFull(ctx, ty.inner_ty_idx, x, undefined));
        case 'tensor': case 'shapedTuple':
            return ty.items_ty_idx.map((t, i) => fromValueJsonFull(ctx, t, j[i], undefined));
        case 'mapKV': {
            const dict = Dictionary.empty(dictKeyOf(ctx, ty.key_ty_idx), dictValueOf(ctx, ty.value_ty_idx));
            for (const [k, val] of j) {
                const key = ctx.symbols.tyByIdx(ty.key_ty_idx).kind === 'address'
                    ? fromValueJsonFull(ctx, ty.key_ty_idx, k, undefined)
                    : BigInt(k);
                dict.set(key, fromValueJsonFull(ctx, ty.value_ty_idx, val, undefined));
            }
            return dict;
        }
        case 'EnumRef': {
            const en = ctx.symbols.getEnum(ty.enum_name);
            return fromValueJsonFull(ctx, en.encoded_as_ty_idx, j, undefined);
        }
        case 'StructRef': {
            const out = { $: ty.struct_name };
            for (const f of ctx.symbols.structFieldsOf(tyIdx, false)) {
                out[f.name] = fromValueJsonFull(ctx, f.ty_idx, j[f.name], f.uLabelTyIdx);
            }
            return out;
        }
        case 'AliasRef': {
            const target = ctx.symbols.aliasTargetOf(tyIdx);
            return fromValueJsonFull(ctx, target.ty_idx, j, target.uLabelTyIdx);
        }
        case 'union': {
            const variants = createLabelsForUnion(ctx.symbols, ty.variants, uLabelTyIdx);
            if (j && j.$ === 'void') return undefined;
            const idx = variants.findIndex(vv => j && j.$ === vv.labelStr);
            if (idx < 0) throw new Error(`fromValueJsonFull: no union variant matches $ for ${JSON.stringify(j)}`);
            const variant = variants[idx];
            const actualJson = variant.hasValueField ? j.value : j;
            const inner = fromValueJsonFull(ctx, ty.variants[idx].variant_ty_idx, actualJson, undefined);
            // A hasValueField variant uses the {$,value} shape expected by
            // dynamicPack; other variants pass their inner value directly.
            return variant.hasValueField ? { $: variant.labelStr, value: inner } : inner;
        }
        default:
            throw new Error(`fromValueJsonFull: unsupported kind '${ty.kind}'`);
    }
}

function packHexFull(ctx, tyIdx, obj) {
    const b = beginCell();
    packToBuilderDynamic(ctx, tyIdx, obj, b);
    return b.endCell().beginParse().toString();
}

// Generates one vector for `structName` in `fixture`: sample -> pack ->
// unpack (oracle) -> derive canonical value-JSON from the UNPACKED result ->
// self-check the value-JSON round-trips losslessly back to the same hex.
function genW8Vector(ctx, fixture, structName, explicitSample) {
    const structTyIdx = ctx.symbols.getStruct(structName).ty_idx;
    const sample = explicitSample !== undefined ? explicitSample : sampleValue(ctx, structTyIdx, undefined, 0);
    // Pack exactly once because Dictionary serialization mutates internal
    // state. Derive both the hex and unpack cursor from that resulting cell.
    const b = beginCell();
    packToBuilderDynamic(ctx, structTyIdx, sample, b);
    const cell = b.endCell();
    const hex = cell.beginParse().toString();
    const s = cell.beginParse();
    const unpacked = unpackFromSliceDynamic(ctx, structTyIdx, s);
    if (s.remainingBits !== 0 || s.remainingRefs !== 0) {
        throw new Error(`genW8Vector(${fixture}::${structName}): unpack did not consume slice fully`);
    }
    const value = toValueJsonFull(ctx, structTyIdx, unpacked, undefined);
    const reparsed = fromValueJsonFull(ctx, structTyIdx, value, undefined);
    const hex2 = packHexFull(ctx, structTyIdx, reparsed);
    if (hex2 !== hex) {
        throw new Error(`genW8Vector(${fixture}::${structName}): value-JSON round-trip mismatch: ${hex} vs ${hex2}\n${JSON.stringify(value)}`);
    }
    return { fixture, struct: structName, value, golden_hex: hex };
}

async function main() {
    fs.mkdirSync(FIXTURES_DIR, { recursive: true });

    const fixtureNames = ['tolk_counter', 'lots-of-messages', 'lots-of-wrappers'];
    const fixtureFiles = {
        'tolk_counter': 'tolk_counter.tolk',
        'lots-of-messages': 'lots-of-messages.tolk',
        'lots-of-wrappers': 'lots-of-wrappers.tolk',
    };

    const vectors = [];
    const abiByFixture = {};

    for (const fixture of fixtureNames) {
        const srcFile = path.join(FIXTURES_SRC, fixtureFiles[fixture]);
        const dstFile = path.join(FIXTURES_DIR, fixtureFiles[fixture]);
        fs.copyFileSync(srcFile, dstFile);

        const abi = await compileTolk(srcFile);
        abiByFixture[fixture] = abi;
        if (abi.abi_schema_version !== '1.0') {
            throw new Error(`unexpected abi_schema_version '${abi.abi_schema_version}' for ${fixture} (locked decision 15 pins exactly "1.0")`);
        }
        fs.writeFileSync(
            path.join(FIXTURES_DIR, `${fixture}.abi.json`),
            JSON.stringify(abi, null, 2) + '\n',
        );

        const ctx = new DynamicCtx(abi);

        if (fixture === 'tolk_counter') {
            const tyIdx = ctx.symbols.getStruct('IncreaseCounter').ty_idx;
            for (const val of [
                { queryId: 0n, increaseBy: 1n },
                { queryId: 123456789n, increaseBy: 4294967295n },
            ]) {
                const hex = packHex(ctx, tyIdx, val);
                const json = roundTripCheckOrThrow(ctx, abi, tyIdx, val, hex);
                vectors.push({ fixture, struct: 'IncreaseCounter', value: json, golden_hex: hex });
            }
            const tyIdxReset = ctx.symbols.getStruct('ResetCounter').ty_idx;
            for (const val of [{ queryId: 0n }, { queryId: 18446744073709551615n }]) {
                const hex = packHex(ctx, tyIdxReset, val);
                const json = roundTripCheckOrThrow(ctx, abi, tyIdxReset, val, hex);
                vectors.push({ fixture, struct: 'ResetCounter', value: json, golden_hex: hex });
            }
        }

        if (fixture === 'lots-of-messages') {
            const tyIdx = ctx.symbols.getStruct('IncreaseBy').ty_idx;
            for (const val of [{ counter_id: 0n, inc_by: 1n }, { counter_id: -5n, inc_by: 100n }]) {
                const hex = packHex(ctx, tyIdx, val);
                const json = roundTripCheckOrThrow(ctx, abi, tyIdx, val, hex);
                vectors.push({ fixture, struct: 'IncreaseBy', value: json, golden_hex: hex });
            }
            const tyIdxEmpty = ctx.symbols.getStruct('EmptyMsg').ty_idx;
            const hexEmpty = packHex(ctx, tyIdxEmpty, {});
            const jsonEmpty = roundTripCheckOrThrow(ctx, abi, tyIdxEmpty, {}, hexEmpty);
            vectors.push({ fixture, struct: 'EmptyMsg', value: jsonEmpty, golden_hex: hexEmpty });
        }

        if (fixture === 'lots-of-wrappers') {
            // Literal golds transcribed from the reference fixture tests.
            const literalCases = [
                { struct: 'MsgSinglePrefix32', val: { amount1: 80n, amount2: 800000000n }, hex: 'x{8765432115042FAF0800}' },
                { struct: 'CounterIncrement', val: { counter_id: 123n, inc_by: 78n }, hex: 'x{123456787B0000004E}' },
                { struct: 'CounterDecrement', val: { counter_id: 0n, dec_by: -38n }, hex: 'x{2345678900FFFFFFDA}' },
                { struct: 'CounterReset0', val: { counter_id: 0n }, hex: 'x{3456789000}' },
                { struct: 'CounterResetTo', val: { counter_id: 0n, initial_value: 29874329774732n }, hex: 'x{001843000000001B2BA8D06A8C}' },
            ];
            for (const { struct, val, hex } of literalCases) {
                const tyIdx = ctx.symbols.getStruct(struct).ty_idx;
                const json = roundTripCheckOrThrow(ctx, abi, tyIdx, val, hex);
                vectors.push({ fixture, struct, value: json, golden_hex: hex, source: 'LotsOfWrappers.spec.ts literal gold' });
            }
        }
    }

    // Copy reference .tolk files including imports/ so fixtures that import
    // from it (jetton-minter/wallet-contract, lots-of-throws) keep resolving.
    fs.mkdirSync(path.join(FIXTURES_DIR, 'imports'), { recursive: true });
    for (const f of fs.readdirSync(path.join(FIXTURES_SRC, 'imports'))) {
        fs.copyFileSync(path.join(FIXTURES_SRC, 'imports', f), path.join(FIXTURES_DIR, 'imports', f));
    }

    const remainingFixtures = [
        'client-type-anno', 'debug-print-demos',
        'err-cont-on-stack-1', 'err-cont-on-stack-2',
        'err-invalid-map-key-1', 'err-invalid-map-key-2',
        'generic-union-labels', 'has-not-init-storage',
        'jetton-minter-contract', 'jetton-wallet-contract',
        'lots-of-annotations', 'lots-of-getters', 'lots-of-storage',
        'lots-of-throws', 'only-header',
    ];
    let loadableCount = 3;  // the 3 vector fixtures above already loaded fine
    for (const fixture of remainingFixtures) {
        const fileName = `${fixture}.tolk`;
        fs.copyFileSync(path.join(FIXTURES_SRC, fileName), path.join(FIXTURES_DIR, fileName));
        const abi = await compileTolk(path.join(FIXTURES_SRC, fileName));
        if (abi.abi_schema_version !== '1.0') {
            throw new Error(`unexpected abi_schema_version '${abi.abi_schema_version}' for ${fixture}`);
        }
        fs.writeFileSync(
            path.join(FIXTURES_DIR, `${fixture}.abi.json`),
            JSON.stringify(abi, null, 2) + '\n',
        );
        abiByFixture[fixture] = abi;  // ctxFor currently uses this for generic-union-labels
        loadableCount++;
    }

    // small.tolk: tolk-js rejects `int` as a boolean condition; no ABI JSON,
    // no consumer.
    fs.copyFileSync(path.join(FIXTURES_SRC, 'small.tolk'), path.join(FIXTURES_DIR, 'small.tolk'));
    try {
        await compileTolk(path.join(FIXTURES_SRC, 'small.tolk'));
        throw new Error('small.tolk unexpectedly compiled -- update this script and the C++ loads-all test');
    } catch (e) {
        if (!/can not use `int` as a boolean condition/.test(e.message)) {
            throw e;  // any OTHER failure is a real regression, not the known one
        }
        console.log(`small.tolk: known compile error (expected, not an ABI/loader issue): ${e.message.split('\n')[0]}`);
    }

    console.log(`copied 26 .tolk fixtures (19 top-level + 7 imports/), ${loadableCount} compile to a valid ABI`);

    // One ctx per fixture, reusing the abi already compiled above.
    const ctxByFixture = {};
    const ctxFor = (fixture) => (ctxByFixture[fixture] ??= new DynamicCtx(abiByFixture[fixture]));

    const w8Before = vectors.length;

    // -- lots-of-wrappers: address, nullable (both branches), cellOf, array,
    // lisp list, tensor, map, enum, addressAny+nested-union-of-address (auto-
    // sampled via the compound WithAnyAddress struct).
    {
        const ctx = ctxFor('lots-of-wrappers');

        // Register the same custom implementations the reference uses on the
        // oracle's own ctx, so pack/unpack route through them for these vectors.
        function TelegramString__packToBuilder(self, b) {
            let bytes = Math.ceil(self.remainingBits / 8);
            b.storeUint(bytes, 8);
            b.storeSlice(self);
        }
        function TelegramString__unpackFromSlice(s) {
            let bytes = s.loadUint(8);
            return new (require('@ton/core').Slice)(new (require('@ton/core').BitReader)(s.loadBits(bytes * 8)), []);
        }
        function Custom8__packToBuilder(self, b) { b.storeUint(self, 8); }
        function Custom8__unpackFromSlice(s) { return s.loadUintBig(8); }
        function MyBorderedInt__packToBuilder(self, b) {
            if (self > 10) { b.storeUint(1, 4); } else if (self > 0) { b.storeUint(2, 4); } else { b.storeUint(3, 4); }
        }
        function MyBorderedInt__unpackFromSlice(s) {
            switch (s.loadUint(4)) {
                case 1: return 10n;
                case 2: return 0n;
                case 3: return -1n;
                default: throw new Error('bad MyBorderedInt tag');
            }
        }
        function CustomPoint__packToBuilder(self, b) {
            b.storeUint(self.x, 8);
            b.storeUint(self.y, 8);
        }
        function CustomPoint__unpackFromSlice(s) {
            return { $: 'CustomPoint', x: s.loadUintBig(8), y: s.loadUintBig(8) };
        }
        ctx.registerCustomPackUnpack('TelegramString', TelegramString__packToBuilder, TelegramString__unpackFromSlice);
        ctx.registerCustomPackUnpack('Custom8', Custom8__packToBuilder, Custom8__unpackFromSlice);
        ctx.registerCustomPackUnpack('MyBorderedInt', MyBorderedInt__packToBuilder, MyBorderedInt__unpackFromSlice);
        ctx.registerCustomPackUnpack('CustomPoint', CustomPoint__packToBuilder, CustomPoint__unpackFromSlice);
        // Color is intentionally not registered, matching the reference's
        // asymmetric custom-serializer setup.
        // No vector is needed; a native unit covers this case.

        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'JustAddress'));
        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'JustMaybeInt32', { $: 'JustMaybeInt32', value: 5n }));
        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'JustMaybeInt32', { $: 'JustMaybeInt32', value: null }));
        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'TwoInts32AndRef64'));
        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'WithArrays2'));
        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'WithLispLists1'));
        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'TransferParams2'));  // exercises a 'tensor' field
        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'WithMaps0'));
        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'WithEnums'));
        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'WithAnyAddress'));

        // Custom-serializer vectors. WithMyBorder (MyBorderedInt) is
        // deliberately NOT a vector here: its custom encoding is lossy AND
        // non-idempotent (pack(20n) -> tag1 -> unpack -> 10n, but
        // pack(10n) -> tag2, NOT tag1 -- 10 is not a fixed point), which
        // breaks the vector harness's own round-trip self-check by design.
        // Covered correctly by the native unit in test/AbiABGateTest.cpp instead.
        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'StorWithStr'));    // TelegramString field
        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'PointWithCustomInt'));  // Custom8 field
        vectors.push(genW8Vector(ctx, 'lots-of-wrappers', 'CustomPoint'));    // custom struct itself
    }

    // -- generic-union-labels: monomorph instantiation (GenericPair<int32,int64>
    // via MsgPair) whose nested union must render generic labels "T1"/"T2",
    // not "int32"/"int64" -- the concrete generic-union-labels feature.
    {
        const ctx = ctxFor('generic-union-labels');
        vectors.push(genW8Vector(ctx, 'generic-union-labels', 'MsgPair', {
            $: 'MsgPair', value: { $: 'GenericPair', value: { $: 'T1', value: 5n } },
        }));
        vectors.push(genW8Vector(ctx, 'generic-union-labels', 'MsgPair', {
            $: 'MsgPair', value: { $: 'GenericPair', value: { $: 'T2', value: 9n } },
        }));
        vectors.push(genW8Vector(ctx, 'generic-union-labels', 'MsgOrInt16'));
        vectors.push(genW8Vector(ctx, 'generic-union-labels', 'MsgAliasInt16'));
    }

    // -- malformed-UTF8 string: hand-built cell (NOT via packToBuilderDynamic,
    // which can only ever emit valid UTF-8 from a JS string) so the wire bytes
    // are genuinely ill-formed; the golden value is the ORACLE's own
    // loadStringRefTail decode of those exact bytes (Buffer.toString('utf8')
    // replacement semantics). unpack_only: true -- there is no valid "pack
    // this JS value and get the same malformed bytes back" direction.
    {
        const ctx = ctxFor('lots-of-wrappers');
        const structTyIdx = ctx.symbols.getStruct('WithStrings1').ty_idx;
        const rawBytes = Buffer.from([0x41, 0xE0, 0x80, 0x80, 0x42]);  // 'A' + overlong E0 80 80 + 'B'
        const inner = beginCell().storeBuffer(rawBytes).endCell();
        const outer = beginCell().storeRef(inner).storeUint(0, 1).endCell();  // s1: ref(snake), s2: nullable absent
        const s = outer.beginParse();
        const unpacked = unpackFromSliceDynamic(ctx, structTyIdx, s);
        if (s.remainingBits !== 0 || s.remainingRefs !== 0) {
            throw new Error('malformed-UTF8 vector: unpack did not consume slice fully');
        }
        const value = toValueJsonFull(ctx, structTyIdx, unpacked, undefined);
        vectors.push({
            fixture: 'lots-of-wrappers', struct: 'WithStrings1',
            value, golden_hex: outer.beginParse().toString(), unpack_only: true,
            source: 'hand-built malformed-UTF8 wire bytes (E0 80 80 overlong), golden value = Node Buffer.toString(utf8) oracle decode',
        });
    }

    console.log(`W8: added ${vectors.length - w8Before} value/hex coverage vectors`);

    // Error vectors (per-field truncation, prefix mismatch, union no-match).
    // No `value`/oracle involved -- these assert unpack FAILS.
    const errBefore = vectors.length;
    {
        // truncation: drop the last byte of a valid IncreaseCounter encoding.
        const ctr = ctxFor('tolk_counter');
        const ctrTy = ctr.symbols.getStruct('IncreaseCounter').ty_idx;
        const goodHex = packHexFull(ctr, ctrTy, { $: 'IncreaseCounter', queryId: 0n, increaseBy: 1n });
        const goodBits = goodHex.match(/x\{([0-9A-F_]*)\}/)[1];
        const truncatedHex = `x{${goodBits.slice(0, goodBits.length - 2)}}`;  // drop the last byte (2 hex chars)
        vectors.push({
            fixture: 'tolk_counter', struct: 'IncreaseCounter',
            expect_error: true, golden_hex: truncatedHex,
            source: 'per-field truncation: IncreaseCounter with its last byte dropped',
        });

        // prefix mismatch: flip the low nibble of the 32-bit struct-opcode prefix.
        const flippedNibble = (parseInt(goodBits[7], 16) ^ 0xF).toString(16).toUpperCase();
        const badPrefixHex = `x{${goodBits.slice(0, 7)}${flippedNibble}${goodBits.slice(8)}}`;
        vectors.push({
            fixture: 'tolk_counter', struct: 'IncreaseCounter',
            expect_error: true, golden_hex: badPrefixHex,
            source: 'prefix mismatch: IncreaseCounter opcode nibble flipped',
        });
    }
    {
        // union no-match: a hand-authored (NOT tolk-compiled) minimal ABI with
        // a 2-variant explicit-prefix union (8-bit opcodes 0x0A / 0x0B) that,
        // unlike compiler-generated implicit-prefix unions (which are always
        // dispatch-complete by construction -- no real fixture has a gap),
        // leaves the rest of the 8-bit space unmatched on purpose.
        const handAbiPath = path.join(FIXTURES_DIR, 'w8-hand-union.abi.json');
        const handAbi = {
            abi_schema_version: '1.0', contract_name: 'W8HandUnion',
            unique_types: [
                { kind: 'intN', n: 8 },
                { kind: 'union', variants: [
                    { variant_ty_idx: 0, prefix_num: 10, prefix_len: 8, is_prefix_implicit: false },
                    { variant_ty_idx: 0, prefix_num: 11, prefix_len: 8, is_prefix_implicit: false },
                ] },
                { kind: 'StructRef', struct_name: 'Holder' },
            ],
            struct_instantiations: [], alias_instantiations: [],
            declarations: [
                { kind: 'struct', name: 'Holder', ty_idx: 2, fields: [{ name: 'u', ty_idx: 1 }] },
            ],
            storage: {}, incoming_messages: [], incoming_external: [], outgoing_messages: [],
            emitted_events: [], get_methods: [], thrown_errors: [],
            compiler_name: 'hand-authored', compiler_version: 'n/a',
        };
        fs.writeFileSync(handAbiPath, JSON.stringify(handAbi, null, 2) + '\n');
        vectors.push({
            fixture: 'w8-hand-union', struct: 'Holder',
            expect_error: true, golden_hex: 'x{0C}',  // neither 0x0A nor 0x0B
            source: 'hand-authored ABI (not tolk-compiled): explicit-prefix union with a deliberate dispatch gap',
        });
    }
    console.log(`W8: added ${vectors.length - errBefore} error vectors`);

    const output = {
        header: {
            pkg_version: tolkAbiPkg.version,
            tolk_js_version: tolkJsPkg.version,
            core_version: corePkg.version,
            compiler_version: abiByFixture['tolk_counter'].compiler_version,
            abi_schema_version: '1.0',
        },
        vectors,
    };

    fs.writeFileSync(VECTORS_FILE, JSON.stringify(output, null, 2) + '\n');
    console.log(`wrote ${vectors.length} vectors to ${VECTORS_FILE}`);
}

main().catch(e => { console.error(e); process.exit(1); });
