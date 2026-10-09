// Test-only cross-language fixture generator adapted from the independent review.
// Usage: node generate-reference.cjs ACTON_NODE_MODULES OUTPUT [--all]
// Production Go bindings and offline Go tests do not depend on Node or this SDK.
const fs = require('node:fs');
const path = require('node:path');
const base = path.resolve(process.argv[2]);
const core = require(path.join(base, '@ton/core'));
const { DynamicCtx } = require(path.join(base, '@ton/tolk-abi-to-typescript/dist/dynamic-ctx'));
const { dynamicPack, createTonCoreDictionaryKey } = require(path.join(base, '@ton/tolk-abi-to-typescript/dist/dynamic-serialization'));
const { renderTy, createLabelsForUnion } = require(path.join(base, '@ton/tolk-abi-to-typescript/dist/types-kernel'));
const catalog = JSON.parse(fs.readFileSync(path.join(__dirname, '../catalog.json'), 'utf8'));
const selected = new Set(['bidask.BidaskRange', 'cocoon.CocoonRoot', 'wallets.WalletTg',
    'wallets.WalletV5r1', 'wallets/w4r2.WalletV4r2', 'Jetton Vesting.JettonVesting']);
const all = process.argv.includes('--all');
const rows = [], skipped = {};
for (const contract of catalog.contracts) {
    if (!all && !selected.has(contract.id)) continue;
    const a = contract.compilerAbi, ctx = new DynamicCtx(a), symbols = ctx.symbols;
    function value(i, deep, depth = 0, uLabel) {
        if (depth > 25) throw new Error('recursive probe');
        const ty = symbols.tyByIdx(i);
        const child = (j, label) => value(j, deep, depth + 1, label);
        let go, ts;
        switch (ty.kind) {
            case 'intN': case 'uintN': case 'coins': case 'varintN': case 'varuintN':
                go = '0'; ts = 0n; break;
            case 'bool': go = ts = true; break;
            case 'string': go = ts = 'probe'; break;
            case 'address': case 'addressOpt': case 'addressAny':
                if (ty.kind !== 'address' && !deep) { go = null; ts = ty.kind === 'addressAny' ? 'none' : null; break; }
                ts = core.Address.parse('0:' + 'ab'.repeat(32)); go = ts.toRawString(); break;
            case 'addressExt': go = {bits:5, hex:'a8'}; ts = new core.ExternalAddress(21n,5); break;
            case 'bitsN':
                go = {bits:ty.n,hex:'00'.repeat(Math.ceil(ty.n/8))};
                ts = core.beginCell().storeUint(0,ty.n).endCell().beginParse(); break;
            case 'cell': ts = core.Cell.EMPTY; go = ts.toBoc().toString('base64'); break;
            case 'remaining':
                ts = core.Cell.EMPTY.beginParse(); go = core.Cell.EMPTY.toBoc().toString('base64'); break;
            case 'slice': {
                const c = core.beginCell().storeUint(5,3).storeRef(core.Cell.EMPTY).endCell();
                ts = c.beginParse(); go = c.toBoc().toString('base64'); break;
            }
            case 'nullable': if (!deep) {go = ts = null;} else { return child(ty.inner_ty_idx); } break;
            case 'cellOf': { const v = child(ty.inner_ty_idx); go = v.go; ts = {ref:v.ts}; break; }
            case 'arrayOf': case 'lispListOf': {
                if (ty.kind === 'lispListOf' && deep) throw new Error('known TS lisp ref-order discrepancy');
                const vs = deep ? [child(ty.inner_ty_idx)] : [];
                go = vs.map(v=>v.go); ts = vs.map(v=>v.ts); break;
            }
            case 'tensor': case 'shapedTuple': {
                const vs = ty.items_ty_idx.map(j=>child(j)); go = vs.map(v=>v.go); ts = vs.map(v=>v.ts); break;
            }
            case 'mapKV': {
                const key = createTonCoreDictionaryKey(ctx,'probe',ty.key_ty_idx);
                ts = core.Dictionary.empty(key); go = [];
                if (deep) { const k=child(ty.key_ty_idx), v=child(ty.value_ty_idx); ts.set(k.ts,v.ts); go.push({key:k.go,value:v.go}); }
                break;
            }
            case 'StructRef': {
                const d=symbols.getStruct(ty.struct_name);
                if (d.custom_pack_unpack) throw new Error('custom hook');
                go={};ts={$:ty.struct_name};
                for (const f of symbols.structFieldsOf(i,false)) { const v=child(f.ty_idx,f.uLabelTyIdx); go[f.name]=v.go;ts[f.name]=v.ts; }
                break;
            }
            case 'AliasRef': {const target=symbols.aliasTargetOf(i); if(symbols.getAlias(ty.alias_name).custom_pack_unpack) throw new Error('custom hook'); return child(target.ty_idx,target.uLabelTyIdx);}
            case 'EnumRef': {
                const d=symbols.getEnum(ty.enum_name); if(d.custom_pack_unpack) throw new Error('custom hook');
                go=d.members[0].value;ts=BigInt(go);break;
            }
            case 'union': {
                const variants=createLabelsForUnion(symbols,ty.variants,uLabel);
                const v=variants[deep ? 0 : variants.length-1]; const tv=symbols.tyByIdx(v.variant_ty_idx);
                if(tv.kind==='nullLiteral') {go=ts=null;break;}
                const inner=child(v.variant_ty_idx);
                go={$:renderTy(symbols,v.variant_ty_idx),value:inner.go};
                ts=v.hasValueField?{$:v.labelStr,value:inner.ts}:inner.ts;break;
            }
            case 'void': go=null;ts=undefined;break;
            case 'nullLiteral': go=ts=null;break;
            default: throw new Error('unsupported probe type '+ty.kind);
        }
        return {go,ts};
    }
    const roots = [];
    if(a.storage.storage_ty_idx!==undefined) roots.push(['storage',a.storage.storage_ty_idx]);
    if(a.storage.storage_at_deployment_ty_idx!==undefined) roots.push(['deployment',a.storage.storage_at_deployment_ty_idx]);
    for(const direction of ['incoming_messages','incoming_external','outgoing_messages','emitted_events']) {
        a[direction].forEach((m,j)=>roots.push([direction+':'+j,m.body_ty_idx]));
    }
    for(const [root,i] of roots) for(const deep of [false,true]) {
        try {
            const v=value(i,deep), b=core.beginCell(); dynamicPack(ctx,'probe',i,v.ts,b);
            rows.push({id:contract.id,root,deep,value:v.go,boc:b.endCell().toBoc().toString('base64')});
        } catch(err) {const msg=String(err.message);skipped[msg]=(skipped[msg]||0)+1;}
    }
}
fs.writeFileSync(process.argv[3], JSON.stringify({source:'Acton 5dd8d80af21734efc31480849f3d313f4d89e751; independent review TS serializer',rows,skipped})+'\n');
console.log(JSON.stringify({vectors:rows.length,skipped},null,2));
