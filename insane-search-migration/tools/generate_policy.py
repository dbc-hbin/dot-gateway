#!/usr/bin/env python3
"""Generate immutable-policy tables and Python Unicode semantics; no imports/network."""
import ast, ctypes, json, pathlib, sys, unicodedata
root = pathlib.Path(__file__).resolve().parents[1]
tree = ast.parse((root/'vendor/scripts/scan_ai_promotions.py').read_text())
values = {}
for node in tree.body:
    if not isinstance(node, ast.Assign) or not isinstance(node.targets[0], ast.Name): continue
    name=node.targets[0].id
    if name in ('SOURCES','GENERIC_AI_TERMS','SPECIFIC_AI_TERMS','STRONG_DEAL_TERMS','WEAK_DEAL_TERMS','OFFICIAL_VENDORS','OFFICIAL_HOSTS'):
        values[name]=ast.literal_eval(node.value)
    elif name.endswith('_RE') and isinstance(node.value,ast.Call) and node.value.args:
        values[name]=ast.literal_eval(node.value.args[0])
(root/'internal/collector/policy_data.json').write_text(json.dumps(values,ensure_ascii=False,indent=2)+'\n')
fold={str(i):chr(i).casefold() for i in range(sys.maxunicode+1) if chr(i).casefold()!=chr(i)}
# Whole-string Unicode lowercase has one language-neutral contextual rule:
# Greek final sigma. Persist CPython's exact Cased/Case_Ignorable properties.
def property_spans(symbol):
    fn=getattr(ctypes.pythonapi,symbol)
    fn.argtypes=[ctypes.c_uint]; fn.restype=ctypes.c_int
    out=[]; start=last=None
    for n in range(sys.maxunicode+1):
        if not fn(n): continue
        if start is None: start=last=n
        elif n==last+1: last=n
        else: out.append([start,last]);start=last=n
    if start is not None: out.append([start,last])
    return out
special={'cased':property_spans('_PyUnicode_IsCased'),'case_ignorable':property_spans('_PyUnicode_IsCaseIgnorable')}
(root/'internal/collector/casefold_data.json').write_text(json.dumps({'unicode_version':unicodedata.unidata_version,**special,'map':fold,'lower':{str(i):chr(i).lower() for i in range(sys.maxunicode+1) if chr(i).lower()!=chr(i)}},ensure_ascii=False,separators=(',',':'))+'\n')
print('Generated source policy and Python Unicode '+unicodedata.unidata_version)
def ranges(pred):
    spans=[]; start=previous=None
    for n in range(sys.maxunicode+1):
        if not pred(chr(n)): continue
        if start is None: start=previous=n
        elif n==previous+1: previous=n
        else: spans.append((start,previous)); start=previous=n
    if start is not None: spans.append((start,previous))
    def esc(n):
        c=chr(n)
        return '\\'+c if c in r'\-[]^' else c
    return ''.join(esc(a) if a==b else esc(a)+'-'+esc(b) for a,b in spans)
classes={'word':ranges(lambda c:c.isalnum() or c=='_'),'digit':ranges(str.isdecimal),'space':ranges(str.isspace)}
(root/'internal/collector/unicode_classes.json').write_text(json.dumps(classes,ensure_ascii=False,separators=(',',':'))+'\n')
