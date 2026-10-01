#!/usr/bin/env python3
"""Require every LLVM-emitted source line and branch; no exclusions."""
import sys
from pathlib import Path

lines = branches = hit_lines = hit_branches = 0
misses = []
source = None
for record in Path(sys.argv[1]).read_text().splitlines():
    if record.startswith('SF:'):
        source = record[3:]
    elif record.startswith('DA:'):
        assert source, 'line evidence requires a source file'
        line, count = map(int, record[3:].split(',')[:2])
        assert line > 0 and count >= 0, 'invalid line evidence'
        lines += 1
        hit_lines += count > 0
        if count == 0:
            misses.append(f'{source}:{line}')
    elif record.startswith('BRDA:'):
        assert source, 'branch evidence requires a source file'
        parts = record[5:].split(',')
        assert len(parts) == 4, 'invalid branch evidence'
        count = 0 if parts[3] == '-' else int(parts[3])
        assert count >= 0, 'invalid branch hit count'
        branches += 1
        hit_branches += count > 0
        if count == 0:
            misses.append(f'{source}:{parts[0]} branch {parts[1]}:{parts[2]}')
    elif record == 'end_of_record':
        source = None
print(f'Lines: {hit_lines}/{lines}; branches: {hit_branches}/{branches}')
assert lines > 0 and branches > 0, 'empty coverage evidence'
assert not misses, '\n'.join(misses)
