const fs = require('fs');
const path = require('path');

function findNative(dir, depth) {
  if (depth > 4) return [];
  let results = [];
  try {
    const items = fs.readdirSync(dir);
    for (const item of items) {
      const full = path.join(dir, item);
      const stat = fs.statSync(full);
      if (stat.isFile() && (item === 'binding.gyp' || item.endsWith('.node'))) {
        results.push(dir);
      } else if (stat.isDirectory() && item !== '.bin' && depth < 4) {
        results = results.concat(findNative(full, depth + 1));
      }
    }
  } catch (e) {}
  return results;
}

const natives = new Set();
const dirs = findNative('./node_modules', 0);
dirs.forEach(d => {
  const parts = d.replace(/\\/g, '/').split('/');
  if (parts[1].startsWith('@')) natives.add(parts[1] + '/' + parts[2]);
  else natives.add(parts[1]);
});
console.log([...natives].sort().join('\n'));
