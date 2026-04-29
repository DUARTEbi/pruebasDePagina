const fs = require('fs');
let dash = fs.readFileSync('public/dashboard.html', 'utf8');
let auto = fs.readFileSync('public/partials/auto.html', 'utf8');

function replaceRGB(css) {
  css = css.replace(/rgba\(57,255,20,\.?(\d+)\)/g, (m, p1) => {
    let pct = p1.length === 1 ? p1 + '0' : (p1.startsWith('0') ? p1.substring(1) : p1);
    if(pct === '03' || pct === '04' || pct === '05' || pct === '06' || pct === '07' || pct === '08' || pct === '09') {
        pct = pct.replace(/^0/, '');
    }
    return `color-mix(in srgb, var(--accent) ${pct}%, transparent)`;
  });
  css = css.replace(/rgba\(0,255,30,\.?(\d+)\)/g, (m, p1) => {
    let pct = p1.length === 1 ? p1 + '0' : (p1.startsWith('0') ? p1.substring(1) : p1);
    if(pct === '03' || pct === '04' || pct === '05' || pct === '06' || pct === '07' || pct === '08' || pct === '09') {
        pct = pct.replace(/^0/, '');
    }
    return `color-mix(in srgb, var(--accent) ${pct}%, transparent)`;
  });
  return css;
}

fs.writeFileSync('public/dashboard.html', replaceRGB(dash));
fs.writeFileSync('public/partials/auto.html', replaceRGB(auto));
console.log('Done replacing rgba colors.');
